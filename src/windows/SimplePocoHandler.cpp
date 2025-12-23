#include <vector>
#include <thread>
#include <chrono>
#include <cstring>
#include <cassert>
#include <iostream>
#include <mutex>
#include <atomic>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/RejectCertificateHandler.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/NetException.h>
#include <Poco/Exception.h>

#include "SimplePocoHandler.h"

using namespace Poco::Net;

namespace
{
	std::once_flag sslInitFlag;
	std::atomic<uint32_t> sslUsers{0};

	void ensureSSLInitialized()
	{
		std::call_once(sslInitFlag, []() { Poco::Net::initializeSSL(); });
	}

	void releaseSSLOneRef()
	{
		if (sslUsers.fetch_sub(1, std::memory_order_acq_rel) == 1)
		{
			Poco::Net::uninitializeSSL();
		}
	}

	class Buffer
	{
	public:
		explicit Buffer(size_t size) :
			m_data(size, 0),
			m_use(0)
		{
		}

		size_t write(const char* data, size_t size)
		{
			if (m_use == m_data.size())
			{
				return 0;
			}

			const size_t length = (size + m_use);
			size_t write = length < m_data.size() ? size : m_data.size() - m_use;
			memcpy(m_data.data() + m_use, data, write);
			m_use += write;
			return write;
		}

		void drain()
		{
			m_use = 0;
		}

		size_t available() const
		{
			return m_use;
		}

		const char* data() const
		{
			return m_data.data();
		}

		void shl(size_t count)
		{
			assert(count <= m_use);
			if (count == 0)
			{
				return;
			}

			const size_t diff = m_use - count;
			if (diff)
			{
				std::memmove(m_data.data(), m_data.data() + count, diff);
			}
			m_use = diff;
		}

	private:
		std::vector<char> m_data;
		size_t m_use;
	};
}

struct SimplePocoHandlerImpl
{
	SimplePocoHandlerImpl(bool ssl, const std::string& host) :
		connection(nullptr),
		inputBuffer(SimplePocoHandler::BUFFER_SIZE),
		outBuffer(SimplePocoHandler::BUFFER_SIZE),
		tmpBuff(SimplePocoHandler::TEMP_BUFFER_SIZE),
		pollTimeout(0, 10000), // 10 ms poll to avoid spinning when idle
		overflowBytes(0),
		overflowLastLog(),
		useSSL(ssl)
	{
		if (ssl) 
		{
			ensureSSLInitialized();
			sslUsers.fetch_add(1, std::memory_order_relaxed);
			Poco::SharedPtr<InvalidCertificateHandler> pInvHandler = new RejectCertificateHandler(false);
			Context::Ptr pContext = new Poco::Net::Context(Context::TLS_CLIENT_USE, "", "", "", Context::VERIFY_STRICT);
			SSLManager::instance().initializeClient(nullptr, pInvHandler, pContext);
			SecureStreamSocket* sslSocket = new SecureStreamSocket();
			sslSocket->setPeerHostName(host);
			sslSocket->setLazyHandshake(true);
			socket.reset(sslSocket);
		}
		else
		{
			socket.reset(new StreamSocket());
		}
	}

	~SimplePocoHandlerImpl() {
		if (useSSL)
		{
			releaseSSLOneRef();
		}
	}

	std::unique_ptr<Poco::Net::StreamSocket> socket;
	AMQP::Connection* connection;
	Buffer inputBuffer;
	Buffer outBuffer;
	std::vector<char> tmpBuff;
	Poco::Timespan pollTimeout;
	std::mutex outMutex;
	size_t overflowBytes;
	std::chrono::steady_clock::time_point overflowLastLog;
	bool useSSL;
};
SimplePocoHandler::SimplePocoHandler(const std::string& host, uint16_t port, bool ssl, uint16_t heartbeat) :
	m_impl(new SimplePocoHandlerImpl(ssl, host)), stop(false), desiredHeartbeat(heartbeat)
{
	const Poco::Net::SocketAddress address(host, port);
	m_impl->socket->connect(address);
	m_impl->socket->setBlocking(false);
	m_impl->socket->setSendBufferSize(TEMP_BUFFER_SIZE);
	m_impl->socket->setReceiveBufferSize(TEMP_BUFFER_SIZE);
	m_impl->socket->setKeepAlive(true);
}

SimplePocoHandler::~SimplePocoHandler()
{
	close();
}

void SimplePocoHandler::setConnection(AMQP::Connection* connection)
{
	m_impl->connection = connection;
}

void SimplePocoHandler::loopThread(SimplePocoHandler* obj)
{
	obj->loopRead();
}

void SimplePocoHandler::loopRead()
{
	while (!stop.load(std::memory_order_relaxed))
	{
		try
		{
			loopIteration();
		}
		catch (const Poco::Net::ConnectionResetException& exc) {
			Biterp::Logging::error(exc.displayText());
			m_impl->connection->close();
			std::this_thread::sleep_for(std::chrono::milliseconds(50));
		}
		catch (const Poco::Exception& exc)
		{
			std::string err = typeid(exc).name() + std::string(": ") + exc.displayText() + std::string(". ") + exc.what();
			Biterp::Logging::error(err);
			std::cerr << err << std::endl;
			std::this_thread::sleep_for(std::chrono::milliseconds(50));
		}
	}
}

void SimplePocoHandler::loopIteration() {

	if (!m_impl->connection)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
		return;
	}

	bool socketClosed = false;

	if (m_impl->socket->poll(m_impl->pollTimeout, Socket::SELECT_READ))
	{
		while (true)
		{
			int expected = m_impl->connection->expected();
			if (expected <= 0)
			{
				expected = 4096;
			}
			if (m_impl->tmpBuff.size() < static_cast<size_t>(expected))
			{
				m_impl->tmpBuff.resize(static_cast<size_t>(expected), 0);
			}

			int received = 0;
			try
			{
				received = m_impl->socket->receiveBytes(m_impl->tmpBuff.data(), expected);
			}
			catch (const Poco::TimeoutException&)
			{
				break;
			}
			catch (const Poco::Exception& exc)
			{
				throw;
			}

			if (received <= 0)
			{
				socketClosed = true;
				break;
			}

			const size_t pushed = m_impl->inputBuffer.write(m_impl->tmpBuff.data(), static_cast<size_t>(received));
			if (pushed < static_cast<size_t>(received))
			{
				// throttle logging to avoid gigabyte log files
				m_impl->overflowBytes += static_cast<size_t>(received) - pushed;
				auto now = std::chrono::steady_clock::now();
				if (m_impl->overflowLastLog.time_since_epoch().count() == 0 ||
					std::chrono::duration_cast<std::chrono::seconds>(now - m_impl->overflowLastLog).count() >= 1) {
					Biterp::Logging::error("Input buffer overflow, dropped " + std::to_string(m_impl->overflowBytes) + " bytes");
					m_impl->overflowBytes = 0;
					m_impl->overflowLastLog = now;
				}
				if (m_impl->connection)
				{
					m_impl->connection->close();
				}
				stop.store(true, std::memory_order_relaxed);
				socketClosed = true;
				break;
			}

			if (m_impl->socket->available() <= 0)
			{
				break;
			}
		}
	}
	else
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}

	if (socketClosed || m_impl->socket->available() < 0)
	{
		Biterp::Logging::error("Socket closed or unavailable");
		if (m_impl->connection)
		{
			m_impl->connection->close();
		}
		stop.store(true, std::memory_order_relaxed);
		return;
	}

	if (m_impl->connection && m_impl->inputBuffer.available())
	{
		size_t count = m_impl->connection->parse(m_impl->inputBuffer.data(),
			m_impl->inputBuffer.available());

		if (count == m_impl->inputBuffer.available())
		{
			m_impl->inputBuffer.drain();
		}
		else if (count > 0) {
			m_impl->inputBuffer.shl(count);
		}
	}
	sendDataFromBuffer();
}

void SimplePocoHandler::close()
{
	m_impl->socket->close();
}

void SimplePocoHandler::onData(
	AMQP::Connection* connection, const char* data, size_t size)
{
	m_impl->connection = connection;
	size_t totalWritten = 0;
	while (totalWritten < size)
	{
		size_t written = 0;
		{
			std::lock_guard<std::mutex> lock(m_impl->outMutex);
			written = m_impl->outBuffer.write(data + totalWritten, size - totalWritten);
		}

		if (written == 0)
		{
			if (!sendDataFromBuffer())
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}
		else
		{
			totalWritten += written;
		}
	}
	sendDataFromBuffer();
}

void SimplePocoHandler::onReady(AMQP::Connection* connection)
{
}

void SimplePocoHandler::onError(
	AMQP::Connection* connection, const char* message)
{
	error = message;
	Biterp::Logging::error("AMQP error: " + error);
}

void SimplePocoHandler::onClosed(AMQP::Connection* connection)
{
}

uint16_t SimplePocoHandler::onNegotiate(AMQP::Connection* connection, uint16_t interval) {
	if (desiredHeartbeat > 0)
	{
		return desiredHeartbeat;
	}
	return interval;
}


bool SimplePocoHandler::sendDataFromBuffer()
{
	std::lock_guard<std::mutex> lock(m_impl->outMutex);
	const size_t pending = m_impl->outBuffer.available();
	if (!pending)
	{
		return false;
	}

	if (!m_impl->socket->poll(m_impl->pollTimeout, Socket::SELECT_WRITE))
	{
		return false;
	}

	int sent = 0;
	try
	{
		sent = m_impl->socket->sendBytes(m_impl->outBuffer.data(), static_cast<int>(pending));
	}
	catch (const Poco::TimeoutException&)
	{
		return false;
	}
	catch (const Poco::Exception& exc)
	{
		Biterp::Logging::error(exc.displayText());
		if (m_impl->connection)
		{
			m_impl->connection->close();
		}
		stop.store(true, std::memory_order_relaxed);
		return false;
	}

	if (sent <= 0)
	{
		return false;
	}

	if (static_cast<size_t>(sent) >= pending)
	{
		m_impl->outBuffer.drain();
	}
	else
	{
		m_impl->outBuffer.shl(static_cast<size_t>(sent));
	}
	return true;
}
