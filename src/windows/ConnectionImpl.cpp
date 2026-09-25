#include "ConnectionImpl.h"

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/NetSSL.h>
#include <Poco/Net/NetException.h>
#include <Poco/Exception.h>
#include <Poco/Timespan.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <winsock2.h>
#include <ws2tcpip.h>
#include <mstcpip.h>
#include <wincrypt.h>

#include <addin/biterp/Error.hpp>
#include <addin/biterp/Logger.hpp>
#include <addin/biterp/Utf.hpp>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <new>

#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "crypt32.lib")

using Clock = std::chrono::steady_clock;

namespace {
	constexpr size_t READ_CHUNK = 256 * 1024;
	constexpr size_t READ_BUDGET = 8 * 1024 * 1024;
	constexpr size_t MAX_IN_BUFFER = 64 * 1024 * 1024;
	constexpr size_t MAX_OUT_BUFFER = static_cast<size_t>(1024) * 1024 * 1024;
	constexpr size_t SHRINK_THRESHOLD = 4 * 1024 * 1024;
	constexpr size_t WRITE_CHUNK = 1024 * 1024;
	constexpr int MAX_POLL_MS = 1000;
	constexpr int NO_WAKE_POLL_MS = 10;
	constexpr ULONG KEEPALIVE_TIME_MS = 30000;
	constexpr ULONG KEEPALIVE_INTERVAL_MS = 5000;
	// an attempt to connect to one more address is not started with less time left
	constexpr long long MIN_ATTEMPT_US = 50 * 1000;
	// the TLS context with the loaded system certificates is rebuilt after this time
	constexpr auto TLS_CONTEXT_TTL = std::chrono::minutes(60);
	const char* const CA_FILE_ENV = "PINKRABBITMQ_TLS_CA_FILE";
	// OpenSSL level 2: RSA/DH keys of 2048 bits and more, no SHA-1 and MD5 signatures, no anonymous ciphers
	const char* const CIPHER_LIST = "HIGH:!aNULL:!eNULL:!ADH:!LOW:!EXP:!MD5:!RC4:!3DES:@STRENGTH";
	const char* const OUT_OF_MEMORY = "Out of memory in the IO thread";

	std::once_flag sslInitFlag;

	// Poco SSL is initialized once per process and never uninitialized:
	// several connections of different sessions may use it at the same time
	void ensureSslInitialized() {
		std::call_once(sslInitFlag, []() { Poco::Net::initializeSSL(); });
	}

	// Trust anchors: the Windows ROOT store. From the CA (intermediate) store only certificates
	// that are not self-signed are added: they help to build a chain but cannot end it.
	void loadSystemCertificates(SSL_CTX* ctx) {
		X509_STORE* store = SSL_CTX_get_cert_store(ctx);
		if (!store) {
			return;
		}
		const wchar_t* storeNames[] = { L"ROOT", L"CA" };
		for (const wchar_t* storeName : storeNames) {
			const bool anchors = std::wcscmp(storeName, L"ROOT") == 0;
			HCERTSTORE systemStore = CertOpenSystemStoreW(0, storeName);
			if (!systemStore) {
				continue;
			}
			PCCERT_CONTEXT cert = nullptr;
			while ((cert = CertEnumCertificatesInStore(systemStore, cert)) != nullptr) {
				const unsigned char* data = cert->pbCertEncoded;
				X509* x509 = d2i_X509(nullptr, &data, static_cast<long>(cert->cbCertEncoded));
				if (x509) {
					if (anchors || X509_check_issued(x509, x509) != X509_V_OK) {
						X509_STORE_add_cert(store, x509);
					}
					X509_free(x509);
				}
			}
			CertCloseStore(systemStore, 0);
		}
	}

	Poco::Net::Context::Ptr createTlsContext(const std::string& caFile) {
		Poco::Net::Context::Params params;
		params.verificationMode = Poco::Net::Context::VERIFY_STRICT;
		params.loadDefaultCAs = false;
		params.securityLevel = Poco::Net::Context::SECURITY_LEVEL_112_BITS;
		params.cipherList = CIPHER_LIST;
		Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::TLS_CLIENT_USE, params);
		context->requireMinimumProtocol(Poco::Net::Context::PROTO_TLSV1_2);
		// the output buffer may be reallocated between a WANT_WRITE and the retry of SSL_write
		SSL_CTX_set_mode(context->sslContext(), SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER | SSL_MODE_ENABLE_PARTIAL_WRITE);
		loadSystemCertificates(context->sslContext());
		if (!caFile.empty()) {
			if (SSL_CTX_load_verify_locations(context->sslContext(), caFile.c_str(), nullptr) != 1) {
				throw Biterp::Error(std::string("Cannot load CA certificates from ") + CA_FILE_ENV + "=" + caFile);
			}
		}
		return context;
	}

	// One TLS context for all connections of the process: loading the system stores costs
	// milliseconds, and a reconnect loop would repeat it on every attempt
	Poco::Net::Context::Ptr sharedTlsContext() {
		static std::mutex mutex;
		static Poco::Net::Context::Ptr context;
		static std::string contextCaFile;
		static Clock::time_point created;
		const std::string caFile = Biterp::Logging::environment(CA_FILE_ENV);
		std::lock_guard<std::mutex> lock(mutex);
		const auto now = Clock::now();
		if (!context || caFile != contextCaFile || now - created > TLS_CONTEXT_TTL) {
			context = createTlsContext(caFile);
			contextCaFile = caFile;
			created = now;
		}
		return context;
	}

	void setKeepAliveValues(poco_socket_t fd) {
		tcp_keepalive values = {};
		values.onoff = 1;
		values.keepalivetime = KEEPALIVE_TIME_MS;
		values.keepaliveinterval = KEEPALIVE_INTERVAL_MS;
		DWORD returned = 0;
		WSAIoctl(fd, SIO_KEEPALIVE_VALS, &values, sizeof(values), nullptr, 0, &returned, nullptr, nullptr);
	}

	// Poco 1.13 may throw IOException("Operation would block") for a full non-blocking socket
	// instead of returning a negative value
	bool isWouldBlock(const Poco::Exception& e) {
		if (e.code() == WSAEWOULDBLOCK || e.code() == WSAEINPROGRESS) {
			return true;
		}
		return e.displayText().find("would block") != std::string::npos;
	}

	bool isCertificateError(const std::string& text) {
		return text.find("certificate") != std::string::npos || text.find("Certificate") != std::string::npos;
	}

	long long remainingUs(Clock::time_point deadline) {
		return std::chrono::duration_cast<std::chrono::microseconds>(deadline - Clock::now()).count();
	}

	// getaddrinfo has no timeout, so it runs in a helper thread and the caller waits until the deadline.
	// The helper owns its data and ends by itself if the caller stopped waiting.
	std::vector<Poco::Net::SocketAddress> resolveAddresses(const std::string& host, uint16_t port, Clock::time_point deadline) {
		struct Resolution {
			std::mutex mutex;
			std::condition_variable cv;
			bool done = false;
			int code = 0;
			std::vector<Poco::Net::SocketAddress> addresses;
		};
		std::shared_ptr<Resolution> state = std::make_shared<Resolution>();
		const std::string service = std::to_string(port);
		std::thread([state, host, service]() {
			int code = 0;
			std::vector<Poco::Net::SocketAddress> addresses;
			try {
				addrinfo hints = {};
				hints.ai_family = AF_UNSPEC;
				hints.ai_socktype = SOCK_STREAM;
				hints.ai_protocol = IPPROTO_TCP;
				addrinfo* info = nullptr;
				code = getaddrinfo(host.c_str(), service.c_str(), &hints, &info);
				if (code == 0) {
					for (addrinfo* item = info; item; item = item->ai_next) {
						if (item->ai_family != AF_INET && item->ai_family != AF_INET6) {
							continue;
						}
						try {
							addresses.emplace_back(item->ai_addr, static_cast<poco_socklen_t>(item->ai_addrlen));
						}
						catch (...) {
						}
					}
					freeaddrinfo(info);
				}
			}
			catch (...) {
				code = WSA_NOT_ENOUGH_MEMORY;
			}
			std::lock_guard<std::mutex> lock(state->mutex);
			state->code = code;
			state->addresses = std::move(addresses);
			state->done = true;
			state->cv.notify_all();
		}).detach();
		std::unique_lock<std::mutex> lock(state->mutex);
		if (!state->cv.wait_until(lock, deadline, [&state] { return state->done; })) {
			throw Biterp::Error("Connection timeout: host name " + host + " was not resolved in time");
		}
		if (state->code != 0 || state->addresses.empty()) {
			throw Biterp::Error("Wrong hostname: " + host + " (error " + std::to_string(state->code) + ")");
		}
		return state->addresses;
	}

	std::string computerName() {
		wchar_t buffer[MAX_COMPUTERNAME_LENGTH + 1] = { 0 };
		DWORD size = MAX_COMPUTERNAME_LENGTH + 1;
		if (GetComputerNameW(buffer, &size)) {
			return Biterp::Utf::toUtf8(reinterpret_cast<const char16_t*>(buffer), size);
		}
		return "";
	}
}

ConnectionImpl::ConnectionImpl(const AMQP::Address& address, uint16_t heartbeat, ConnectionEvents* events) :
	host(address.hostname()),
	port(address.port()),
	secure(address.secure()),
	login(address.login()),
	vhost(address.vhost()),
	desiredHeartbeat(heartbeat),
	events(events),
	wakeSocket(static_cast<uintptr_t>(INVALID_SOCKET)),
	pollSocket(static_cast<uintptr_t>(INVALID_SOCKET))
{
}

ConnectionImpl::~ConnectionImpl() {
	try {
		stop(0);
	}
	catch (...) {
	}
}

void ConnectionImpl::connectSocket(const std::vector<Poco::Net::SocketAddress>& addresses, Clock::time_point deadline) {
	Poco::Net::Context::Ptr context;
	if (secure) {
		ensureSslInitialized();
		context = sharedTlsContext();
	}
	std::string lastError = "Connection timeout.";
	std::string certificateError;
	// try every address of the host (for example ::1 and 127.0.0.1 for localhost) within the timeout
	for (const auto& address : addresses) {
		if (remainingUs(deadline) < MIN_ATTEMPT_US) {
			break;
		}
		try {
			Poco::Net::StreamSocket plain;
			plain.connect(address, Poco::Timespan(static_cast<Poco::Timespan::TimeDiff>(remainingUs(deadline))));
			plain.setNoDelay(true);
			plain.setKeepAlive(true);
			setKeepAliveValues(plain.impl()->sockfd());
			if (secure) {
				const long long left = remainingUs(deadline);
				if (left < MIN_ATTEMPT_US) {
					break;
				}
				// the handshake with the certificate and host name check runs in blocking mode,
				// each read and write of it is limited by the rest of the timeout
				const Poco::Timespan handshake(static_cast<Poco::Timespan::TimeDiff>(std::max<long long>(left, 1000)));
				plain.setReceiveTimeout(handshake);
				plain.setSendTimeout(handshake);
				Poco::Net::SecureStreamSocket tls = Poco::Net::SecureStreamSocket::attach(plain, host, context);
				plain.setReceiveTimeout(Poco::Timespan(0));
				plain.setSendTimeout(Poco::Timespan(0));
				// SecureStreamSocketImpl reads and writes through this socket: it must be the non-blocking one,
				// otherwise every WANT_READ turns into a 100 ms wait inside Poco
				plain.setBlocking(false);
				socket.reset(new Poco::Net::SecureStreamSocket(tls));
			}
			else {
				plain.setBlocking(false);
				socket.reset(new Poco::Net::StreamSocket(plain));
			}
			pollSocket = static_cast<uintptr_t>(plain.impl()->sockfd());
			return;
		}
		catch (const Poco::Exception& e) {
			lastError = e.displayText();
			if (secure && certificateError.empty() && isCertificateError(lastError)) {
				certificateError = lastError;
			}
		}
	}
	if (!certificateError.empty()) {
		throw Biterp::Error("TLS certificate of " + host + " is rejected: " + certificateError +
			". Check that the CA certificate is in the Windows ROOT store or in the file from " + CA_FILE_ENV +
			", that the certificate matches the host name and that its key is at least RSA 2048 bits with a SHA-256 or stronger signature");
	}
	throw Biterp::Error(lastError);
}

void ConnectionImpl::start(int timeoutMs) {
	if (thread.joinable()) {
		throw Biterp::Error("Connection is already started");
	}
	// one deadline for name resolution, TCP, TLS and the AMQP handshake
	const auto deadline = Clock::now() + std::chrono::milliseconds(timeoutMs);
	WSADATA wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) == 0) {
		networkStarted = true;
	}
	try {
		const std::vector<Poco::Net::SocketAddress> addresses = resolveAddresses(host, port, deadline);
		connectSocket(addresses, deadline);
		createWakeSocket();
		inBuf.resize(READ_CHUNK);
		{
			std::lock_guard<std::mutex> lock(cmdMutex);
			accepting = true;
		}
		setState(State::Connecting);
		amqp.reset(new AMQP::Connection(this, login, vhost));
		thread = std::thread(&ConnectionImpl::run, this);
	}
	catch (const Poco::Exception& e) {
		releaseResources();
		throw Biterp::Error(e.displayText());
	}
	catch (...) {
		releaseResources();
		throw;
	}

	std::unique_lock<std::mutex> lock(stateMutex);
	const bool completed = stateCv.wait_until(lock, deadline, [this] {
		return state == State::Ready || state == State::Failed || state == State::Closed;
	});
	const State current = state;
	const std::string reason = failText;
	lock.unlock();
	if (current != State::Ready) {
		stop(0);
		if (!completed) {
			throw Biterp::Error("Connection timeout.");
		}
		throw Biterp::Error(reason.empty() ? std::string("Connection failed") : reason);
	}
}

bool ConnectionImpl::post(std::function<void()> command) {
	{
		std::lock_guard<std::mutex> lock(cmdMutex);
		if (!accepting) {
			return false;
		}
		commands.push_back(std::move(command));
	}
	wake();
	return true;
}

void ConnectionImpl::stop(int gracefulTimeoutMs) {
	if (thread.joinable()) {
		if (gracefulTimeoutMs > 0 && getState() == State::Ready && !failPending) {
			const bool posted = post([this]() {
				if (amqp && getState() == State::Ready) {
					setState(State::Closing);
					if (!amqp->close()) {
						setState(State::Closed);
						exitLoop = true;
					}
				}
			});
			if (posted) {
				std::unique_lock<std::mutex> lock(stateMutex);
				stateCv.wait_until(lock, Clock::now() + std::chrono::milliseconds(gracefulTimeoutMs), [this] {
					return state == State::Closed || state == State::Failed;
				});
			}
		}
		stopRequested = true;
		wake();
		thread.join();
	}
	releaseResources();
}

std::unique_ptr<AMQP::Channel> ConnectionImpl::createChannel() {
	if (!amqp) {
		throw Biterp::Error("Connection is closed");
	}
	return std::unique_ptr<AMQP::Channel>(new AMQP::Channel(amqp.get()));
}

bool ConnectionImpl::failed() const {
	return failPending.load() || getState() == State::Failed;
}

std::string ConnectionImpl::failReason() const {
	std::lock_guard<std::mutex> lock(stateMutex);
	if (failText.empty() && outOfMemory.load()) {
		return OUT_OF_MEMORY;
	}
	return failText;
}

//---------------------------------------------------------------------------//
// AMQP::ConnectionHandler, IO thread

void ConnectionImpl::onProperties(AMQP::Connection* /*connection*/, const AMQP::Table& /*server*/, AMQP::Table& client) {
	client.set("product", "PinkRabbitMQ 1C");
	client.set("connection_name", "1C PinkRabbitMQ " + computerName() + " pid " + std::to_string(GetCurrentProcessId()));
}

uint16_t ConnectionImpl::onNegotiate(AMQP::Connection* /*connection*/, uint16_t interval) {
	heartbeat = desiredHeartbeat > 0 ? desiredHeartbeat : interval;
	return heartbeat;
}

void ConnectionImpl::onData(AMQP::Connection* /*connection*/, const char* buffer, size_t size) {
	// called from AMQP-CPP code, also from destructors: must not throw
	try {
		outBuf.insert(outBuf.end(), buffer, buffer + size);
		if (outBuf.size() - outHead > MAX_OUT_BUFFER) {
			requestFail("Outgoing buffer overflow: the broker does not read data");
		}
	}
	catch (...) {
		failNoAlloc();
	}
}

void ConnectionImpl::onReady(AMQP::Connection* /*connection*/) {
	const auto now = Clock::now();
	lastRecv = now;
	lastSend = now;
	lastHeartbeat = now;
	setState(State::Ready);
}

void ConnectionImpl::onError(AMQP::Connection* /*connection*/, const char* message) {
	requestFail(message && *message ? std::string(message) : std::string("AMQP connection error"));
}

void ConnectionImpl::onClosed(AMQP::Connection* /*connection*/) {
	if (getState() == State::Closing) {
		setState(State::Closed);
		exitLoop = true;
	}
	else {
		requestFail("Connection closed by server");
	}
}

void ConnectionImpl::onBlocked(AMQP::Connection* /*connection*/, const char* reason) {
	events->onConnectionBlocked(reason ? std::string(reason) : std::string());
}

void ConnectionImpl::onUnblocked(AMQP::Connection* /*connection*/) {
	events->onConnectionUnblocked();
}

//---------------------------------------------------------------------------//
// IO loop

void ConnectionImpl::run() {
	try {
		try {
			runLoop();
		}
		catch (const std::exception& e) {
			requestFail(std::string("IO loop error: ") + e.what());
		}
		catch (...) {
			requestFail("IO loop error");
		}
	}
	catch (...) {
		// the error text itself could not be built
		failNoAlloc();
	}
	try {
		if (failPending) {
			processFailure();
		}
	}
	catch (...) {
	}
	finish();
}

void ConnectionImpl::runLoop() {
	const auto now = Clock::now();
	lastRecv = now;
	lastSend = now;
	lastHeartbeat = now;
	while (true) {
		// a detected failure is processed before any further command runs on the dead connection
		if (failPending) {
			processFailure();
		}
		if (exitLoop || stopRequested) {
			return;
		}
		runCommands();
		if (failPending) {
			processFailure();
		}
		if (exitLoop || stopRequested) {
			return;
		}
		writeSocket();
		if (failPending) {
			continue;
		}

		WSAPOLLFD fds[2] = {};
		ULONG count = 1;
		fds[0].fd = static_cast<SOCKET>(pollSocket);
		fds[0].events = POLLRDNORM | (wantWrite() ? POLLWRNORM : 0);
		if (wakeSocket != static_cast<uintptr_t>(INVALID_SOCKET)) {
			fds[1].fd = static_cast<SOCKET>(wakeSocket);
			fds[1].events = POLLRDNORM;
			count = 2;
		}
		const int rc = WSAPoll(fds, count, pollTimeoutMs());
		if (rc == SOCKET_ERROR) {
			const int err = WSAGetLastError();
			if (err != WSAEINTR) {
				requestFail("Socket poll failed, error " + std::to_string(err));
			}
			continue;
		}
		if (count == 2 && fds[1].revents) {
			drainWake();
		}
		const SHORT revents = fds[0].revents;
		if (revents & POLLNVAL) {
			requestFail("Socket is not valid");
			continue;
		}
		if ((revents & (POLLRDNORM | POLLHUP | POLLERR)) || moreToRead || (recvWantsWrite && (revents & POLLWRNORM))) {
			const bool progress = readSocket();
			if (!progress && !failPending && (revents & (POLLHUP | POLLERR))) {
				// hang-up without data or error from recv: do not spin on the same event
				requestFail("Connection closed by server (socket hang-up)");
			}
		}
		if (!failPending && (revents & POLLWRNORM)) {
			writeSocket();
		}
		// after reading: data that arrived while the thread was not running counts as received
		if (!failPending) {
			checkHeartbeat();
		}
	}
}

void ConnectionImpl::finish() {
	try {
		std::deque<std::function<void()>> dropped;
		{
			std::lock_guard<std::mutex> lock(cmdMutex);
			accepting = false;
			dropped.swap(commands);
		}
		dropped.clear();
	}
	catch (...) {
	}
	try {
		events->onConnectionShutdown();
	}
	catch (...) {
	}
	try {
		amqp.reset();
	}
	catch (...) {
	}
	try {
		// best effort, without waiting: connection.close-ok for a broker that closed the connection
		if (socket && !outBuf.empty()) {
			writeSocket();
		}
	}
	catch (...) {
	}
	closeSocket();
	try {
		std::lock_guard<std::mutex> lock(stateMutex);
		if (state != State::Failed) {
			state = State::Closed;
		}
	}
	catch (...) {
	}
	stateCv.notify_all();
}

void ConnectionImpl::runCommands() {
	std::deque<std::function<void()>> batch;
	{
		std::lock_guard<std::mutex> lock(cmdMutex);
		batch.swap(commands);
	}
	for (auto& command : batch) {
		try {
			command();
		}
		catch (const std::exception& e) {
			Biterp::Logging::error(std::string("AMQP command failed: ") + e.what());
		}
		catch (...) {
			Biterp::Logging::error("AMQP command failed: unknown error");
		}
	}
}

bool ConnectionImpl::readSocket() {
	bool progress = false;
	moreToRead = false;
	sendWantsRead = false;
	recvWantsWrite = false;
	size_t budget = READ_BUDGET;
	while (true) {
		if (inBuf.size() - inUsed < READ_CHUNK) {
			const size_t need = inUsed + READ_CHUNK;
			if (need > MAX_IN_BUFFER + READ_CHUNK) {
				requestFail("Incoming AMQP frame is too large");
				return true;
			}
			inBuf.resize(std::min(std::max(need, inBuf.size() * 2), MAX_IN_BUFFER + READ_CHUNK));
		}
		int received = 0;
		try {
			received = socket->receiveBytes(inBuf.data() + inUsed, static_cast<int>(inBuf.size() - inUsed));
		}
		catch (const Poco::TimeoutException&) {
			return progress;
		}
		catch (const Poco::Exception& e) {
			if (isWouldBlock(e)) {
				return progress;
			}
			requestFail(e.displayText());
			return true;
		}
		if (received > 0) {
			progress = true;
			inUsed += static_cast<size_t>(received);
			lastRecv = Clock::now();
			parseInput();
			if (failPending || exitLoop) {
				return true;
			}
			if (static_cast<size_t>(received) >= budget) {
				// give the commands a chance, continue reading without waiting
				moreToRead = true;
				return true;
			}
			budget -= static_cast<size_t>(received);
			continue;
		}
		if (received == 0) {
			requestFail(getState() == State::Closing ? "Connection closed" : "Connection closed by server");
			return true;
		}
		// negative: no data now (for TLS: WANT_READ or WANT_WRITE)
		if (secure && received == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE) {
			recvWantsWrite = true;
		}
		return progress;
	}
}

void ConnectionImpl::parseInput() {
	size_t offset = 0;
	try {
		while (offset < inUsed && amqp) {
			const uint64_t processed = amqp->parse(inBuf.data() + offset, inUsed - offset);
			if (processed == 0) {
				break;
			}
			offset += static_cast<size_t>(std::min<uint64_t>(processed, inUsed - offset));
			if (failPending || exitLoop) {
				break;
			}
		}
	}
	catch (const std::exception& e) {
		requestFail(std::string("AMQP parse error: ") + e.what());
	}
	catch (...) {
		requestFail("AMQP parse error");
	}
	if (offset > 0) {
		const size_t rest = inUsed - offset;
		if (rest) {
			std::memmove(inBuf.data(), inBuf.data() + offset, rest);
		}
		inUsed = rest;
	}
	if (inBuf.size() > SHRINK_THRESHOLD && inUsed < READ_CHUNK) {
		std::vector<char> smaller(READ_CHUNK);
		if (inUsed) {
			std::memcpy(smaller.data(), inBuf.data(), inUsed);
		}
		inBuf.swap(smaller);
	}
}

void ConnectionImpl::writeSocket() {
	while (outHead < outBuf.size() && !sendWantsRead) {
		const size_t length = std::min(outBuf.size() - outHead, WRITE_CHUNK);
		int sent = 0;
		try {
			sent = socket->sendBytes(outBuf.data() + outHead, static_cast<int>(length));
		}
		catch (const Poco::TimeoutException&) {
			break;
		}
		catch (const Poco::Exception& e) {
			if (isWouldBlock(e)) {
				break;
			}
			requestFail(e.displayText());
			return;
		}
		if (sent > 0) {
			outHead += static_cast<size_t>(sent);
			lastSend = Clock::now();
			continue;
		}
		// negative or zero: socket buffer is full (for TLS: WANT_WRITE or WANT_READ)
		if (secure && sent == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_READ) {
			sendWantsRead = true;
		}
		break;
	}
	if (outHead == outBuf.size()) {
		outBuf.clear();
		outHead = 0;
		if (outBuf.capacity() > SHRINK_THRESHOLD) {
			std::vector<char>().swap(outBuf);
		}
	}
	else if (outHead > WRITE_CHUNK && outHead * 2 > outBuf.size()) {
		outBuf.erase(outBuf.begin(), outBuf.begin() + static_cast<std::ptrdiff_t>(outHead));
		outHead = 0;
	}
}

void ConnectionImpl::checkHeartbeat() {
	if (heartbeat == 0 || !amqp || getState() != State::Ready) {
		return;
	}
	const auto now = Clock::now();
	if (now - lastRecv > std::chrono::seconds(2 * static_cast<int>(heartbeat))) {
		requestFail("Heartbeat timeout: no data from the server for " + std::to_string(2 * static_cast<int>(heartbeat)) + " s");
		return;
	}
	if (now - std::max(lastSend, lastHeartbeat) >= std::chrono::milliseconds(500 * static_cast<int>(heartbeat))) {
		lastHeartbeat = now;
		amqp->heartbeat();
	}
}

int ConnectionImpl::pollTimeoutMs() const {
	if (moreToRead) {
		return 0;
	}
	int timeout = MAX_POLL_MS;
	// heartbeat deadlines matter only on a ready connection; in other states they would give a zero timeout
	if (heartbeat && getState() == State::Ready) {
		const auto now = Clock::now();
		const auto nextSend = std::max(lastSend, lastHeartbeat) + std::chrono::milliseconds(500 * static_cast<int>(heartbeat));
		const auto deadline = lastRecv + std::chrono::seconds(2 * static_cast<int>(heartbeat));
		const auto next = std::min(nextSend, deadline);
		const long long wait = std::chrono::duration_cast<std::chrono::milliseconds>(next - now).count() + 1;
		timeout = static_cast<int>(std::max<long long>(1, std::min<long long>(timeout, wait)));
	}
	if (wakeSocket == static_cast<uintptr_t>(INVALID_SOCKET)) {
		timeout = std::min(timeout, NO_WAKE_POLL_MS);
	}
	return timeout;
}

bool ConnectionImpl::wantWrite() const {
	return (outHead < outBuf.size() && !sendWantsRead) || recvWantsWrite;
}

void ConnectionImpl::requestFail(const std::string& reason) {
	try {
		std::lock_guard<std::mutex> lock(stateMutex);
		if (!failPending) {
			failText = reason;
			failPending = true;
		}
	}
	catch (...) {
		failNoAlloc();
	}
}

void ConnectionImpl::failNoAlloc() noexcept {
	outOfMemory = true;
	failPending = true;
}

void ConnectionImpl::processFailure() {
	if (failureProcessed) {
		return;
	}
	failureProcessed = true;
	exitLoop = true;
	const bool closing = getState() == State::Closing;
	const std::string reason = failReason();
	setState(closing ? State::Closed : State::Failed);
	if (!closing) {
		try {
			events->onConnectionLost(reason);
		}
		catch (...) {
		}
	}
	if (amqp) {
		try {
			amqp->fail(reason.c_str());
		}
		catch (...) {
		}
	}
}

void ConnectionImpl::setState(State newState) {
	{
		std::lock_guard<std::mutex> lock(stateMutex);
		state = newState;
	}
	stateCv.notify_all();
}

ConnectionImpl::State ConnectionImpl::getState() const {
	std::lock_guard<std::mutex> lock(stateMutex);
	return state;
}

//---------------------------------------------------------------------------//
// wake-up socket

void ConnectionImpl::createWakeSocket() {
	SOCKET s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (s == INVALID_SOCKET) {
		Biterp::Logging::warning("Cannot create wake-up socket, error " + std::to_string(WSAGetLastError()));
		return;
	}
	BOOL exclusive = TRUE;
	::setsockopt(s, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, reinterpret_cast<const char*>(&exclusive), sizeof(exclusive));
	sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;
	int length = sizeof(addr);
	u_long nonBlocking = 1;
	if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
		::getsockname(s, reinterpret_cast<sockaddr*>(&addr), &length) != 0 ||
		::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
		::ioctlsocket(s, FIONBIO, &nonBlocking) != 0) {
		Biterp::Logging::warning("Cannot set up wake-up socket, error " + std::to_string(WSAGetLastError()));
		::closesocket(s);
		return;
	}
	wakeSocket = static_cast<uintptr_t>(s);
}

void ConnectionImpl::closeWakeSocket() {
	if (wakeSocket != static_cast<uintptr_t>(INVALID_SOCKET)) {
		::closesocket(static_cast<SOCKET>(wakeSocket));
		wakeSocket = static_cast<uintptr_t>(INVALID_SOCKET);
	}
}

void ConnectionImpl::wake() {
	if (wakeSocket == static_cast<uintptr_t>(INVALID_SOCKET)) {
		return;
	}
	if (wakePending.exchange(true)) {
		return;
	}
	const char byte = 1;
	if (::send(static_cast<SOCKET>(wakeSocket), &byte, 1, 0) != 1) {
		// nothing was sent: the next post must try again
		wakePending.store(false);
	}
}

void ConnectionImpl::drainWake() {
	// read first, then allow new wake-ups: a byte sent after this point is not consumed here,
	// and a command posted before it is executed by runCommands right after this call
	char buffer[64];
	while (::recv(static_cast<SOCKET>(wakeSocket), buffer, sizeof(buffer), 0) > 0) {
	}
	wakePending.store(false);
}

void ConnectionImpl::closeSocket() {
	if (!socket) {
		return;
	}
	try {
		socket->close();
	}
	catch (...) {
	}
	socket.reset();
	pollSocket = static_cast<uintptr_t>(INVALID_SOCKET);
}

void ConnectionImpl::releaseResources() {
	try {
		amqp.reset();
	}
	catch (...) {
	}
	closeSocket();
	closeWakeSocket();
	if (networkStarted) {
		WSACleanup();
		networkStarted = false;
	}
}
