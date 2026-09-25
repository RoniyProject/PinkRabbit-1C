#pragma once

#include <amqpcpp.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <functional>
#include <atomic>
#include <memory>
#include <vector>
#include <string>
#include <chrono>
#include "../ConnectionEvents.h"

namespace Poco {
	namespace Net {
		class StreamSocket;
		class SocketAddress;
	}
}

/**
 * Windows transport: Poco socket (plain or TLS) and AMQP::Connection owned by one IO thread.
 * The IO thread waits in WSAPoll on the AMQP socket and on a loopback UDP socket that is used
 * to wake it up when a command is posted. No polling with sleeps.
 */
class ConnectionImpl : public AMQP::ConnectionHandler {
public:
	ConnectionImpl(const AMQP::Address& address, uint16_t heartbeat, ConnectionEvents* events);
	virtual ~ConnectionImpl();

	void start(int timeoutMs);
	bool post(std::function<void()> command);
	void stop(int gracefulTimeoutMs);
	std::unique_ptr<AMQP::Channel> createChannel();
	bool failed() const;
	std::string failReason() const;

private:
	enum class State { Idle, Connecting, Ready, Closing, Closed, Failed };

	// AMQP::ConnectionHandler, IO thread
	void onProperties(AMQP::Connection* connection, const AMQP::Table& server, AMQP::Table& client) override;
	uint16_t onNegotiate(AMQP::Connection* connection, uint16_t interval) override;
	void onData(AMQP::Connection* connection, const char* buffer, size_t size) override;
	void onReady(AMQP::Connection* connection) override;
	void onError(AMQP::Connection* connection, const char* message) override;
	void onClosed(AMQP::Connection* connection) override;
	void onBlocked(AMQP::Connection* connection, const char* reason) override;
	void onUnblocked(AMQP::Connection* connection) override;

	void connectSocket(const std::vector<Poco::Net::SocketAddress>& addresses, std::chrono::steady_clock::time_point deadline);
	void run();
	void runLoop();
	void finish();
	void runCommands();
	bool readSocket();
	void parseInput();
	void writeSocket();
	void checkHeartbeat();
	int pollTimeoutMs() const;
	bool wantWrite() const;
	void requestFail(const std::string& reason);
	void failNoAlloc() noexcept;
	void processFailure();
	void setState(State state);
	State getState() const;
	void wake();
	void drainWake();
	void createWakeSocket();
	void closeWakeSocket();
	void closeSocket();
	void releaseResources();

private:
	std::string host;
	uint16_t port;
	bool secure;
	AMQP::Login login;
	std::string vhost;
	uint16_t desiredHeartbeat;
	ConnectionEvents* events;

	std::unique_ptr<Poco::Net::StreamSocket> socket;
	std::unique_ptr<AMQP::Connection> amqp;
	std::thread thread;
	bool networkStarted = false;

	// wake-up socket and the socket to poll (SOCKET stored as uintptr_t to keep winsock out of the header)
	uintptr_t wakeSocket;
	uintptr_t pollSocket;
	std::atomic<bool> wakePending{ false };

	// commands
	std::mutex cmdMutex;
	std::deque<std::function<void()>> commands;
	bool accepting = false;

	// state
	mutable std::mutex stateMutex;
	std::condition_variable stateCv;
	State state = State::Idle;
	std::atomic<bool> failPending{ false };
	std::atomic<bool> stopRequested{ false };
	std::atomic<bool> outOfMemory{ false };
	std::string failText;
	bool failureProcessed = false;
	bool exitLoop = false;

	// IO thread only
	std::vector<char> inBuf;
	size_t inUsed = 0;
	std::vector<char> outBuf;
	size_t outHead = 0;
	bool moreToRead = false;
	bool sendWantsRead = false;
	bool recvWantsWrite = false;
	uint16_t heartbeat = 0;
	std::chrono::steady_clock::time_point lastRecv;
	std::chrono::steady_clock::time_point lastSend;
	std::chrono::steady_clock::time_point lastHeartbeat;
};
