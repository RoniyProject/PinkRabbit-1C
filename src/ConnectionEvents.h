#pragma once

#include <string>

/**
 * Notifications from the platform transport (ConnectionImpl) to Connection.
 * All methods are called on the IO thread.
 *
 * Contract of the platform ConnectionImpl class (windows/ConnectionImpl.h, linux/ConnectionImpl.h):
 *   ConnectionImpl(const AMQP::Address& address, uint16_t heartbeat, ConnectionEvents* events);
 *   void start(int timeoutMs);                 // 1C thread: connect, AMQP handshake, start IO thread; throws Biterp::Error
 *   bool post(std::function<void()> command);  // any thread: run command on the IO thread; false if the loop is gone
 *   void stop(int gracefulTimeoutMs);          // 1C thread: graceful close within the timeout, then join the IO thread
 *   std::unique_ptr<AMQP::Channel> createChannel(); // IO thread only
 *   bool failed() const; std::string failReason() const;
 * The AMQP connection, channels and socket are used only on the IO thread.
 */
class ConnectionEvents {
public:
	virtual ~ConnectionEvents() = default;
	// Connection is lost (socket error, heartbeat timeout, closed by the broker)
	virtual void onConnectionLost(const std::string& reason) = 0;
	// Broker sent connection.blocked / connection.unblocked
	virtual void onConnectionBlocked(const std::string& reason) = 0;
	virtual void onConnectionUnblocked() = 0;
	// IO loop is exiting: destroy all channels now, the AMQP connection is destroyed right after
	virtual void onConnectionShutdown() = 0;
};
