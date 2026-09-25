#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <map>
#include <set>
#include <vector>
#include <atomic>
#include <amqpcpp.h>
#include "ConnectionEvents.h"

class ConnectionImpl;

/**
 * State of one AMQP operation. It is completed once on the IO thread and waited for
 * on the 1C thread. Result fields are written before succeed() and read after wait().
 */
class Operation {
public:
	void succeed();
	void fail(const std::string& error);
	// true if the operation completed before the deadline
	bool wait(std::chrono::steady_clock::time_point deadline);
	bool ok();
	std::string error();
	// The waiter gives up (timeout). Returns false if the IO thread already claimed or completed
	// the operation: then the waiter must wait for the result instead of reporting a timeout.
	bool abandon();
	bool abandoned();
	// The IO thread takes the right to complete an operation with side effects (for example a started
	// consumer). Returns false if the waiter already gave up: the side effect must be undone.
	bool claim();

	std::string strValue;
	uint64_t numValue = 0;

private:
	std::mutex mutex;
	std::condition_variable cv;
	bool done = false;
	bool success = false;
	bool gaveUp = false;
	bool claimed = false;
	std::string errorText;
};

using OperationPtr = std::shared_ptr<Operation>;

/**
 * Notifications from Connection to the component. Called on the IO thread.
 */
class ConnectionListener {
public:
	virtual ~ConnectionListener() = default;
	virtual void onConsumeChannelOpened(uint32_t channelId) = 0;
	virtual void onConsumeChannelClosed(uint32_t channelId, const std::string& reason) = 0;
	virtual void onConnectionLost(const std::string& reason) = 0;
};

/**
 * AMQP connection with a dedicated IO thread.
 * All AMQP-CPP objects (connection, channels) live on the IO thread. The 1C thread only posts
 * commands and waits for their Operation with a deadline.
 */
class Connection : private ConnectionEvents {
public:
	static const char* const TIMEOUT_ERROR;

	struct Options {
		int timeoutSec = 5;
		uint16_t heartbeat = 0; // 0 - accept the value proposed by the server
	};

	/**
	 * Access to channels for commands. Valid only on the IO thread inside a command.
	 */
	class Io {
	public:
		explicit Io(Connection& owner) : owner(owner) {}
		// Channel for declare/delete/bind operations
		AMQP::Channel& managementChannel();
		// Channel for consumers, acks and rejects
		AMQP::Channel& consumeChannel();
		uint32_t consumeChannelId() const;
		// Current consumer channel or nullptr, does not open a new one
		AMQP::Channel* consumeChannelIfAlive();
		void closeConsumeChannel(const std::string& reason);
		// Ack or reject a delivery if it belongs to the current consumer channel
		bool ack(uint32_t channelId, uint64_t deliveryTag, int flags);
		bool reject(uint32_t channelId, uint64_t deliveryTag, int flags);
		// Publish with publisher confirms. With waitConfirm the operation completes on basic.ack,
		// otherwise right after the message is handed to the socket buffer.
		void publish(const std::string& exchange, const std::string& routingKey, const AMQP::Envelope& envelope,
			bool waitConfirm, const OperationPtr& op);
		// Complete the operation when all asynchronous publications are confirmed
		void waitConfirms(const OperationPtr& op);
	private:
		Connection& owner;
	};

	using Command = std::function<void(Io& io, const OperationPtr& op)>;

	Connection(const AMQP::Address& address, const Options& options, ConnectionListener* listener);
	virtual ~Connection();

	void connect();
	bool alive() const;
	std::string lostReason() const;
	int timeoutMs() const { return options.timeoutSec * 1000; }

	/**
	 * Run the command on the IO thread and wait for its operation.
	 * Throws Biterp::Error on failure and TIMEOUT_ERROR on timeout.
	 * @param timeoutMs - wait limit, negative means the connection timeout
	 */
	OperationPtr call(const Command& command, int timeoutMs = -1);

	/**
	 * Same as call, but returns false on timeout instead of throwing.
	 */
	bool tryCall(const Command& command, int timeoutMs, OperationPtr& result);

	/**
	 * Run the command on the IO thread without waiting. Returns false if the connection is gone.
	 */
	bool post(const std::function<void(Io& io)>& command);

private:
	// ConnectionEvents, IO thread
	void onConnectionLost(const std::string& reason) override;
	void onConnectionBlocked(const std::string& reason) override;
	void onConnectionUnblocked() override;
	void onConnectionShutdown() override;

	// IO thread helpers
	AMQP::Channel& publishChannel();
	void retire(std::unique_ptr<AMQP::Channel>& channel);
	void onPublishChannelError(uint32_t channelId, const std::string& reason);
	void onManagementChannelError(uint32_t channelId, const std::string& reason);
	void onConsumeChannelError(uint32_t channelId, const std::string& reason);
	void onConfirm(uint32_t channelId, uint64_t tag, bool multiple, bool ack);
	void failConfirms(const std::string& reason);
	void completeDrainWaiters();
	void failPendingOperations(const std::string& reason);

private:
	Options options;
	ConnectionListener* listener;
	std::unique_ptr<ConnectionImpl> impl;
	Io io;

	// guarded by opsMutex
	mutable std::mutex opsMutex;
	std::set<OperationPtr> pendingOps;
	bool lost = false;
	std::string lostText;

	// IO thread only
	std::unique_ptr<AMQP::Channel> mgmtChannel;
	uint32_t mgmtId = 0;
	std::unique_ptr<AMQP::Channel> pubChannel;
	uint32_t pubId = 0;
	uint64_t pubSeq = 0;
	struct PendingConfirm {
		OperationPtr op; // nullptr for asynchronous publications
		size_t bytes = 0;
	};
	std::map<uint64_t, PendingConfirm> confirms;
	size_t asyncPending = 0;
	size_t asyncPendingBytes = 0;
	size_t asyncFailed = 0;
	std::string asyncError;
	std::vector<OperationPtr> drainWaiters;
	std::unique_ptr<AMQP::Channel> conChannel;
	uint32_t conId = 0;
	std::vector<std::unique_ptr<AMQP::Channel>> graveyard;
	std::string channelError; // reason of the last channel failure
	bool blocked = false;
	std::string blockedReason;
};
