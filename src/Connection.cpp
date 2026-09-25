#include "Connection.h"
#include <addin/biterp/Error.hpp>
#include <addin/biterp/Logger.hpp>
#include <chrono>
#include <algorithm>

#if defined(__linux__)

#include <linux/ConnectionImpl.h>

#elif defined(_WIN32) || defined(_WIN64)

#include <windows/ConnectionImpl.h>

#else
#error "Unsupported platform"
#endif

const char* const Connection::TIMEOUT_ERROR = "AMQP server timeout error";

namespace {
	// Asynchronous publications above these limits are confirmed synchronously (back pressure)
	constexpr size_t MAX_ASYNC_PENDING = 10000;
	constexpr size_t MAX_ASYNC_PENDING_BYTES = 64 * 1024 * 1024;
	// Shorter waits would expire before the IO thread takes the command
	constexpr int MIN_WAIT_MS = 20;
	// Wait for an operation the IO thread is already completing
	constexpr int CLAIMED_WAIT_MS = 30000;
	// Graceful close limit in the destructor
	constexpr int CLOSE_TIMEOUT_MS = 1000;

	// Channel ids are unique in the process, so a late callback of an old channel
	// can never be taken for a callback of a new one
	std::atomic<uint32_t> channelCounter{ 0 };

	uint32_t nextChannelId() {
		uint32_t id = ++channelCounter;
		if (id == 0) {
			id = ++channelCounter;
		}
		return id;
	}

	std::string safeText(const char* message, const char* fallback) {
		return message && *message ? std::string(message) : std::string(fallback);
	}
}

//---------------------------------------------------------------------------//
// Operation

void Operation::succeed() {
	std::lock_guard<std::mutex> lock(mutex);
	if (done) {
		return;
	}
	done = true;
	success = true;
	cv.notify_all();
}

void Operation::fail(const std::string& error) {
	std::lock_guard<std::mutex> lock(mutex);
	if (done) {
		return;
	}
	done = true;
	success = false;
	errorText = error.empty() ? std::string("Unknown AMQP error") : error;
	cv.notify_all();
}

bool Operation::wait(std::chrono::steady_clock::time_point deadline) {
	std::unique_lock<std::mutex> lock(mutex);
	return cv.wait_until(lock, deadline, [this] { return done; });
}

bool Operation::ok() {
	std::lock_guard<std::mutex> lock(mutex);
	return done && success;
}

std::string Operation::error() {
	std::lock_guard<std::mutex> lock(mutex);
	return errorText;
}

bool Operation::abandon() {
	std::lock_guard<std::mutex> lock(mutex);
	if (done || claimed) {
		return false;
	}
	gaveUp = true;
	return true;
}

bool Operation::claim() {
	std::lock_guard<std::mutex> lock(mutex);
	if (gaveUp || done) {
		return false;
	}
	claimed = true;
	return true;
}

bool Operation::abandoned() {
	std::lock_guard<std::mutex> lock(mutex);
	return gaveUp;
}

//---------------------------------------------------------------------------//
// Connection

Connection::Connection(const AMQP::Address& address, const Options& options, ConnectionListener* listener) :
	options(options),
	listener(listener),
	impl(new ConnectionImpl(address, options.heartbeat, this)),
	io(*this)
{
	if (this->options.timeoutSec < 1) {
		this->options.timeoutSec = 1;
	}
}

Connection::~Connection() {
	try {
		impl->stop(CLOSE_TIMEOUT_MS);
	}
	catch (...) {
	}
	try {
		failPendingOperations("Connection closed");
		impl.reset();
	}
	catch (...) {
	}
}

void Connection::connect() {
	impl->start(timeoutMs());
}

bool Connection::alive() const {
	std::lock_guard<std::mutex> lock(opsMutex);
	return !lost && !impl->failed();
}

std::string Connection::lostReason() const {
	std::lock_guard<std::mutex> lock(opsMutex);
	if (!lostText.empty()) {
		return lostText;
	}
	return impl->failReason();
}

OperationPtr Connection::call(const Command& command, int timeoutMs) {
	OperationPtr op;
	if (!tryCall(command, timeoutMs, op)) {
		throw Biterp::Error(TIMEOUT_ERROR);
	}
	return op;
}

bool Connection::tryCall(const Command& command, int timeoutMs, OperationPtr& result) {
	OperationPtr op = std::make_shared<Operation>();
	result = op;
	if (timeoutMs < 0) {
		timeoutMs = this->timeoutMs();
	}
	timeoutMs = std::max(timeoutMs, MIN_WAIT_MS);
	{
		std::lock_guard<std::mutex> lock(opsMutex);
		if (lost) {
			throw Biterp::Error("Connection lost: " + lostText);
		}
		pendingOps.insert(op);
	}
	bool posted = false;
	try {
		posted = impl->post([this, op, command]() {
			if (op->abandoned()) {
				// the caller already reported a timeout: do not start the operation
				return;
			}
			try {
				command(io, op);
			}
		catch (const std::exception& e) {
			op->fail(e.what());
		}
			catch (...) {
				op->fail("Unknown error");
			}
		});
	}
	catch (...) {
		std::lock_guard<std::mutex> lock(opsMutex);
		pendingOps.erase(op);
		throw;
	}
	if (!posted) {
		op->fail("Connection lost: " + lostReason());
	}
	bool finished = op->wait(std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs));
	if (!finished && !op->abandon()) {
		// the IO thread is completing the operation right now
		finished = op->wait(std::chrono::steady_clock::now() + std::chrono::milliseconds(CLAIMED_WAIT_MS));
	}
	{
		std::lock_guard<std::mutex> lock(opsMutex);
		pendingOps.erase(op);
	}
	if (!finished) {
		return false;
	}
	if (!op->ok()) {
		throw Biterp::Error(op->error());
	}
	return true;
}

bool Connection::post(const std::function<void(Io& io)>& command) {
	{
		std::lock_guard<std::mutex> lock(opsMutex);
		if (lost) {
			return false;
		}
	}
	return impl->post([this, command]() {
		try {
			command(io);
		}
		catch (const std::exception& e) {
			Biterp::Logging::error(std::string("AMQP command failed: ") + e.what());
		}
		catch (...) {
			Biterp::Logging::error("AMQP command failed: unknown error");
		}
	});
}

void Connection::failPendingOperations(const std::string& reason) {
	std::set<OperationPtr> ops;
	{
		std::lock_guard<std::mutex> lock(opsMutex);
		ops = pendingOps;
	}
	for (const auto& op : ops) {
		op->fail(reason);
	}
}

//---------------------------------------------------------------------------//
// ConnectionEvents, IO thread

void Connection::onConnectionLost(const std::string& reason) {
	{
		std::lock_guard<std::mutex> lock(opsMutex);
		if (!lost) {
			lost = true;
			lostText = reason;
		}
	}
	Biterp::Logging::error("Connection lost: " + reason);
	// channels are told after this, so the consumer sees the connection error first
	conId = 0;
	pubId = 0;
	mgmtId = 0;
	listener->onConnectionLost(reason);
	failConfirms("Connection lost: " + reason);
	failPendingOperations("Connection lost: " + reason);
}

void Connection::onConnectionBlocked(const std::string& reason) {
	blocked = true;
	blockedReason = reason;
	Biterp::Logging::warning("Connection blocked by the broker: " + reason);
}

void Connection::onConnectionUnblocked() {
	blocked = false;
	blockedReason.clear();
	Biterp::Logging::warning("Connection unblocked by the broker");
}

void Connection::onConnectionShutdown() {
	{
		std::lock_guard<std::mutex> lock(opsMutex);
		if (!lost) {
			lost = true;
			lostText = "Connection closed";
		}
	}
	failConfirms("Connection closed");
	mgmtId = 0;
	pubId = 0;
	conId = 0;
	graveyard.clear();
	mgmtChannel.reset();
	pubChannel.reset();
	conChannel.reset();
	failPendingOperations("Connection closed");
}

//---------------------------------------------------------------------------//
// Channels, IO thread

void Connection::retire(std::unique_ptr<AMQP::Channel>& channel) {
	if (!channel) {
		return;
	}
	// The channel may be retired from inside its own callback, so it is destroyed later
	graveyard.push_back(std::move(channel));
	if (graveyard.size() == 1) {
		impl->post([this]() { graveyard.clear(); });
	}
}

AMQP::Channel& Connection::publishChannel() {
	if (pubChannel && pubChannel->usable()) {
		return *pubChannel;
	}
	if (pubChannel) {
		pubId = 0;
		failConfirms("Publish channel closed");
		retire(pubChannel);
	}
	pubChannel = impl->createChannel();
	const uint32_t id = nextChannelId();
	pubId = id;
	pubSeq = 0;
	pubChannel->onError([this, id](const char* message) {
		onPublishChannelError(id, safeText(message, "Publish channel error"));
	});
	// AMQP-CPP calls the error callback right away if the channel is already unusable
	if (!pubChannel) {
		throw Biterp::Error("Publish channel is not usable: " + channelError);
	}
	pubChannel->confirmSelect()
		.onAck([this, id](uint64_t tag, bool multiple) {
			onConfirm(id, tag, multiple, true);
		})
		.onNack([this, id](uint64_t tag, bool multiple, bool /*requeue*/) {
			onConfirm(id, tag, multiple, false);
		})
		.onError([this, id](const char* message) {
			onPublishChannelError(id, safeText(message, "Publisher confirms are not supported"));
		});
	if (!pubChannel) {
		throw Biterp::Error("Publish channel is not usable: " + channelError);
	}
	return *pubChannel;
}

void Connection::onPublishChannelError(uint32_t channelId, const std::string& reason) {
	if (channelId != pubId) {
		return;
	}
	pubId = 0;
	channelError = reason;
	failConfirms(reason);
	retire(pubChannel);
}

void Connection::onManagementChannelError(uint32_t channelId, const std::string& reason) {
	if (channelId != mgmtId) {
		return;
	}
	mgmtId = 0;
	channelError = reason;
	retire(mgmtChannel);
}

void Connection::onConsumeChannelError(uint32_t channelId, const std::string& reason) {
	if (channelId != conId) {
		return;
	}
	conId = 0;
	channelError = reason;
	retire(conChannel);
	listener->onConsumeChannelClosed(channelId, reason);
}

void Connection::onConfirm(uint32_t channelId, uint64_t tag, bool multiple, bool ack) {
	if (channelId != pubId) {
		return;
	}
	auto first = confirms.begin();
	auto last = confirms.end();
	if (multiple) {
		last = confirms.upper_bound(tag);
	}
	else {
		first = confirms.find(tag);
		if (first == confirms.end()) {
			return;
		}
		last = std::next(first);
	}
	static const std::string nackText = "Message was rejected by the broker (basic.nack)";
	for (auto it = first; it != last; ++it) {
		const PendingConfirm& pending = it->second;
		if (pending.op) {
			if (ack) {
				pending.op->succeed();
			}
			else {
				pending.op->fail(nackText);
			}
		}
		else {
			if (asyncPending) {
				--asyncPending;
			}
			asyncPendingBytes -= std::min(asyncPendingBytes, pending.bytes);
			if (!ack) {
				++asyncFailed;
				asyncError = nackText;
			}
		}
	}
	confirms.erase(first, last);
	completeDrainWaiters();
}

void Connection::failConfirms(const std::string& reason) {
	for (auto& pair : confirms) {
		if (pair.second.op) {
			pair.second.op->fail(reason);
		}
		else {
			++asyncFailed;
			asyncError = reason;
		}
	}
	confirms.clear();
	asyncPending = 0;
	asyncPendingBytes = 0;
	completeDrainWaiters();
}

void Connection::completeDrainWaiters() {
	if (asyncPending != 0 || drainWaiters.empty()) {
		return;
	}
	std::vector<OperationPtr> waiters;
	for (const auto& op : drainWaiters) {
		// claim: a waiter that gave up at this very moment must not take the failure counters
		if (op->claim()) {
			waiters.push_back(op);
		}
	}
	drainWaiters.clear();
	if (waiters.empty()) {
		// nobody waits any more: keep the failure counters for the next WaitForConfirms
		return;
	}
	for (const auto& op : waiters) {
		if (asyncFailed) {
			op->fail(std::to_string(asyncFailed) + " message(s) were not confirmed by the broker: " + asyncError);
		}
		else {
			op->succeed();
		}
	}
	asyncFailed = 0;
	asyncError.clear();
}

//---------------------------------------------------------------------------//
// Io

AMQP::Channel& Connection::Io::managementChannel() {
	Connection& o = owner;
	if (o.mgmtChannel && o.mgmtChannel->usable()) {
		return *o.mgmtChannel;
	}
	if (o.mgmtChannel) {
		o.mgmtId = 0;
		o.retire(o.mgmtChannel);
	}
	o.mgmtChannel = o.impl->createChannel();
	const uint32_t id = nextChannelId();
	o.mgmtId = id;
	o.mgmtChannel->onError([&o, id](const char* message) {
		o.onManagementChannelError(id, safeText(message, "Channel error"));
	});
	// AMQP-CPP calls the error callback right away if the channel is already unusable
	if (!o.mgmtChannel) {
		throw Biterp::Error("Channel is not usable: " + o.channelError);
	}
	return *o.mgmtChannel;
}

AMQP::Channel& Connection::Io::consumeChannel() {
	Connection& o = owner;
	if (o.conChannel && o.conChannel->usable()) {
		return *o.conChannel;
	}
	if (o.conChannel) {
		const uint32_t old = o.conId;
		o.conId = 0;
		o.retire(o.conChannel);
		if (old) {
			o.listener->onConsumeChannelClosed(old, "Consumer channel closed");
		}
	}
	o.conChannel = o.impl->createChannel();
	const uint32_t id = nextChannelId();
	o.conId = id;
	o.conChannel->onError([&o, id](const char* message) {
		o.onConsumeChannelError(id, safeText(message, "Consumer channel error"));
	});
	// AMQP-CPP calls the error callback right away if the channel is already unusable
	if (!o.conChannel) {
		throw Biterp::Error("Consumer channel is not usable: " + o.channelError);
	}
	o.listener->onConsumeChannelOpened(id);
	return *o.conChannel;
}

uint32_t Connection::Io::consumeChannelId() const {
	return owner.conId;
}

AMQP::Channel* Connection::Io::consumeChannelIfAlive() {
	Connection& o = owner;
	if (o.conId && o.conChannel && o.conChannel->usable()) {
		return o.conChannel.get();
	}
	return nullptr;
}

void Connection::Io::closeConsumeChannel(const std::string& reason) {
	Connection& o = owner;
	if (!o.conChannel) {
		return;
	}
	const uint32_t id = o.conId;
	o.conId = 0;
	o.retire(o.conChannel);
	if (id) {
		o.listener->onConsumeChannelClosed(id, reason);
	}
}

bool Connection::Io::ack(uint32_t channelId, uint64_t deliveryTag, int flags) {
	AMQP::Channel* channel = consumeChannelIfAlive();
	if (!channel || channelId != owner.conId) {
		return false;
	}
	return channel->ack(deliveryTag, flags);
}

bool Connection::Io::reject(uint32_t channelId, uint64_t deliveryTag, int flags) {
	AMQP::Channel* channel = consumeChannelIfAlive();
	if (!channel || channelId != owner.conId) {
		return false;
	}
	return channel->reject(deliveryTag, flags);
}

void Connection::Io::publish(const std::string& exchange, const std::string& routingKey, const AMQP::Envelope& envelope,
	bool waitConfirm, const OperationPtr& op) {
	Connection& o = owner;
	if (o.blocked) {
		op->fail("Connection is blocked by the broker: " + o.blockedReason);
		return;
	}
	AMQP::Channel& channel = o.publishChannel();
	const uint32_t channelId = o.pubId;
	const size_t bytes = static_cast<size_t>(envelope.bodySize());
	const bool async = !waitConfirm && o.asyncPending < MAX_ASYNC_PENDING &&
		o.asyncPendingBytes + bytes <= MAX_ASYNC_PENDING_BYTES;
	if (!channel.publish(exchange, routingKey, envelope)) {
		op->fail("Publish failed: channel is not usable");
		return;
	}
	if (o.pubId != channelId) {
		// the channel failed while the message was being sent
		op->fail("Publish failed: " + o.channelError);
		return;
	}
	const uint64_t seq = ++o.pubSeq;
	PendingConfirm pending;
	pending.bytes = bytes;
	if (async) {
		o.confirms.emplace(seq, pending);
		++o.asyncPending;
		o.asyncPendingBytes += bytes;
		op->succeed();
	}
	else {
		pending.op = op;
		o.confirms.emplace(seq, pending);
	}
}

void Connection::Io::waitConfirms(const OperationPtr& op) {
	owner.drainWaiters.push_back(op);
	owner.completeDrainWaiters();
}
