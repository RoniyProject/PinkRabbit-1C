#include "ConnectionImpl.h"
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>

ConnectionImpl::ConnectionImpl(const AMQP::Address& address) : 
	handler(address.hostname(), address.port(), address.secure()),
	trChannel(nullptr)
{
	connection.reset(new AMQP::Connection(&handler, address.login(), address.vhost()));
	handler.setConnection(connection.get());
	thread = std::thread(SimplePocoHandler::loopThread, &handler);
}

ConnectionImpl::~ConnectionImpl() {
	closeChannel(trChannel);
	closeChannel(rcChannel);
	if (connection && connection->usable()) {
		// Try to close AMQP connection gracefully before stopping the IO loop
		connection->close();
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
	}
	handler.stopLoop();
	thread.join();
	connection.reset(nullptr);
}

void ConnectionImpl::openChannel(std::unique_ptr<AMQP::Channel>& channel) {
	if (channel) {
		closeChannel(channel);
	}
	if (!connection->usable()) {
		throw Biterp::Error("Connection lost");
	}
	struct ChannelOpenState
	{
		std::mutex mutex;
		std::condition_variable cv;
		bool ready = false;
	};
	auto state = std::make_shared<ChannelOpenState>();
	channel.reset(new AMQP::Channel(connection.get()));
	channel->onReady([state]() {
		std::unique_lock<std::mutex> lock(state->mutex);
		state->ready = true;
		state->cv.notify_all();
		});
	channel->onError([state, &channel](const char* message) {
		Biterp::Logging::error("Channel closed with reason: " + std::string(message));
		channel.reset(nullptr);
		std::unique_lock<std::mutex> lock(state->mutex);
		state->ready = true;
		state->cv.notify_all();
		});
	std::unique_lock<std::mutex> lock(state->mutex);
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(5000);
	if (!state->cv.wait_until(lock, deadline, [&] { return state->ready; }))
	{
		channel.reset(nullptr);
		throw Biterp::Error("Channel open timeout");
	}
	if (!channel) {
		throw Biterp::Error("Channel not opened");
	}
}

void ConnectionImpl::closeChannel(std::unique_ptr<AMQP::Channel>& channel) {
	if (!channel) {
		return;
	}
	if (channel->usable()) {
		struct ChannelCloseState
		{
			std::mutex mutex;
			std::condition_variable cv;
			bool done = false;
		};
		auto state = std::make_shared<ChannelCloseState>();
		channel->close()
			.onSuccess([state]()
				{
					std::unique_lock<std::mutex> lock(state->mutex);
					state->done = true;
					state->cv.notify_all();
				})
			.onError([state](const char* message)
				{
					Biterp::Logging::error("Channel close failed: " + std::string(message));
					std::unique_lock<std::mutex> lock(state->mutex);
					state->done = true;
					state->cv.notify_all();
				});
		std::unique_lock<std::mutex> lock(state->mutex);
		state->cv.wait_until(lock, std::chrono::steady_clock::now() + std::chrono::milliseconds(1500), [&] { return state->done; });
	}
	channel.reset(nullptr);
}


void ConnectionImpl::connect() {
	const uint16_t timeout = 15000;
	std::chrono::milliseconds timeoutMs{ timeout };
	auto end = std::chrono::system_clock::now() + timeoutMs;
	while (connection->waiting() && (end - std::chrono::system_clock::now()).count() > 0) {
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
	if (!connection->ready()) {
		if (!handler.getError().empty()){
			throw Biterp::Error(handler.getError());
		}
		throw Biterp::Error("Connection timeout.");
	}
}


AMQP::Channel* ConnectionImpl::channel() {
	if (!trChannel || !trChannel->usable()) {
		openChannel(trChannel);
	}
	return trChannel.get();
}


AMQP::Channel* ConnectionImpl::readChannel() {
	if (!rcChannel || !rcChannel->usable()) {
		openChannel(rcChannel);
	}
	return rcChannel.get();
}
