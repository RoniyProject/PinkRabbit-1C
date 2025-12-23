#pragma once

#include <string>
#include <mutex>
#include <condition_variable>
#include <amqpcpp.h>

class ConnectionImpl;

class Connection {
public:
	Connection(const AMQP::Address& address, int timeout, uint16_t heartbeat = 0);
	virtual ~Connection();
	void connect();
	AMQP::Channel* channel();
	AMQP::Channel* readChannel();
	void loop();
	void loopbreak(std::string error = "");

private:
	ConnectionImpl* pimpl;
	int timeout;
	uint16_t heartbeat;
	volatile bool broken;
	std::string error;
	std::mutex _mutex;
	std::condition_variable cvBroken;
};
