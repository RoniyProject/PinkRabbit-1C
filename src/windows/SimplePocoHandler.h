#ifndef SRC_SIMPLEPOCOHANDLER_H_
#define SRC_SIMPLEPOCOHANDLER_H_

#include <atomic>
#include <memory>
#include <amqpcpp.h>
#include "RabbitMQClient.h"

struct SimplePocoHandlerImpl;
class SimplePocoHandler: public AMQP::ConnectionHandler
{
public:

    static constexpr size_t BUFFER_SIZE = 32 * 1024 * 1024; //32Mb
    static constexpr size_t TEMP_BUFFER_SIZE = 2 * 1024 * 1024; //2Mb

    SimplePocoHandler(const std::string& host, uint16_t port, bool ssl, uint16_t heartbeat);
    virtual ~SimplePocoHandler();

    void setConnection(AMQP::Connection* connection);
 	void loopRead();
 	inline void stopLoop() {stop.store(true, std::memory_order_relaxed);}
	static void loopThread(SimplePocoHandler* clazz);
	void loopIteration();
    inline const std::string& getError(){ return error;}

private:

    SimplePocoHandler(const SimplePocoHandler&) = delete;
    SimplePocoHandler& operator=(const SimplePocoHandler&) = delete;

	bool sendDataFromBuffer();
    void close();

    virtual void onData(
            AMQP::Connection *connection, const char *data, size_t size) override;

    virtual void onReady(AMQP::Connection *connection) override;

    virtual void onError(AMQP::Connection *connection, const char *message) override;

    virtual void onClosed(AMQP::Connection *connection) override;

    virtual uint16_t onNegotiate(AMQP::Connection* connection, uint16_t interval) override;

    std::shared_ptr<SimplePocoHandlerImpl> m_impl;
    std::string error;
    std::atomic<bool> stop;
    uint16_t desiredHeartbeat;

};

#endif /* SRC_SIMPLEPOCOHANDLER_H_ */
