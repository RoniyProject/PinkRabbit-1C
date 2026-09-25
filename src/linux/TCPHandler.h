#pragma once

#include <amqpcpp.h>
#include <amqpcpp/libevent.h>
#include <string>
#include <openssl/ssl.h>

class ConnectionImpl;

/**
 * libevent handler that forwards connection events to ConnectionImpl.
 * All callbacks are called on the IO thread.
 */
class TCPHandler: public AMQP::LibEventHandler{
public:

    TCPHandler(struct event_base *evbase, ConnectionImpl* owner): AMQP::LibEventHandler(evbase), owner(owner) {}

    void onConnected(AMQP::TcpConnection *connection) override;
    void onProperties(AMQP::TcpConnection *connection, const AMQP::Table &server, AMQP::Table &client) override;
    uint16_t onNegotiate(AMQP::TcpConnection *connection, uint16_t interval) override;
    void onReady(AMQP::TcpConnection *connection) override;
    void onError(AMQP::TcpConnection *connection, const char *message) override;
    void onClosed(AMQP::TcpConnection *connection) override;
    void onBlocked(AMQP::TcpConnection *connection, const char *reason) override;
    void onUnblocked(AMQP::TcpConnection *connection) override;
    void onLost(AMQP::TcpConnection *connection) override;

private:
    ConnectionImpl* owner;
};
