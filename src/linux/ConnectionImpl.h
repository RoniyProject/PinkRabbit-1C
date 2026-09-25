#pragma once

#include <amqpcpp.h>
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <functional>
#include <atomic>
#include <string>
#include <chrono>
#include "TCPHandler.h"
#include "../ConnectionEvents.h"

struct event;

/**
 * Linux transport: AMQP::TcpConnection on a libevent loop owned by one IO thread.
 * Commands are passed through an eventfd, a timer sends heartbeats. Nothing else touches
 * the event base, so libevent does not need its thread support.
 */
class ConnectionImpl {
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
    friend class TCPHandler;
    enum class State { Idle, Connecting, Ready, Closing, Closed, Failed };

    // TCPHandler callbacks, IO thread
    uint16_t negotiate(uint16_t interval);
    void connected(int fd);
    void ready();
    void error(const std::string& message);
    void closed();
    void lost();
    void blocked(const std::string& reason);
    void unblocked();
    const std::string& hostName() const { return host; }

    static void wakeCallback(int fd, short what, void* arg);
    static void timerCallback(int fd, short what, void* arg);
    void run();
    void onWake();
    void onTimer();
    void runCommands();
    void requestFail(const std::string& reason);
    void processFailure();
    void breakLoopIfDone();
    void finish();
    void setState(State state);
    State getState() const;
    void wake();
    void releaseResources();

private:
    AMQP::Address address;
    std::string host;
    uint16_t desiredHeartbeat;
    ConnectionEvents* events;

    struct event_base* eventLoop = nullptr;
    struct event* wakeEvent = nullptr;
    struct event* timerEvent = nullptr;
    int wakeFd = -1;
    std::unique_ptr<TCPHandler> handler;
    std::unique_ptr<AMQP::TcpConnection> connection;
    std::thread thread;

    std::mutex cmdMutex;
    std::deque<std::function<void()>> commands;
    bool accepting = false;

    mutable std::mutex stateMutex;
    std::condition_variable stateCv;
    State state = State::Idle;
    std::atomic<bool> failPending{ false };
    std::atomic<bool> stopRequested{ false };
    std::string failText;
    bool failureProcessed = false;
    bool exitLoop = false;

    // IO thread only
    uint16_t heartbeat = 0;
    std::chrono::steady_clock::time_point lastHeartbeat;
};
