#include "ConnectionImpl.h"
#include <addin/biterp/Error.hpp>
#include <addin/biterp/Logger.hpp>
#include <event2/event.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <unistd.h>
#include <cstdint>
#include <cstring>

using Clock = std::chrono::steady_clock;

namespace {
    constexpr int KEEPALIVE_IDLE_SEC = 30;
    constexpr int KEEPALIVE_INTERVAL_SEC = 5;
    constexpr int KEEPALIVE_COUNT = 3;
    constexpr unsigned int KEEPALIVE_USER_TIMEOUT_MS = 60000;

    std::once_flag sslInitFlag;

    std::string localHostName() {
        char buffer[256] = { 0 };
        if (gethostname(buffer, sizeof(buffer) - 1) == 0) {
            return buffer;
        }
        return "";
    }
}

//---------------------------------------------------------------------------//
// TCPHandler

void TCPHandler::onConnected(AMQP::TcpConnection *connection) {
    owner->connected(connection->fileno());
}

void TCPHandler::onProperties(AMQP::TcpConnection * /*connection*/, const AMQP::Table & /*server*/, AMQP::Table &client) {
    client.set("product", "PinkRabbitMQ 1C");
    client.set("connection_name", "1C PinkRabbitMQ " + localHostName() + " pid " + std::to_string(getpid()));
}

uint16_t TCPHandler::onNegotiate(AMQP::TcpConnection * /*connection*/, uint16_t interval) {
    return owner->negotiate(interval);
}

void TCPHandler::onReady(AMQP::TcpConnection * /*connection*/) {
    owner->ready();
}

void TCPHandler::onError(AMQP::TcpConnection * /*connection*/, const char *message) {
    owner->error(message && *message ? std::string(message) : std::string("AMQP connection error"));
}

void TCPHandler::onClosed(AMQP::TcpConnection * /*connection*/) {
    owner->closed();
}

void TCPHandler::onBlocked(AMQP::TcpConnection * /*connection*/, const char *reason) {
    owner->blocked(reason ? std::string(reason) : std::string());
}

void TCPHandler::onUnblocked(AMQP::TcpConnection * /*connection*/) {
    owner->unblocked();
}

void TCPHandler::onLost(AMQP::TcpConnection * /*connection*/) {
    owner->lost();
}

//---------------------------------------------------------------------------//
// ConnectionImpl

ConnectionImpl::ConnectionImpl(const AMQP::Address& address, uint16_t heartbeat, ConnectionEvents* events) :
    address(address),
    host(address.hostname()),
    desiredHeartbeat(heartbeat),
    events(events)
{
}

ConnectionImpl::~ConnectionImpl() {
    try {
        stop(0);
    }
    catch (...) {
    }
}

void ConnectionImpl::start(int timeoutMs) {
    if (thread.joinable()) {
        throw Biterp::Error("Connection is already started");
    }
    struct addrinfo* info = nullptr;
    if (getaddrinfo(host.c_str(), nullptr, nullptr, &info) != 0) {
        throw Biterp::Error("Wrong hostname: " + host);
    }
    freeaddrinfo(info);
    if (address.secure()) {
        std::call_once(sslInitFlag, []() { SSL_library_init(); });
    }
    try {
        eventLoop = event_base_new();
        if (!eventLoop) {
            throw Biterp::Error("Cannot create event loop");
        }
        wakeFd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wakeFd < 0) {
            throw Biterp::Error("Cannot create eventfd");
        }
        wakeEvent = event_new(eventLoop, wakeFd, EV_READ | EV_PERSIST, &ConnectionImpl::wakeCallback, this);
        timerEvent = event_new(eventLoop, -1, EV_PERSIST, &ConnectionImpl::timerCallback, this);
        if (!wakeEvent || !timerEvent) {
            throw Biterp::Error("Cannot create loop events");
        }
        event_add(wakeEvent, nullptr);
        struct timeval second = { 1, 0 };
        event_add(timerEvent, &second);
        handler.reset(new TCPHandler(eventLoop, this));
        {
            std::lock_guard<std::mutex> lock(cmdMutex);
            accepting = true;
        }
        setState(State::Connecting);
        connection.reset(new AMQP::TcpConnection(handler.get(), address));
        thread = std::thread(&ConnectionImpl::run, this);
    }
    catch (...) {
        releaseResources();
        throw;
    }

    std::unique_lock<std::mutex> lock(stateMutex);
    const bool completed = stateCv.wait_until(lock, Clock::now() + std::chrono::milliseconds(timeoutMs), [this] {
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
                if (connection && getState() == State::Ready) {
                    setState(State::Closing);
                    if (!connection->close()) {
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
    if (!connection) {
        throw Biterp::Error("Connection is closed");
    }
    return std::unique_ptr<AMQP::Channel>(new AMQP::TcpChannel(connection.get()));
}

bool ConnectionImpl::failed() const {
    return failPending.load() || getState() == State::Failed;
}

std::string ConnectionImpl::failReason() const {
    std::lock_guard<std::mutex> lock(stateMutex);
    return failText;
}

//---------------------------------------------------------------------------//
// handler callbacks, IO thread

uint16_t ConnectionImpl::negotiate(uint16_t interval) {
    heartbeat = desiredHeartbeat > 0 ? desiredHeartbeat : interval;
    return heartbeat;
}

void ConnectionImpl::connected(int fd) {
    if (fd < 0) {
        return;
    }
    int on = 1;
    int idle = KEEPALIVE_IDLE_SEC;
    int interval = KEEPALIVE_INTERVAL_SEC;
    int count = KEEPALIVE_COUNT;
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(count));
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
#ifdef TCP_USER_TIMEOUT
    // unacknowledged outgoing data for this long means a dead peer (keepalive works only on an idle socket)
    unsigned int userTimeoutMs = KEEPALIVE_USER_TIMEOUT_MS;
    setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &userTimeoutMs, sizeof(userTimeoutMs));
#endif
}

void ConnectionImpl::ready() {
    lastHeartbeat = Clock::now();
    setState(State::Ready);
}

void ConnectionImpl::error(const std::string& message) {
    requestFail(message);
    if (wakeEvent) {
        event_active(wakeEvent, EV_READ, 0);
    }
}

void ConnectionImpl::closed() {
    if (getState() == State::Closing) {
        setState(State::Closed);
        exitLoop = true;
    }
    else {
        requestFail("Connection closed by server");
    }
    if (wakeEvent) {
        event_active(wakeEvent, EV_READ, 0);
    }
}

void ConnectionImpl::lost() {
    requestFail(getState() == State::Closing ? "Connection closed" : "Connection lost");
    if (wakeEvent) {
        event_active(wakeEvent, EV_READ, 0);
    }
}

void ConnectionImpl::blocked(const std::string& reason) {
    events->onConnectionBlocked(reason);
}

void ConnectionImpl::unblocked() {
    events->onConnectionUnblocked();
}

//---------------------------------------------------------------------------//
// IO loop

void ConnectionImpl::wakeCallback(int /*fd*/, short /*what*/, void* arg) {
    static_cast<ConnectionImpl*>(arg)->onWake();
}

void ConnectionImpl::timerCallback(int /*fd*/, short /*what*/, void* arg) {
    static_cast<ConnectionImpl*>(arg)->onTimer();
}

void ConnectionImpl::run() {
    try {
        event_base_dispatch(eventLoop);
    }
    catch (const std::exception& e) {
        requestFail(std::string("IO loop error: ") + e.what());
    }
    catch (...) {
        requestFail("IO loop error");
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

void ConnectionImpl::onWake() {
    try {
        uint64_t value = 0;
        while (read(wakeFd, &value, sizeof(value)) > 0) {
        }
        runCommands();
    }
    catch (const std::exception& e) {
        requestFail(std::string("IO loop error: ") + e.what());
    }
    catch (...) {
        requestFail("IO loop error");
    }
    breakLoopIfDone();
}

void ConnectionImpl::onTimer() {
    try {
        if (heartbeat && connection && getState() == State::Ready) {
            const auto now = Clock::now();
            if (now - lastHeartbeat >= std::chrono::milliseconds(500 * static_cast<int>(heartbeat))) {
                lastHeartbeat = now;
                connection->heartbeat();
            }
        }
    }
    catch (...) {
    }
    breakLoopIfDone();
}

void ConnectionImpl::breakLoopIfDone() {
    if (failPending) {
        processFailure();
    }
    if (exitLoop || stopRequested) {
        event_base_loopbreak(eventLoop);
    }
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

void ConnectionImpl::requestFail(const std::string& reason) {
    std::lock_guard<std::mutex> lock(stateMutex);
    if (!failPending) {
        failText = reason;
        failPending = true;
    }
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
    if (connection) {
        try {
            // immediate close fails all pending operations and channels
            connection->close(true);
        }
        catch (...) {
        }
    }
}

void ConnectionImpl::finish() {
    std::deque<std::function<void()>> dropped;
    {
        std::lock_guard<std::mutex> lock(cmdMutex);
        accepting = false;
        dropped.swap(commands);
    }
    dropped.clear();
    try {
        events->onConnectionShutdown();
    }
    catch (...) {
    }
    try {
        connection.reset();
        handler.reset();
    }
    catch (...) {
    }
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        if (state != State::Failed) {
            state = State::Closed;
        }
    }
    stateCv.notify_all();
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

void ConnectionImpl::wake() {
    if (wakeFd < 0) {
        return;
    }
    const uint64_t one = 1;
    ssize_t written = write(wakeFd, &one, sizeof(one));
    (void) written;
}

void ConnectionImpl::releaseResources() {
    try {
        connection.reset();
        handler.reset();
    }
    catch (...) {
    }
    if (wakeEvent) {
        event_free(wakeEvent);
        wakeEvent = nullptr;
    }
    if (timerEvent) {
        event_free(timerEvent);
        timerEvent = nullptr;
    }
    if (eventLoop) {
        event_base_free(eventLoop);
        eventLoop = nullptr;
    }
    if (wakeFd >= 0) {
        close(wakeFd);
        wakeFd = -1;
    }
}
