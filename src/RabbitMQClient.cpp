#include "RabbitMQClient.h"
#include "Utils.h"
#include <mutex>
#include <algorithm>
#include <cstdlib>
#include <cmath>
#include <limits>
#include <thread>
#include <iterator>
#include <cstring>
#include <ostream>
#include <nlohmann/json.hpp>
#include "amqpcpp/decimalfield.h"
#include "amqpcpp/outbuffer.h"

using json = nlohmann::json;
using Biterp::Utf::toUtf16;

namespace {
	// How many tags of noConfirm consumers are remembered, so BasicAck for them is a no-op
	constexpr size_t AUTO_ACK_MEMORY = 16384;
	constexpr int DEFAULT_SELECT_SIZE = 200;
	constexpr int MAX_SELECT_SIZE = 2000;
	constexpr int MAX_TIMEOUT_SEC = 3600;
	// Upper limit of waits given by 1C (7 days), protects the chrono arithmetic
	constexpr int64_t MAX_WAIT_MS = 7LL * 24 * 60 * 60 * 1000;
	// Nesting limit of header tables and arrays
	constexpr int MAX_NESTING = 64;

	std::string errorText(const char* message, const char* fallback) {
		return message && *message ? std::string(message) : std::string(fallback);
	}

	std::string staleTagError(uint64_t tag) {
		return "Delivery tag " + std::to_string(tag) +
			" is stale: the consumer channel was closed, the message will be delivered again";
	}

	std::string unknownTagError(uint64_t tag) {
		return "Delivery tag " + std::to_string(tag) + " is unknown or already acknowledged";
	}

	AMQP::ExchangeType exchangeType(const std::string& type) {
		if (type == "topic") {
			return AMQP::ExchangeType::topic;
		}
		if (type == "fanout") {
			return AMQP::ExchangeType::fanout;
		}
		if (type == "direct") {
			return AMQP::ExchangeType::direct;
		}
		if (type == "headers") {
			return AMQP::ExchangeType::headers;
		}
		throw Biterp::Error("Exchange type not supported: " + type);
	}

	bool littleEndianHost() {
		const uint16_t probe = 1;
		return *reinterpret_cast<const uint8_t*>(&probe) == 1;
	}

	// AMQP-CPP writes and reads 'd' and 'f' fields in host byte order (OutBuffer::add(double),
	// InBuffer::nextDouble), the specification and RabbitMQ use network byte order.
	// This field writes a double in network byte order.
	class NetworkDouble : public AMQP::Field {
	public:
		explicit NetworkDouble(double value) : value(value) {}
		std::unique_ptr<AMQP::Field> clone() const override {
			return std::unique_ptr<AMQP::Field>(new NetworkDouble(value));
		}
		size_t size() const override {
			return sizeof(double);
		}
		void fill(AMQP::OutBuffer& buffer) const override {
			uint64_t bits = 0;
			std::memcpy(&bits, &value, sizeof(bits));
			char bytes[sizeof(bits)];
			for (size_t i = 0; i < sizeof(bits); i++) {
				bytes[i] = static_cast<char>((bits >> (8 * (sizeof(bits) - 1 - i))) & 0xFF);
			}
			buffer.add(bytes, sizeof(bytes));
		}
		char typeID() const override {
			return 'd';
		}
		void output(std::ostream& stream) const override {
			stream << "double(" << value << ")";
		}
		operator double() const override {
			return value;
		}
		operator float() const override {
			return static_cast<float>(value);
		}
	private:
		double value;
	};

	// Value of a received 'd' or 'f' field: AMQP-CPP read the network bytes as host order
	double receivedFloatingValue(const AMQP::Field& field) {
		if (field.typeID() == 'd') {
			double value = static_cast<double>(field);
			if (littleEndianHost()) {
				uint64_t bits = 0;
				std::memcpy(&bits, &value, sizeof(bits));
				uint64_t swapped = 0;
				for (size_t i = 0; i < sizeof(bits); i++) {
					swapped = (swapped << 8) | ((bits >> (8 * i)) & 0xFF);
				}
				std::memcpy(&value, &swapped, sizeof(value));
			}
			return value;
		}
		float value = static_cast<float>(field);
		if (littleEndianHost()) {
			uint32_t bits = 0;
			std::memcpy(&bits, &value, sizeof(bits));
			uint32_t swapped = 0;
			for (size_t i = 0; i < sizeof(bits); i++) {
				swapped = (swapped << 8) | ((bits >> (8 * i)) & 0xFF);
			}
			std::memcpy(&value, &swapped, sizeof(value));
		}
		return static_cast<double>(value);
	}

	// Decimal with a signed 32 bit value and the fewest fractional digits (0..6) that represent
	// the number exactly; otherwise a double in network byte order
	void setFloatField(AMQP::Table& table, const std::string& name, double value) {
		static const double scales[] = { 1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0 };
		const double limit = static_cast<double>(std::numeric_limits<int32_t>::max());
		if (std::isfinite(value)) {
			for (int places = 0; places <= 6; places++) {
				const double scaled = value * scales[places];
				const double rounded = std::round(scaled);
				// exact: the rest is below a millionth of the last digit (double error for |scaled| < 2^31 is ~2e-7)
				if (std::fabs(scaled - rounded) <= 1e-6) {
					if (std::fabs(rounded) <= limit) {
						const int32_t number = static_cast<int32_t>(rounded);
						table.set(name, AMQP::DecimalField(static_cast<uint8_t>(places), static_cast<uint32_t>(number)));
						return;
					}
					break;
				}
			}
		}
		table.set(name, NetworkDouble(value));
	}

	void fillTable(AMQP::Table& table, const json& object, int depth);

	void appendArray(AMQP::Array& array, const json& items, int depth);

	// RabbitMQ and the Java client read 'l' as a signed 64 bit integer and do not know 'L'.
	// AMQP-CPP calls 'l' ULongLong, so the signed value is written with its two's complement bits.
	std::unique_ptr<AMQP::Field> signedLongField(int64_t value) {
		return std::unique_ptr<AMQP::Field>(new AMQP::ULongLong(static_cast<uint64_t>(value)));
	}

	std::unique_ptr<AMQP::Field> fieldFromJson(const std::string& name, const json& value, int depth) {
		if (depth > MAX_NESTING) {
			throw Biterp::Error("Arguments are nested too deep: " + name);
		}
		if (value.is_boolean()) {
			return std::unique_ptr<AMQP::Field>(new AMQP::BooleanSet(value.get<bool>()));
		}
		if (value.is_number_unsigned()) {
			const uint64_t number = value.get<uint64_t>();
			if (number > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
				// does not fit a signed 64 bit field
				return std::unique_ptr<AMQP::Field>(new NetworkDouble(static_cast<double>(number)));
			}
			return signedLongField(static_cast<int64_t>(number));
		}
		if (value.is_number_integer()) {
			return signedLongField(value.get<int64_t>());
		}
		if (value.is_string()) {
			return std::unique_ptr<AMQP::Field>(new AMQP::LongString(value.get<std::string>()));
		}
		if (value.is_null()) {
			return std::unique_ptr<AMQP::Field>(new AMQP::VoidField());
		}
		if (value.is_object()) {
			std::unique_ptr<AMQP::Table> nested(new AMQP::Table());
			fillTable(*nested, value, depth + 1);
			return std::unique_ptr<AMQP::Field>(nested.release());
		}
		if (value.is_array()) {
			std::unique_ptr<AMQP::Array> nested(new AMQP::Array());
			appendArray(*nested, value, depth + 1);
			return std::unique_ptr<AMQP::Field>(nested.release());
		}
		throw Biterp::Error("Unsupported json type for property " + name);
	}

	void fillTable(AMQP::Table& table, const json& object, int depth) {
		for (auto& it : object.items()) {
			const std::string& name = it.key();
			const json& value = it.value();
			if (value.is_number_float()) {
				setFloatField(table, name, value.get<double>());
				continue;
			}
			table.set(name, *fieldFromJson(name, value, depth));
		}
	}

	void appendArray(AMQP::Array& array, const json& items, int depth) {
		for (const json& value : items) {
			if (value.is_number_float()) {
				AMQP::Table holder;
				setFloatField(holder, "v", value.get<double>());
				array.push_back(holder.get("v"));
				continue;
			}
			array.push_back(*fieldFromJson("array item", value, depth));
		}
	}

	json fieldToJson(const AMQP::Field& field, int depth) {
		if (depth > MAX_NESTING) {
			return json();
		}
		switch (field.typeID()) {
		case 't': {
			const AMQP::BooleanSet& value = dynamic_cast<const AMQP::BooleanSet&>(field);
			return json(static_cast<bool>(value.get(0)));
		}
		case 'b':
		case 'B':
		case 'U':
		case 'u':
		case 'I':
		case 'i':
		case 'L':
		case 'T':
			return json(static_cast<int64_t>(field));
		case 'l':
			// signed 64 bit for RabbitMQ and other clients, AMQP-CPP decoded it as unsigned
			return json(static_cast<int64_t>(static_cast<uint64_t>(field)));
		case 'f':
		case 'd':
			return json(receivedFloatingValue(field));
		case 'D': {
			const AMQP::DecimalField& value = dynamic_cast<const AMQP::DecimalField&>(field);
			const int32_t number = static_cast<int32_t>(value.number());
			return json(static_cast<double>(number) / std::pow(10.0, value.places()));
		}
		case 's':
		case 'S':
			return json(static_cast<const std::string&>(field));
		case 'F': {
			const AMQP::Table& table = dynamic_cast<const AMQP::Table&>(field);
			json object = json::object();
			for (const std::string& key : table.keys()) {
				object[key] = fieldToJson(table.get(key), depth + 1);
			}
			return object;
		}
		case 'A': {
			const AMQP::Array& array = dynamic_cast<const AMQP::Array&>(field);
			json items = json::array();
			const uint32_t count = array.count();
			for (uint32_t i = 0; i < count && i <= std::numeric_limits<uint8_t>::max(); i++) {
				items.push_back(fieldToJson(array.get(static_cast<uint8_t>(i)), depth + 1));
			}
			return items;
		}
		default:
			return json();
		}
	}
}

RabbitMQClient::~RabbitMQClient() {
	// Stop the IO thread first: its callbacks use the consumer state below
	try {
		connection.reset();
	}
	catch (...) {
	}
}

void RabbitMQClient::connectImpl(Biterp::CallContext& ctx) {
	std::string host = ctx.stringParamUtf8();
	int64_t port = ctx.longParam();
	std::string user = ctx.stringParamUtf8();
	std::string pwd = ctx.stringParamUtf8();
	std::string vhost = ctx.stringParamUtf8();
	int64_t pingRate = ctx.optLongParam(0);
	bool ssl = ctx.optBoolParam(false);
	int64_t timeout = ctx.optLongParam(5);

	if (host.empty()) {
		throw Biterp::Error("Empty hostname not allowed");
	}
	if (port <= 0 || port > 65535) {
		throw Biterp::Error("Wrong port: ") << port;
	}
	pingRate = std::max<int64_t>(0, std::min<int64_t>(pingRate, 65535));
	timeout = std::max<int64_t>(1, std::min<int64_t>(timeout, MAX_TIMEOUT_SEC));

	// The old connection is stopped completely (IO thread joined) before the state is reset
	connection.reset();
	resetConsumerState();

	AMQP::Address address(host, static_cast<uint16_t>(port), AMQP::Login(user, pwd), vhost, ssl);
	Connection::Options options;
	options.timeoutSec = static_cast<int>(timeout);
	options.heartbeat = static_cast<uint16_t>(pingRate);
	std::unique_ptr<Connection> fresh(new Connection(address, options, this));
	fresh->connect();
	connection = std::move(fresh);
}


void RabbitMQClient::declareExchangeImpl(Biterp::CallContext& ctx) {
	checkConnection();

	std::string name = ctx.stringParamUtf8();
	std::string type = ctx.stringParamUtf8();
	bool onlyCheckIfExists = ctx.boolParam();
	bool durable = ctx.boolParam();
	bool autodelete = ctx.optBoolParam(false);
	std::string propsJson = ctx.optStringParamUtf8();

	const AMQP::ExchangeType kind = exchangeType(type);
	const int flags = (onlyCheckIfExists ? AMQP::passive : 0) | (durable ? AMQP::durable : 0) | (autodelete ? AMQP::autodelete : 0);
	AMQP::Table args = headersFromJson(propsJson);
	connection->call([name, kind, flags, args](Connection::Io& io, const OperationPtr& op) {
		io.managementChannel().declareExchange(name, kind, flags, args)
			.onSuccess([op]() { op->succeed(); })
			.onError([op](const char* message) { op->fail(errorText(message, "Declare exchange failed")); });
	});
}


void RabbitMQClient::deleteExchangeImpl(Biterp::CallContext& ctx) {
	checkConnection();

	std::string name = ctx.stringParamUtf8();
	bool ifunused = ctx.boolParam();
	const int flags = ifunused ? AMQP::ifunused : 0;
	connection->call([name, flags](Connection::Io& io, const OperationPtr& op) {
		io.managementChannel().removeExchange(name, flags)
			.onSuccess([op]() { op->succeed(); })
			.onError([op](const char* message) { op->fail(errorText(message, "Delete exchange failed")); });
	});
}

void RabbitMQClient::declareQueueImpl(Biterp::CallContext& ctx) {
	checkConnection();

	std::string name = ctx.stringParamUtf8();
	bool onlyCheckIfExists = ctx.boolParam();
	bool durable = ctx.boolParam();
	bool exclusive = ctx.boolParam();
	bool autodelete = ctx.boolParam();
	int64_t maxPriority = ctx.optLongParam(0);
	std::string propsJson = ctx.optStringParamUtf8();

	AMQP::Table args = headersFromJson(propsJson);
	if (maxPriority > 0) {
		args.set("x-max-priority", static_cast<uint16_t>(std::min<int64_t>(maxPriority, 255)));
	}
	const int flags = (onlyCheckIfExists ? AMQP::passive : 0) | (durable ? AMQP::durable : 0) |
		(exclusive ? AMQP::exclusive : 0) | (autodelete ? AMQP::autodelete : 0);
	OperationPtr result = connection->call([name, flags, args](Connection::Io& io, const OperationPtr& op) {
		io.managementChannel().declareQueue(name, flags, args)
			.onSuccess([op](const std::string& queueName, uint32_t /*messageCount*/, uint32_t /*consumerCount*/) {
				op->strValue = queueName;
				op->succeed();
			})
			.onError([op](const char* message) { op->fail(errorText(message, "Declare queue failed")); });
	});
	ctx.setStringResult(toUtf16(result->strValue.empty() ? name : result->strValue));
}


void RabbitMQClient::deleteQueueImpl(Biterp::CallContext& ctx) {
	checkConnection();

	std::string name = ctx.stringParamUtf8();
	bool ifunused = ctx.boolParam();
	bool ifempty = ctx.boolParam();
	const int flags = (ifunused ? AMQP::ifunused : 0) | (ifempty ? AMQP::ifempty : 0);
	connection->call([name, flags](Connection::Io& io, const OperationPtr& op) {
		io.managementChannel().removeQueue(name, flags)
			.onSuccess([op]() { op->succeed(); })
			.onError([op](const char* message) { op->fail(errorText(message, "Delete queue failed")); });
	});
}

void RabbitMQClient::bindQueueImpl(Biterp::CallContext& ctx) {
	checkConnection();

	std::string queue = ctx.stringParamUtf8();
	std::string exchange = ctx.stringParamUtf8();
	std::string routingKey = ctx.stringParamUtf8();
	std::string propsJson = ctx.optStringParamUtf8();

	AMQP::Table args = headersFromJson(propsJson);
	connection->call([queue, exchange, routingKey, args](Connection::Io& io, const OperationPtr& op) {
		io.managementChannel().bindQueue(exchange, queue, routingKey, args)
			.onSuccess([op]() { op->succeed(); })
			.onError([op](const char* message) { op->fail(errorText(message, "Bind queue failed")); });
	});
}

void RabbitMQClient::unbindQueueImpl(Biterp::CallContext& ctx) {
	checkConnection();

	std::string queue = ctx.stringParamUtf8();
	std::string exchange = ctx.stringParamUtf8();
	std::string routingKey = ctx.stringParamUtf8();
	connection->call([queue, exchange, routingKey](Connection::Io& io, const OperationPtr& op) {
		io.managementChannel().unbindQueue(exchange, queue, routingKey)
			.onSuccess([op]() { op->succeed(); })
			.onError([op](const char* message) { op->fail(errorText(message, "Unbind queue failed")); });
	});
}



void RabbitMQClient::basicPublishImpl(Biterp::CallContext& ctx) {
	checkConnection();

	struct Outgoing {
		std::string exchange;
		std::string routingKey;
		std::string body;
		std::map<int, std::string> props;
		uint8_t priority = 0;
		bool persistent = false;
		AMQP::Table headers;
	};
	std::shared_ptr<Outgoing> out = std::make_shared<Outgoing>();
	out->exchange = ctx.stringParamUtf8();
	out->routingKey = ctx.stringParamUtf8();
	out->body = ctx.stringParamUtf8();
	ctx.skipOptional(); // livingTime is not implemented, use the Expiration property
	out->persistent = ctx.optBoolParam(false);
	std::string propsJson = ctx.optStringParamUtf8();
	const bool waitConfirm = ctx.optBoolParam(true);

	out->headers = headersFromJson(propsJson);
	for (const auto& prop : msgProps) {
		if (!prop.second.empty()) {
			out->props.insert(prop);
		}
	}
	out->priority = static_cast<uint8_t>(std::max(0, std::min(priority, 255)));

	connection->call([out, waitConfirm](Connection::Io& io, const OperationPtr& op) {
		AMQP::Envelope envelope(out->body.data(), out->body.size());
		for (const auto& prop : out->props) {
			switch (prop.first) {
			case CORRELATION_ID: envelope.setCorrelationID(prop.second); break;
			case MESSAGE_ID: envelope.setMessageID(prop.second); break;
			case TYPE_NAME: envelope.setTypeName(prop.second); break;
			case APP_ID: envelope.setAppID(prop.second); break;
			case CONTENT_ENCODING: envelope.setContentEncoding(prop.second); break;
			case CONTENT_TYPE: envelope.setContentType(prop.second); break;
			case USER_ID: envelope.setUserID(prop.second); break;
			case CLUSTER_ID: envelope.setClusterID(prop.second); break;
			case EXPIRATION: envelope.setExpiration(prop.second); break;
			case REPLY_TO: envelope.setReplyTo(prop.second); break;
			default: break;
			}
		}
		if (out->priority != 0) {
			envelope.setPriority(out->priority);
		}
		if (out->persistent) {
			envelope.setDeliveryMode(2);
		}
		envelope.setHeaders(out->headers);
		io.publish(out->exchange, out->routingKey, envelope, waitConfirm, op);
	});
}

void RabbitMQClient::waitForConfirmsImpl(Biterp::CallContext& ctx) {
	checkConnection();
	int64_t timeoutMs = ctx.optLongParam(-1);
	if (timeoutMs < 0) {
		timeoutMs = connection->timeoutMs();
	}
	timeoutMs = std::min<int64_t>(timeoutMs, std::numeric_limits<int>::max());
	OperationPtr op;
	const bool completed = connection->tryCall([](Connection::Io& io, const OperationPtr& op) {
		io.waitConfirms(op);
	}, static_cast<int>(timeoutMs), op);
	ctx.setBoolResult(completed);
}


void RabbitMQClient::basicConsumeImpl(Biterp::CallContext& ctx) {
	checkConnection();
	std::string queue = ctx.stringParamUtf8();
	std::string consumerId = ctx.stringParamUtf8(true);
	bool noconfirm = ctx.boolParam();
	bool exclusive = ctx.boolParam();
	int64_t selectSize = ctx.optLongParam(DEFAULT_SELECT_SIZE);
	// Unlimited prefetch would flood the memory of the process
	if (selectSize <= 0) {
		selectSize = DEFAULT_SELECT_SIZE;
	}
	else if (selectSize > MAX_SELECT_SIZE) {
		selectSize = MAX_SELECT_SIZE;
	}
	std::string propsJson = ctx.optStringParamUtf8();

	AMQP::Table args = headersFromJson(propsJson, true);
	// noConfirm is emulated: the subscription always uses manual acks, so the prefetch limit works,
	// and the message is acknowledged when it is given to 1C
	const bool autoAck = noconfirm;
	const int flags = exclusive ? AMQP::exclusive : 0;
	const uint16_t prefetch = static_cast<uint16_t>(selectSize);
	OperationPtr result = connection->call([this, queue, consumerId, flags, args, prefetch, autoAck](Connection::Io& io, const OperationPtr& op) {
		// Io lives as long as the connection, the callbacks below run on the IO thread before it ends
		Connection::Io* ioPtr = &io;
		auto startConsume = [this, ioPtr, queue, consumerId, flags, args, prefetch, autoAck, op]() {
			AMQP::Channel& channel = ioPtr->consumeChannel();
			const uint32_t channelId = ioPtr->consumeChannelId();
			std::shared_ptr<std::string> consumerTag = std::make_shared<std::string>();
			channel.setQos(prefetch);
			channel.consume(queue, consumerId, flags, args)
				.onSuccess([this, ioPtr, op, channelId, consumerTag](const std::string& tag) {
					*consumerTag = tag;
					if (!op->claim()) {
						// 1C already got a timeout and does not know this consumer: stop it
						AMQP::Channel* current = ioPtr->consumeChannelIfAlive();
						if (current && ioPtr->consumeChannelId() == channelId) {
							current->cancel(tag);
						}
						return;
					}
					onConsumerStarted(channelId, tag);
					op->strValue = tag;
					op->succeed();
				})
				.onMessage([this, ioPtr, channelId, autoAck, consumerTag](const AMQP::Message& message, uint64_t deliveryTag, bool /*redelivered*/) {
					if (!onDelivery(channelId, *consumerTag, message, deliveryTag, autoAck)) {
						// the consumer is not known to 1C (stopped after a timeout): give the message back
						ioPtr->reject(channelId, deliveryTag, AMQP::requeue);
					}
				})
				.onCancelled([this, channelId](const std::string& tag) {
					onConsumerCancelled(channelId, tag);
				})
				.onError([op](const char* message) {
					op->fail(errorText(message, "Consume failed"));
				});
		};
		if (queue.empty()) {
			startConsume();
			return;
		}
		// A missing queue closes the channel of the failed command. The passive check runs on the
		// management channel, so a wrong queue name does not close the channel of the other consumers.
		io.managementChannel().declareQueue(queue, AMQP::passive)
			.onSuccess([startConsume, op](const std::string& /*name*/, uint32_t /*messages*/, uint32_t /*consumers*/) {
				if (op->abandoned()) {
					return;
				}
				try {
					startConsume();
				}
				catch (const std::exception& e) {
					op->fail(e.what());
				}
				catch (...) {
					op->fail("Consume failed");
				}
			})
			.onError([op](const char* message) {
				op->fail(errorText(message, "Queue check failed"));
			});
	});
	ctx.setStringResult(toUtf16(result->strValue));
}


void RabbitMQClient::basicConsumeMessageImpl(Biterp::CallContext& ctx) {
	ctx.skipParam(); // consumerId is not used, all consumers share one local queue
	tVariant* outdata = ctx.skipParam();
	tVariant* outMessageTag = ctx.skipParam();
	int64_t timeout = ctx.longParam();
	if (timeout < 0) {
		timeout = 0;
	}
	timeout = std::min<int64_t>(timeout, MAX_WAIT_MS);
	ctx.setEmptyResult(outdata);
	ctx.setIntResult(0, outMessageTag);

	MessageObject message;
	bool postAck = false;
	{
		std::unique_lock<std::mutex> lock(_mutex);
		auto ready = [this] { return !messageQueue.empty() || !consumerError.empty() || consumers.empty(); };
		if (!ready()) {
			if (!cvDataArrived.wait_for(lock, std::chrono::milliseconds(timeout), ready)) {
				ctx.setBoolResult(false);
				return;
			}
		}
		if (messageQueue.empty()) {
			if (!consumerError.empty()) {
				throw Biterp::Error(consumerError);
			}
			throw Biterp::Error("No active consumers");
		}
		message = std::move(messageQueue.front());
		messageQueue.pop_front();
		if (message.autoAck) {
			postAck = true;
			autoAcked.insert(message.messageTag);
			autoAckedOrder.push_back(message.messageTag);
			while (autoAckedOrder.size() > AUTO_ACK_MEMORY) {
				autoAcked.erase(autoAckedOrder.front());
				autoAckedOrder.pop_front();
			}
		}
		else {
			IssuedTag issued;
			issued.channelId = message.channelId;
			issued.deliveryTag = message.deliveryTag;
			issuedTags[message.messageTag] = issued;
		}
	}
	if (postAck && connection) {
		const uint32_t channelId = message.channelId;
		const uint64_t deliveryTag = message.deliveryTag;
		connection->post([channelId, deliveryTag](Connection::Io& io) {
			io.ack(channelId, deliveryTag, 0);
		});
	}
	std::string body = std::move(message.body);
	lastMessage = std::move(message);
	ctx.setStringResult(toUtf16(body), outdata);
	setTagResult(ctx, lastMessage.messageTag, outMessageTag);
	ctx.setBoolResult(true);
}

void RabbitMQClient::setTagResult(Biterp::CallContext& ctx, uint64_t tag, tVariant* param) {
	if (tag <= static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
		ctx.setIntResult(static_cast<int>(tag), param);
	}
	else {
		ctx.setDoubleResult(static_cast<double>(tag), param);
	}
}

void RabbitMQClient::resetConsumerState() {
	std::lock_guard<std::mutex> lock(_mutex);
	consumeChannelId = 0;
	tagBase = lastAssignedTag;
	staleUpTo = lastAssignedTag;
	consumers.clear();
	messageQueue.clear();
	consumerError.clear();
	issuedTags.clear();
	// autoAcked is kept: BasicAck for a noConfirm message stays a no-op after a reconnect
	cvDataArrived.notify_all();
}

void RabbitMQClient::basicCancelImpl(Biterp::CallContext& ctx) {
	checkConnection();
	std::string consumerId = ctx.stringParamUtf8(true);
	std::vector<std::string> toCancel;
	{
		std::lock_guard<std::mutex> lock(_mutex);
		if (consumerId.empty()) {
			toCancel = consumers;
		}
		else {
			auto it = std::find(consumers.begin(), consumers.end(), consumerId);
			if (it != consumers.end()) {
				toCancel.push_back(consumerId);
			}
		}
	}
	if (toCancel.empty()) {
		std::string reason;
		{
			std::lock_guard<std::mutex> lock(_mutex);
			reason = consumerError;
		}
		throw Biterp::Error(reason.empty() ? std::string("Consumer not found") : "Consumer not found: " + reason);
	}

	bool timedOut = false;
	for (const auto& tag : toCancel) {
		OperationPtr op;
		const bool completed = connection->tryCall([tag](Connection::Io& io, const OperationPtr& op) {
			AMQP::Channel* channel = io.consumeChannelIfAlive();
			if (!channel) {
				op->succeed();
				return;
			}
			channel->cancel(tag)
				.onSuccess([op](const std::string& /*consumer*/) { op->succeed(); })
				.onError([op, tag](const char* message) {
					op->fail("Cancel failed for " + tag + ": " + errorText(message, "Unknown cancel error"));
				});
		}, -1, op);
		if (!completed) {
			LOGE("Cancel timeout for consumer " + tag + ". Force close read channel.");
			timedOut = true;
			break;
		}
		// messages of the cancelled consumer that 1C has not taken yet go back to the queue
		std::vector<std::pair<uint32_t, uint64_t>> toReturn;
		{
			std::lock_guard<std::mutex> lock(_mutex);
			consumers.erase(std::remove(consumers.begin(), consumers.end(), tag), consumers.end());
			for (auto it = messageQueue.begin(); it != messageQueue.end();) {
				if (it->consumerTag == tag) {
					// noConfirm messages are acknowledged only when taken by 1C, so all of them are returned
					toReturn.emplace_back(it->channelId, it->deliveryTag);
					it = messageQueue.erase(it);
				}
				else {
					++it;
				}
			}
		}
		if (!toReturn.empty()) {
			connection->post([toReturn](Connection::Io& io) {
				for (const auto& item : toReturn) {
					io.reject(item.first, item.second, AMQP::requeue);
				}
			});
		}
	}
	if (timedOut) {
		try {
			connection->call([](Connection::Io& io, const OperationPtr& op) {
				io.closeConsumeChannel("Consumer cancel timeout");
				op->succeed();
			});
		}
		catch (const std::exception& e) {
			LOGE(std::string("Close read channel failed: ") + e.what());
		}
	}
	{
		std::lock_guard<std::mutex> lock(_mutex);
		if (timedOut) {
			consumers.clear();
		}
		if (consumers.empty()) {
			consumerError.clear();
			cvDataArrived.notify_all();
		}
	}
}

void RabbitMQClient::basicAckImpl(Biterp::CallContext& ctx) {
	uint64_t tag = static_cast<uint64_t>(ctx.longParam());
	bool multiple = ctx.optBoolParam(false);
	settle(tag, multiple, true, false);
}

void RabbitMQClient::basicRejectImpl(Biterp::CallContext& ctx) {
	uint64_t tag = static_cast<uint64_t>(ctx.longParam());
	bool requeue = ctx.optBoolParam(false);
	settle(tag, false, false, requeue);
}

void RabbitMQClient::settle(uint64_t tag, bool multiple, bool ack, bool requeue) {
	checkConnection();
	if (tag == 0) {
		throw Biterp::Error("Message tag cannot be empty!");
	}
	uint32_t channelId = 0;
	uint64_t deliveryTag = 0;
	{
		std::lock_guard<std::mutex> lock(_mutex);
		if (tag > lastAssignedTag) {
			throw Biterp::Error(unknownTagError(tag));
		}
		auto it = issuedTags.find(tag);
		if (it == issuedTags.end() && multiple) {
			// the tag itself was not issued for manual ack, take the latest issued one below it
			auto upper = issuedTags.upper_bound(tag);
			if (upper != issuedTags.begin()) {
				it = std::prev(upper);
			}
		}
		if (it == issuedTags.end()) {
			if (autoAcked.count(tag)) {
				if (!ack) {
					throw Biterp::Error("Message " + std::to_string(tag) +
						" was acknowledged when it was received (noConfirm consumer), it cannot be rejected");
				}
				// noConfirm consumer: the message is already acknowledged
				return;
			}
			if (tag <= staleUpTo) {
				throw Biterp::Error(staleTagError(tag));
			}
			throw Biterp::Error(unknownTagError(tag));
		}
		channelId = it->second.channelId;
		deliveryTag = it->second.deliveryTag;
		if (multiple) {
			issuedTags.erase(issuedTags.begin(), std::next(it));
		}
		else {
			issuedTags.erase(it);
		}
		if (channelId == 0 || channelId != consumeChannelId) {
			throw Biterp::Error(staleTagError(tag));
		}
	}
	int flags = 0;
	if (multiple) {
		flags |= AMQP::multiple;
	}
	if (!ack && requeue) {
		flags |= AMQP::requeue;
	}
	const bool posted = connection->post([channelId, deliveryTag, flags, ack](Connection::Io& io) {
		if (ack) {
			io.ack(channelId, deliveryTag, flags);
		}
		else {
			io.reject(channelId, deliveryTag, flags);
		}
	});
	if (!posted) {
		throw Biterp::Error("Connection lost: " + connection->lostReason() + ". The message will be delivered again");
	}
}

void RabbitMQClient::checkConnection() {
	if (!connection) {
		throw Biterp::Error("Connection is not established! Use the method Connect() first");
	}
}

void RabbitMQClient::sleepNativeImpl(Biterp::CallContext& ctx) {
	int64_t amount = ctx.longParam();
	if (amount > 0) {
		std::this_thread::sleep_for(std::chrono::milliseconds(amount));
	}
}

void RabbitMQClient::setLogLevelImpl(Biterp::CallContext& ctx) {
	int level = -1;
	tVariant* param = ctx.currentParam();
	if (param->vt == VTYPE_PWSTR) {
		std::string name = ctx.stringParamUtf8();
		level = Biterp::Logging::levelFromName(name);
		if (level < 0) {
			throw Biterp::Error("Unknown log level: " + name + ". Use D, I, W, E or F");
		}
	}
	else {
		level = ctx.intParam();
	}
	Biterp::Logging::setLevel(level);
}

void RabbitMQClient::isConnectedImpl(Biterp::CallContext& ctx) {
	ctx.setBoolResult(connection && connection->alive());
}

void RabbitMQClient::getRoutingKeyImpl(Biterp::CallContext& ctx) {
	ctx.setStringResult(toUtf16(lastMessage.routingKey));
}

void RabbitMQClient::getHeadersImpl(Biterp::CallContext& ctx) {
	ctx.setStringResult(toUtf16(lastMessageHeaders()));
}

void RabbitMQClient::setPriorityImpl(Biterp::CallContext& ctx) {
	priority = ctx.intParam();
}

void RabbitMQClient::getPriorityImpl(Biterp::CallContext& ctx) {
	ctx.setIntResult(lastMessage.priority);
}

void RabbitMQClient::getMsgPropImpl(const long propNum, Biterp::CallContext& ctx) {
	auto it = lastMessage.msgProps.find(static_cast<int>(propNum));
	ctx.setStringResult(it == lastMessage.msgProps.end() ? std::u16string() : toUtf16(it->second));
}

void RabbitMQClient::setMsgPropImpl(const long propNum, Biterp::CallContext& ctx) {
	msgProps[static_cast<int>(propNum)] = ctx.stringParamUtf8();
}

AMQP::Table RabbitMQClient::headersFromJson(const std::string& propsJson, bool forConsume)
{
	AMQP::Table headers;
	if (propsJson.empty()) {
		return headers;
	}
	json object;
	try {
		object = json::parse(propsJson);
	}
	catch (const json::exception& e) {
		throw Biterp::Error(std::string("Wrong JSON in arguments: ") + e.what());
	}
	if (object.is_null() || (object.is_array() && object.empty())) {
		return headers;
	}
	if (!object.is_object()) {
		throw Biterp::Error("Arguments must be a JSON object");
	}

	for (auto& it : object.items()) {
		const json& value = it.value();
		const std::string& name = it.key();
		if (forConsume && name == "x-stream-offset")
		{
			if (value.is_string())
			{
				const std::string offset = value.get<std::string>();
				if (offset == "first" || offset == "last" || offset == "next") {
					headers.set(name, offset);
				}
				else {
					headers.set(name, AMQP::Timestamp(Utils::parseDateTime(offset)));
				}
			}
			else if (value.is_number_integer() || value.is_number_unsigned())
			{
				headers.set(name, *signedLongField(value.get<int64_t>()));
			}
			else
			{
				throw Biterp::Error("Unsupported json type for property " + name);
			}
		}
		else if (value.is_number_float())
		{
			setFloatField(headers, name, value.get<double>());
		}
		else
		{
			headers.set(name, *fieldFromJson(name, value, 1));
		}
	}
	return headers;
}

std::string RabbitMQClient::lastMessageHeaders() {
	AMQP::Table& headersTbl = lastMessage.headers;
	json hdr = json::object();
	for (const std::string& key : headersTbl.keys()) {
		hdr[key] = fieldToJson(headersTbl.get(key), 1);
	}
	return hdr.dump(-1, ' ', false, json::error_handler_t::replace);
}

//---------------------------------------------------------------------------//
// IO thread callbacks

void RabbitMQClient::onConsumeChannelOpened(uint32_t channelId) {
	std::lock_guard<std::mutex> lock(_mutex);
	consumeChannelId = channelId;
	tagBase = lastAssignedTag;
}

void RabbitMQClient::onConsumeChannelClosed(uint32_t channelId, const std::string& reason) {
	{
		std::lock_guard<std::mutex> lock(_mutex);
		if (channelId != consumeChannelId) {
			return;
		}
		consumeChannelId = 0;
		staleUpTo = lastAssignedTag;
		// prefetched messages of the channel are returned to the queue by the broker
		messageQueue.erase(std::remove_if(messageQueue.begin(), messageQueue.end(),
			[channelId](const MessageObject& m) { return m.channelId == channelId; }), messageQueue.end());
		issuedTags.clear();
		if (!consumers.empty() && consumerError.empty()) {
			consumerError = "Consumer channel closed: " + reason;
		}
		consumers.clear();
	}
	LOGW("Consumer channel closed: " + reason);
	cvDataArrived.notify_all();
}

void RabbitMQClient::onConnectionLost(const std::string& reason) {
	{
		// one step: 1C must not see the error and still get messages of the dead channel
		std::lock_guard<std::mutex> lock(_mutex);
		consumerError = "Connection lost: " + reason;
		consumeChannelId = 0;
		staleUpTo = lastAssignedTag;
		messageQueue.clear();
		issuedTags.clear();
		consumers.clear();
	}
	cvDataArrived.notify_all();
}

void RabbitMQClient::onConsumerStarted(uint32_t channelId, const std::string& tag) {
	LOGI("Consumer created " + tag);
	std::lock_guard<std::mutex> lock(_mutex);
	if (channelId != consumeChannelId) {
		return;
	}
	consumers.push_back(tag);
	consumerError.clear();
}

void RabbitMQClient::onConsumerCancelled(uint32_t channelId, const std::string& tag) {
	LOGI("Consumer cancelled " + tag);
	{
		std::lock_guard<std::mutex> lock(_mutex);
		if (channelId != consumeChannelId) {
			return;
		}
		consumers.erase(std::remove(consumers.begin(), consumers.end(), tag), consumers.end());
	}
	cvDataArrived.notify_all();
}

bool RabbitMQClient::onDelivery(uint32_t channelId, const std::string& consumerTag, const AMQP::Message& message,
	uint64_t deliveryTag, bool autoAck) {
	MessageObject msgOb;
	msgOb.consumerTag = consumerTag;
	msgOb.body.assign(message.body(), static_cast<size_t>(message.bodySize()));
	msgOb.msgProps[CORRELATION_ID] = message.correlationID();
	msgOb.msgProps[TYPE_NAME] = message.typeName();
	msgOb.msgProps[MESSAGE_ID] = message.messageID();
	msgOb.msgProps[APP_ID] = message.appID();
	msgOb.msgProps[CONTENT_ENCODING] = message.contentEncoding();
	msgOb.msgProps[CONTENT_TYPE] = message.contentType();
	msgOb.msgProps[USER_ID] = message.userID();
	msgOb.msgProps[CLUSTER_ID] = message.clusterID();
	msgOb.msgProps[EXPIRATION] = message.expiration();
	msgOb.msgProps[REPLY_TO] = message.replyTo();
	msgOb.deliveryTag = deliveryTag;
	msgOb.channelId = channelId;
	msgOb.autoAck = autoAck;
	msgOb.priority = message.priority();
	msgOb.routingKey = message.routingkey();
	msgOb.headers = message.headers();
	{
		std::lock_guard<std::mutex> lock(_mutex);
		if (channelId != consumeChannelId) {
			// late delivery of a closed channel, the broker delivers it again
			return true;
		}
		if (std::find(consumers.begin(), consumers.end(), consumerTag) == consumers.end()) {
			return false;
		}
		msgOb.messageTag = tagBase + deliveryTag;
		lastAssignedTag = std::max(lastAssignedTag, msgOb.messageTag);
		messageQueue.push_back(std::move(msgOb));
	}
	cvDataArrived.notify_all();
	return true;
}
