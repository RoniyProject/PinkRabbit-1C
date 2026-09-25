#pragma once

#include "Connection.h"
#include <addin/biterp/Component.hpp>
#include <map>
#include <vector>
#include <deque>
#include <unordered_set>
#include <mutex>
#include <condition_variable>
#include <memory>


class RabbitMQClient : public Biterp::Component, private ConnectionListener {
public:
	// Transiting properties
	static constexpr int CORRELATION_ID = 1;
	static constexpr int TYPE_NAME = 2;
	static constexpr int MESSAGE_ID = 3;
	static constexpr int APP_ID = 4;
	static constexpr int CONTENT_ENCODING = 5;
	static constexpr int CONTENT_TYPE = 6;
	static constexpr int USER_ID = 7;
	static constexpr int CLUSTER_ID = 8;
	static constexpr int EXPIRATION = 9;
	static constexpr int REPLY_TO = 10;
public:
	RabbitMQClient() : Biterp::Component("RabbitMQClient"), priority(0) {};

	virtual ~RabbitMQClient();

	inline bool connect(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::connectImpl, paParams, lSizeArray);
	}
	inline bool basicPublish(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::basicPublishImpl, paParams, lSizeArray);
	}
	inline bool basicCancel(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::basicCancelImpl, paParams, lSizeArray);
	}
	inline bool basicAck(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::basicAckImpl, paParams, lSizeArray);
	}
	inline bool basicReject(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::basicRejectImpl, paParams, lSizeArray);
	}
	inline bool deleteQueue(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::deleteQueueImpl, paParams, lSizeArray);
	}
	inline bool bindQueue(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::bindQueueImpl, paParams, lSizeArray);
	}
	inline bool unbindQueue(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::unbindQueueImpl, paParams, lSizeArray);
	}
	inline bool declareExchange(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::declareExchangeImpl, paParams, lSizeArray);
	}
	inline bool deleteExchange(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::deleteExchangeImpl, paParams, lSizeArray);
	}
	inline bool setPriority(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::setPriorityImpl, paParams, lSizeArray);
	}
	inline bool setLogLevel(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::setLogLevelImpl, paParams, lSizeArray);
	}

	inline bool basicConsume(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::basicConsumeImpl, paParams, lSizeArray, pvarRetValue);
	}
	inline bool basicConsumeMessage(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::basicConsumeMessageImpl, paParams, lSizeArray, pvarRetValue);
	}
	inline bool declareQueue(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::declareQueueImpl, paParams, lSizeArray, pvarRetValue);
	}
	inline bool getPriority(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::getPriorityImpl, paParams, lSizeArray, pvarRetValue);
	}
	inline bool getRoutingKey(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::getRoutingKeyImpl, paParams, lSizeArray, pvarRetValue);
	}
	inline bool getHeaders(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::getHeadersImpl, paParams, lSizeArray, pvarRetValue);
	}
	inline bool waitForConfirms(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::waitForConfirmsImpl, paParams, lSizeArray, pvarRetValue);
	}
	inline bool isConnected(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::isConnectedImpl, paParams, lSizeArray, pvarRetValue);
	}

	inline bool getMsgProp(tVariant* pvarPropVal, const long lPropNum) {
		return wrapLongCall(this, &RabbitMQClient::getMsgPropImpl, lPropNum, nullptr, 0, pvarPropVal);
	}
	inline bool setMsgProp(tVariant* varPropVal, const long lPropNum) {
		return wrapLongCall(this, &RabbitMQClient::setMsgPropImpl, lPropNum, varPropVal, 1);
	}

	inline bool sleepNative(tVariant* paParams, const long lSizeArray) {
		return wrapCall(this, &RabbitMQClient::sleepNativeImpl, paParams, lSizeArray);
	}


private:

	void connectImpl(Biterp::CallContext& ctx);

	void declareExchangeImpl(Biterp::CallContext& ctx);
	void deleteExchangeImpl(Biterp::CallContext& ctx);
	void declareQueueImpl(Biterp::CallContext& ctx);
	void deleteQueueImpl(Biterp::CallContext& ctx);
	void bindQueueImpl(Biterp::CallContext& ctx);
	void unbindQueueImpl(Biterp::CallContext& ctx);

	void basicPublishImpl(Biterp::CallContext& ctx);
	void waitForConfirmsImpl(Biterp::CallContext& ctx);

	void basicConsumeImpl(Biterp::CallContext& ctx);
	void basicConsumeMessageImpl(Biterp::CallContext& ctx);
	void basicCancelImpl(Biterp::CallContext& ctx);
	void basicAckImpl(Biterp::CallContext& ctx);
	void basicRejectImpl(Biterp::CallContext& ctx);

	void sleepNativeImpl(Biterp::CallContext& ctx);
	void setLogLevelImpl(Biterp::CallContext& ctx);
	void isConnectedImpl(Biterp::CallContext& ctx);

	void getRoutingKeyImpl(Biterp::CallContext& ctx);
	void getHeadersImpl(Biterp::CallContext& ctx);
	void setPriorityImpl(Biterp::CallContext& ctx);
	void getPriorityImpl(Biterp::CallContext& ctx);

	void getMsgPropImpl(const long propNum, Biterp::CallContext& ctx);
	void setMsgPropImpl(const long propNum, Biterp::CallContext& ctx);

	AMQP::Table headersFromJson(const std::string& json, bool forConsume=false);
	void checkConnection();
	std::string lastMessageHeaders();
	void settle(uint64_t tag, bool multiple, bool ack, bool requeue);
	void setTagResult(Biterp::CallContext& ctx, uint64_t tag, tVariant* param);
	void resetConsumerState();

	// ConnectionListener, IO thread
	void onConsumeChannelOpened(uint32_t channelId) override;
	void onConsumeChannelClosed(uint32_t channelId, const std::string& reason) override;
	void onConnectionLost(const std::string& reason) override;

	// Consumer callbacks, IO thread
	void onConsumerStarted(uint32_t channelId, const std::string& tag);
	void onConsumerCancelled(uint32_t channelId, const std::string& tag);
	// false if the consumer is unknown (the message must be returned to the broker)
	bool onDelivery(uint32_t channelId, const std::string& consumerTag, const AMQP::Message& message,
		uint64_t deliveryTag, bool autoAck);

private:
	struct MessageObject {
		std::string body;
		uint64_t messageTag = 0;   // tag given to 1C, unique for the component object
		uint64_t deliveryTag = 0;  // AMQP delivery tag on its channel
		uint32_t channelId = 0;
		std::string consumerTag;
		bool autoAck = false;
		int priority = 0;
		std::string routingKey;
		std::map<int, std::string> msgProps;
		AMQP::Table headers;
	};

	struct IssuedTag {
		uint32_t channelId = 0;
		uint64_t deliveryTag = 0;
	};

private:
	// publication properties, 1C thread
	std::map<int, std::string> msgProps;
	int priority;
	MessageObject lastMessage;

	std::unique_ptr<Connection> connection;

	// consumer state, guarded by _mutex
	std::mutex _mutex;
	std::condition_variable cvDataArrived;
	std::string consumerError;
	std::vector<std::string> consumers;
	std::deque<MessageObject> messageQueue;
	uint32_t consumeChannelId = 0;
	uint64_t tagBase = 0;          // messageTag = tagBase + deliveryTag on the current channel
	uint64_t lastAssignedTag = 0;  // the biggest messageTag given out so far
	uint64_t staleUpTo = 0;        // tags up to this value belong to closed channels
	std::map<uint64_t, IssuedTag> issuedTags;       // given to 1C, waiting for BasicAck/BasicReject
	std::unordered_set<uint64_t> autoAcked;         // noConfirm consumers: already acknowledged
	std::deque<uint64_t> autoAckedOrder;

private:

	template<typename T, typename Proc>
	bool wrapLongCall(T* obj, Proc proc, const long param, tVariant* paParams, const long lSizeArray,
		tVariant* pvarRetValue = nullptr) {
		bool result = false;
		try {
			skipAddError = false;
			lastError.clear();
			Biterp::CallContext ctx(memManager, paParams, lSizeArray, pvarRetValue);
			(obj->*proc)(param, ctx);
			result = true;
		}
		catch (std::exception& e) {
			reportException(typeid(e).name(), e.what());
		}
		catch (...) {
			reportException("unknown", "Unknown native exception");
		}
		return result;
	}

};
