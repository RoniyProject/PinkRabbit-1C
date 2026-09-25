
#if defined( __linux__ ) || defined(__APPLE__)

#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <iconv.h>
#include <sys/time.h>

#endif

#include <stdio.h>
#include <wchar.h>
#include "RabbitMQClientNative.h"
#include <string>

// Debug trace of every call from 1C, the message is built only when DEBUG is enabled
#define TRACE(M) do { if (LOG_ENABLED(LDEBUG)) { impl.LOGD(M); } } while (0)

Biterp::Names RabbitMQClientNative::properties{{
	{RabbitMQClientNative::ePropVersion, {u"Version"}},
	{RabbitMQClientNative::ePropCorrelationId, {u"CorrelationId"}},
	{RabbitMQClientNative::ePropType, {u"Type"}},
	{RabbitMQClientNative::ePropMessageId, {u"MessageId"}},
	{RabbitMQClientNative::ePropAppId, {u"AppId"}},
	{RabbitMQClientNative::ePropContentEncoding, {u"ContentEncoding"}},
	{RabbitMQClientNative::ePropContentType, {u"ContentType"}},
	{RabbitMQClientNative::ePropUserId, {u"UserId"}},
	{RabbitMQClientNative::ePropClusterId, {u"ClusterId"}},
	{RabbitMQClientNative::ePropExpiration, {u"Expiration"}},
	{RabbitMQClientNative::ePropReplyTo, {u"ReplyTo"}},
}};

Biterp::Names RabbitMQClientNative::methods{{
	{RabbitMQClientNative::eMethGetLastError, {u"GetLastError"}},
	{RabbitMQClientNative::eMethConnect, {u"Connect"}},
	{RabbitMQClientNative::eMethDeclareQueue, {u"DeclareQueue"}},
	{RabbitMQClientNative::eMethBasicPublish, {u"BasicPublish"}},
	{RabbitMQClientNative::eMethBasicConsume, {u"BasicConsume"}},
	{RabbitMQClientNative::eMethBasicConsumeMessage, {u"BasicConsumeMessage"}},
	{RabbitMQClientNative::eMethBasicCancel, {u"BasicCancel"}},
	{RabbitMQClientNative::eMethBasicAck, {u"BasicAck"}},
	{RabbitMQClientNative::eMethDeleteQueue, {u"DeleteQueue"}},
	{RabbitMQClientNative::eMethBindQueue, {u"BindQueue"}},
	{RabbitMQClientNative::eMethBasicReject, {u"BasicReject"}},
	{RabbitMQClientNative::eMethDeclareExchange, {u"DeclareExchange"}},
	{RabbitMQClientNative::eMethDeleteExchange, {u"DeleteExchange"}},
	{RabbitMQClientNative::eMethUnbindQueue, {u"UnbindQueue"}},
	{RabbitMQClientNative::eMethSetPriority, {u"SetPriority"}},
	{RabbitMQClientNative::eMethGetPriority, {u"GetPriority"}},
	{RabbitMQClientNative::eMethGetRoutingKey, {u"GetRoutingKey"}},
	{RabbitMQClientNative::eMethGetHeaders, {u"GetHeaders"}},
	{RabbitMQClientNative::eMethSleepNative, {u"SleepNative"}},
	{RabbitMQClientNative::eMethWaitForConfirms, {u"WaitForConfirms"}},
	{RabbitMQClientNative::eMethSetLogLevel, {u"SetLogLevel"}},
	{RabbitMQClientNative::eMethIsConnected, {u"IsConnected"}},
}};


const char16_t* RabbitMQClientNative::componentName = u"PinkRabbitMQ" QUOTE(NAME_POSTFIX);

namespace {
	void setDefaultInt(tVariant* value, int number) {
		TV_VT(value) = VTYPE_I4;
		TV_I4(value) = number;
	}

	void setDefaultBool(tVariant* value, bool flag) {
		TV_VT(value) = VTYPE_BOOL;
		TV_BOOL(value) = flag;
	}

	void setDefaultEmptyString(tVariant* value) {
		TV_VT(value) = VTYPE_PWSTR;
		TV_WSTR(value) = nullptr;
		value->wstrLen = 0;
	}
}

// CAddInNative
//---------------------------------------------------------------------------//
RabbitMQClientNative::RabbitMQClientNative() {
	TRACE("construct");
}

//---------------------------------------------------------------------------//
RabbitMQClientNative::~RabbitMQClientNative() {
	TRACE("destruct");
}

//---------------------------------------------------------------------------//
bool RabbitMQClientNative::Init(VOID_PTR pConnection) {
	try {
		TRACE("init start");
		bool ret = impl.init(static_cast<IAddInDefBase*>(pConnection));
		TRACE("init end");
		return ret;
	}
	catch (...) {
		return false;
	}
}

//---------------------------------------------------------------------------//
long RabbitMQClientNative::GetInfo() {
	// Component should put supported component technology version
	// This component supports 2.0 version
	return 2000;
}

//---------------------------------------------------------------------------//
void RabbitMQClientNative::Done() {
	try {
		TRACE("done start");
		impl.done();
		TRACE("done end");
	}
	catch (...) {
	}
}

/////////////////////////////////////////////////////////////////////////////
// ILanguageExtenderBase
//---------------------------------------------------------------------------//
bool RabbitMQClientNative::RegisterExtensionAs(WCHAR_T** wsExtensionName) {
	try {
		return impl.memoryManager().copyString((char16_t**)wsExtensionName, componentName);
	}
	catch (...) {
		return false;
	}
}

//---------------------------------------------------------------------------//
long RabbitMQClientNative::GetNProps() {
	return static_cast<long>(properties.size());
}

//---------------------------------------------------------------------------//
long RabbitMQClientNative::FindProp(const WCHAR_T* wsPropName) {
	try {
		long plPropNum = properties.find((char16_t*)wsPropName);
		if (plPropNum == -1)
			impl.setLastError(u"Property not found: " + std::u16string((char16_t*)wsPropName));
		return plPropNum;
	}
	catch (...) {
		return -1;
	}
}

//---------------------------------------------------------------------------//
const WCHAR_T* RabbitMQClientNative::GetPropName(long lPropNum, long lPropAlias) {
	try {
		const std::u16string& name = properties.name(lPropNum, lPropAlias);
		if (name.empty()){
			return NULL;
		}
		return (WCHAR_T*)impl.memoryManager().allocString(name.c_str());
	}
	catch (...) {
		return NULL;
	}
}

//---------------------------------------------------------------------------//
bool RabbitMQClientNative::GetPropVal(const long lPropNum, tVariant* pvarPropVal) {
	try {
		TRACE("1C get prop start " + properties.utf8(lPropNum));
		bool ret = false;
		switch (lPropNum) {
		case ePropVersion:
			ret = impl.getVersion(pvarPropVal);
			break;
		case ePropCorrelationId:
		case ePropType:
		case ePropMessageId:
		case ePropAppId:
		case ePropContentEncoding:
		case ePropContentType:
		case ePropUserId:
		case ePropClusterId:
		case ePropExpiration:
		case ePropReplyTo:
			ret = impl.getMsgProp(pvarPropVal, lPropNum);
			break;
		default:
			ret = false;
			break;
		}
		TRACE("1C get prop end " + properties.utf8(lPropNum));
		return ret;
	}
	catch (...) {
		return false;
	}
}

//---------------------------------------------------------------------------//
bool RabbitMQClientNative::SetPropVal(const long lPropNum, tVariant* varPropVal) {
	try {
		TRACE("1C set prop start " + properties.utf8(lPropNum));
		bool ret = false;
		switch (lPropNum) {
		case ePropCorrelationId:
		case ePropType:
		case ePropMessageId:
		case ePropAppId:
		case ePropContentEncoding:
		case ePropContentType:
		case ePropUserId:
		case ePropClusterId:
		case ePropExpiration:
		case ePropReplyTo:
			ret = impl.setMsgProp(varPropVal, lPropNum);
			break;
		default:
			ret = false;
			break;
		}
		TRACE("1C set prop end " + properties.utf8(lPropNum));
		return ret;
	}
	catch (...) {
		return false;
	}
}

//---------------------------------------------------------------------------//
bool RabbitMQClientNative::IsPropReadable(const long /*lPropNum*/) {
	return true;
}

//---------------------------------------------------------------------------//
bool RabbitMQClientNative::IsPropWritable(const long lPropNum) {
	return lPropNum >= ePropCorrelationId && lPropNum < ePropLast;
}

//---------------------------------------------------------------------------//
long RabbitMQClientNative::GetNMethods() {
	return eMethLast;
}

//---------------------------------------------------------------------------//
long RabbitMQClientNative::FindMethod(const WCHAR_T* wsMethodName) {
	try {
		long plMethodNum = methods.find((char16_t*)wsMethodName);
		if (plMethodNum == -1)
			impl.setLastError(u"Method not found: " + std::u16string((char16_t*)wsMethodName));
		return plMethodNum;
	}
	catch (...) {
		return -1;
	}
}

//---------------------------------------------------------------------------//
const WCHAR_T* RabbitMQClientNative::GetMethodName(const long lMethodNum, const long lMethodAlias) {
	try {
		const std::u16string& name = methods.name(lMethodNum, lMethodAlias);
		if (name.empty()){
			return NULL;
		}
		return (WCHAR_T*)impl.memoryManager().allocString(name.c_str());
	}
	catch (...) {
		return NULL;
	}
}

//---------------------------------------------------------------------------//
long RabbitMQClientNative::GetNParams(const long lMethodNum) {
	switch (lMethodNum)
	{
	case eMethConnect:
		return 8;
	case eMethDeclareQueue:
	case eMethBasicPublish:
		return 7;
	case eMethDeclareExchange:
	case eMethBasicConsume:
		return 6;
	case eMethBasicConsumeMessage:
	case eMethBindQueue:
		return 4;
	case eMethDeleteQueue:
	case eMethUnbindQueue:
		return 3;
	case eMethDeleteExchange:
	case eMethBasicAck:
	case eMethBasicReject:
		return 2;
	case eMethBasicCancel:
	case eMethSetPriority:
	case eMethSleepNative:
	case eMethWaitForConfirms:
	case eMethSetLogLevel:
		return 1;
	default:
		return 0;
	}
}

//---------------------------------------------------------------------------//
bool RabbitMQClientNative::GetParamDefValue(const long lMethodNum, const long lParamNum,
	tVariant* pvarParamDefValue) {
	switch (lMethodNum)
	{
	case eMethConnect:
		// host, port, login, pwd, vhost, pingRate, secure, timeout
		if (lParamNum == 5) {
			setDefaultInt(pvarParamDefValue, 0);
			return true;
		}
		if (lParamNum == 6) {
			setDefaultBool(pvarParamDefValue, false);
			return true;
		}
		if (lParamNum == 7) {
			setDefaultInt(pvarParamDefValue, 5);
			return true;
		}
		break;
	case eMethDeclareQueue:
		// name, onlyCheckIfExists, durable, exclusive, autodelete, maxPriority, arguments
		if (lParamNum == 5) {
			setDefaultInt(pvarParamDefValue, 0);
			return true;
		}
		if (lParamNum == 6) {
			setDefaultEmptyString(pvarParamDefValue);
			return true;
		}
		break;
	case eMethBasicPublish:
		// exchange, routingKey, message, livingTime, persist, arguments, waitConfirm
		if (lParamNum == 5) {
			setDefaultEmptyString(pvarParamDefValue);
			return true;
		}
		if (lParamNum == 6) {
			setDefaultBool(pvarParamDefValue, true);
			return true;
		}
		break;
	case eMethDeclareExchange:
		// name, type, onlyCheckIfExists, durable, autodelete, arguments
		if (lParamNum == 5) {
			setDefaultEmptyString(pvarParamDefValue);
			return true;
		}
		break;
	case eMethBasicConsume:
		// queue, consumerId, noConfirm, exclusive, selectSize, arguments
		if (lParamNum == 4) {
			// Limited prefetch by default, so the input buffer is not flooded
			setDefaultInt(pvarParamDefValue, 200);
			return true;
		}
		if (lParamNum == 5) {
			setDefaultEmptyString(pvarParamDefValue);
			return true;
		}
		break;
	case eMethBindQueue:
		if (lParamNum == 3) {
			setDefaultEmptyString(pvarParamDefValue);
			return true;
		}
		break;
	case eMethBasicAck:
	case eMethBasicReject:
		// messageTag, multiple (ack) / requeue (reject)
		if (lParamNum == 1) {
			setDefaultBool(pvarParamDefValue, false);
			return true;
		}
		break;
	case eMethWaitForConfirms:
		// timeout in milliseconds, -1 - the timeout of Connect
		if (lParamNum == 0) {
			setDefaultInt(pvarParamDefValue, -1);
			return true;
		}
		break;
	}
	return false;
}

//---------------------------------------------------------------------------//
bool RabbitMQClientNative::HasRetVal(const long lMethodNum) {
	switch (lMethodNum)
	{
	case eMethGetLastError:
	case eMethBasicConsume:
	case eMethBasicConsumeMessage:
	case eMethDeclareQueue:
	case eMethGetPriority:
	case eMethGetRoutingKey:
	case eMethGetHeaders:
	case eMethWaitForConfirms:
	case eMethIsConnected:
		return true;
	default:
		return false;
	}
}

//---------------------------------------------------------------------------//
bool RabbitMQClientNative::CallAsProc(const long lMethodNum,
	tVariant* paParams, const long lSizeArray) {
	try {
		TRACE("1C call proc start " + methods.utf8(lMethodNum));
		bool ret = false;
		switch (lMethodNum) {
		case eMethConnect:
			ret = impl.connect(paParams, lSizeArray);
			break;
		case eMethBasicPublish:
			ret = impl.basicPublish(paParams, lSizeArray);
			break;
		case eMethBasicCancel:
			ret = impl.basicCancel(paParams, lSizeArray);
			break;
		case eMethBasicAck:
			ret = impl.basicAck(paParams, lSizeArray);
			break;
		case eMethBasicReject:
			ret = impl.basicReject(paParams, lSizeArray);
			break;
		case eMethDeleteQueue:
			ret = impl.deleteQueue(paParams, lSizeArray);
			break;
		case eMethBindQueue:
			ret = impl.bindQueue(paParams, lSizeArray);
			break;
		case eMethUnbindQueue:
			ret = impl.unbindQueue(paParams, lSizeArray);
			break;
		case eMethDeclareExchange:
			ret = impl.declareExchange(paParams, lSizeArray);
			break;
		case eMethDeleteExchange:
			ret = impl.deleteExchange(paParams, lSizeArray);
			break;
		case eMethSetPriority:
			ret = impl.setPriority(paParams, lSizeArray);
			break;
		case eMethSleepNative:
			ret = impl.sleepNative(paParams, lSizeArray);
			break;
		case eMethSetLogLevel:
			ret = impl.setLogLevel(paParams, lSizeArray);
			break;
		default:
			ret = false;
			break;
		}
		TRACE("1C call proc end " + methods.utf8(lMethodNum));
		return ret;
	}
	catch (...) {
		return false;
	}
}

//---------------------------------------------------------------------------//
bool RabbitMQClientNative::CallAsFunc(const long lMethodNum,
	tVariant* pvarRetValue, tVariant* paParams,
	const long lSizeArray) {
	try {
		TRACE("1C call func start " + methods.utf8(lMethodNum));
		bool ret = false;
		switch (lMethodNum) {
		case eMethGetLastError:
			ret = impl.getLastError(pvarRetValue, paParams, lSizeArray);
			break;
		case eMethBasicConsume:
			ret = impl.basicConsume(pvarRetValue, paParams, lSizeArray);
			break;
		case eMethBasicConsumeMessage:
			ret = impl.basicConsumeMessage(pvarRetValue, paParams, lSizeArray);
			break;
		case eMethDeclareQueue:
			ret = impl.declareQueue(pvarRetValue, paParams, lSizeArray);
			break;
		case eMethGetPriority:
			ret = impl.getPriority(pvarRetValue, paParams, lSizeArray);
			break;
		case eMethGetRoutingKey:
			ret = impl.getRoutingKey(pvarRetValue, paParams, lSizeArray);
			break;
		case eMethGetHeaders:
			ret = impl.getHeaders(pvarRetValue, paParams, lSizeArray);
			break;
		case eMethWaitForConfirms:
			ret = impl.waitForConfirms(pvarRetValue, paParams, lSizeArray);
			break;
		case eMethIsConnected:
			ret = impl.isConnected(pvarRetValue, paParams, lSizeArray);
			break;
		default:
			ret = false;
			break;
		}
		TRACE("1C call func end " + methods.utf8(lMethodNum));
		return ret;
	}
	catch (...) {
		return false;
	}
}


//---------------------------------------------------------------------------//
void RabbitMQClientNative::SetLocale(const WCHAR_T* /*loc*/) {
	// The component does not depend on the CRT locale. Changing it here would change the
	// locale of the whole component for all sessions of the process at once.
}

/////////////////////////////////////////////////////////////////////////////
// LocaleBase
//---------------------------------------------------------------------------//
bool RabbitMQClientNative::setMemManager(void* mem) {
	impl.memoryManager().setHandle((IMemoryManager*)mem);
	return mem != 0;
}
