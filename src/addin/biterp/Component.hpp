//
// Created by ipalenov on 18.12.2020.
//

#ifndef COMPONENT_HPP
#define COMPONENT_HPP

#include <string>
#include <typeinfo>
#include "../AddInDefBase.h"
#include "MemoryManager.hpp"
#include "CallContext.hpp"
#include "Utf.hpp"
#include "Logger.hpp"

#define NATIVE_ERROR    1006

#define DO_QUOTE(...) #__VA_ARGS__
#define QUOTE(X)  "" DO_QUOTE(X)

#define LOGD(M) getLogger().debug(M)
#define LOGI(M) getLogger().info(M)
#define LOGW(M) getLogger().warning(M)
#define LOGE(M) getLogger().error(M)

// Check before building an expensive log message
#define LOG_ENABLED(L) (Biterp::Logging::isEnabled(Biterp::Logging::Logger::L))


namespace Biterp {

    /**
    * Base class for component
    */
    class Component {
    public:
        Component(const char *className) : addin(nullptr), skipAddError(false) {
            this->className = Utf::toUtf16(className);
            this->version = QUOTE(VERSION);
        }

        virtual ~Component() {}

        /**
        * Initialize component..
        * @param addin
        * @return
        */
        virtual bool init(IAddInDefBase *addin) {
            logger = Biterp::Logging::getLogger(Utf::toUtf8(className), version, addin, this);
            LOGD("init");
            this->addin = addin;
            return true;
        }

        /**
        * Deinitialize component.
        */
        virtual void done() {
            LOGD("done");
        }

        inline void setMemoryManager(IMemoryManager *manager) { memManager.setHandle(manager); }

        inline MemoryManager &memoryManager() { return memManager; }

    public:
        //---- common methods

        /**
        * Return lastError field
        * @param pvarRetValue
        * @param paParams
        * @param lSizeArray
        * @return
        */
        inline bool
        getLastError(tVariant *pvarRetValue, tVariant *paParams, const long lSizeArray) {
            return memManager.variantFromString(pvarRetValue, lastError);
        }

        void setLastError(std::u16string error) { lastError = error; }

        /**
        * Return component version
        * @param pvarRetValue
        * @return
        */
        inline bool getVersion(tVariant *pvarRetValue) {
            std::u16string uver = Utf::toUtf16(version);
            return memManager.variantFromString(pvarRetValue, uver);
        }

        inline const Biterp::Logging::Logger& getLogger(){ return logger; }

    protected:
        //---- utils

        /**
        * Raise 1c exception utf16.
        * @param descr  - exception message.
        * @param source - exception source.
        * @param wcode - internal code.
        * @param scode - hresult.
        */
        void addError(const std::u16string &descr, std::u16string source = u"",
                      unsigned short wcode = NATIVE_ERROR,
                      long scode = E_FAIL) {
            setLastError(descr);
            if (skipAddError) {
                return;
            }
            if (!source.length()) {
                source = className;
            }
            if (addin) {
                addin->AddError(wcode, source.c_str(), descr.c_str(), scode);
            }
        }

        /**
        * Raise 1c exception utf8.
        * @param descr
        * @param source
        * @param wcode
        * @param scode
        */
        void addError(const std::string &descr, const std::string &source = "",
                      unsigned short wcode = NATIVE_ERROR,
                      long scode = E_FAIL) {
            addError(Utf::toUtf16(descr), Utf::toUtf16(source), wcode, scode);
        }

        /**
         * Do not call addin->addError on exceptions. Just set lastError
         * @param skip
         */
        void setSkipAddError(bool skip = true) { skipAddError = skip; }

        /**
         * Report exception to log and to 1C. Never throws.
         */
        void reportException(const std::string& who, const std::string& what) noexcept {
            try {
                LOGE(who + ": " + what);
            }
            catch (...) {
            }
            try {
                addError(what, who);
            }
            catch (...) {
            }
        }


    protected:
        /**
         * Template function to call implementation.
         * Never lets an exception out to the platform.
         * @tparam T - proxy object type
         * @tparam Proc - proxy object method type
         * @param obj - proxy object pointer
         * @param proc - method pointer
         * @param paParams - input params array
         * @param lSizeArray - size of input params array
         * @param pvarRetValue - return value or nullptr
         * @return
         */
        template<typename T, typename Proc>
        bool wrapCall(T *obj, Proc proc, tVariant *paParams, const long lSizeArray,
                      tVariant *pvarRetValue = nullptr) {
            bool result = false;
            try {
                skipAddError = false;
                lastError.clear();
                CallContext ctx(memManager, paParams, lSizeArray, pvarRetValue);
                (obj->*proc)(ctx);
                result = true;
            }
            catch (std::exception &e) {
                reportException(typeid(e).name(), e.what());
            }
            catch (...) {
                reportException("unknown", "Unknown native exception");
            }
            return result;
        }


    protected:
        std::u16string className;
        std::string version;
        IAddInDefBase *addin;
        std::u16string lastError;
        MemoryManager memManager;
        bool skipAddError;
        Biterp::Logging::Logger logger;
    };

}

#endif //COMPONENT_HPP
