//
// Created by ipalenov on 26.01.2021.
//

#ifndef ANDROID_CALLCONTEXT_HPP
#define ANDROID_CALLCONTEXT_HPP

#include <string>
#include "../types.h"
#include "MemoryManager.hpp"
#include "Error.hpp"
#include "Utf.hpp"

namespace Biterp {

    class CallContext {
    public:
        CallContext(const CallContext &) = delete;

        CallContext(const CallContext &&) = delete;

        CallContext operator=(const CallContext &) = delete;

        CallContext operator=(const CallContext &&) = delete;

        CallContext(MemoryManager &memManager, tVariant *paParams, long lSizeArray,
                    tVariant *pvarRetValue) :
                memManager(memManager), params(paParams), retValue(pvarRetValue),
                paramsCount(lSizeArray), index(0) {
        }

        virtual ~CallContext() {}

        tVariant *currentParam() {
            if (index >= paramsCount) {
                throw Error("Extra parameter requested: ") << index;
            }
            return &params[index];
        }

        bool isNullParam() {
            tVariant *param = currentParam();
            return param->vt == VTYPE_EMPTY || param->vt == VTYPE_NULL;
        }

        tVariant *skipParam() {
            tVariant *result = currentParam();
            index++;
            return result;
        }

        std::u16string stringParam(bool nullable = true) {
            tVariant *param = currentParam();
            if (param->vt == VTYPE_PWSTR) {
                index++;
                return std::u16string((char16_t *) param->pwstrVal, param->wstrLen);
            }
            if (nullable && (param->vt == VTYPE_EMPTY || param->vt == VTYPE_NULL)) {
                index++;
                return u"";
            }
            throw TypeError(index, "string", param->vt);
        }

        std::string stringParamUtf8(bool nullable = true) {
            std::u16string value = stringParam(nullable);
            return Utf::toUtf8(value);
        }

        double doubleParam() {
            tVariant *param = currentParam();
            if (param->vt == VTYPE_R4) {
                index++;
                return param->fltVal;
            }
            if (param->vt == VTYPE_R8) {
                index++;
                return param->dblVal;
            }
            throw TypeError(index, "number", param->vt);
        }

        // each integer type is read from its own member of the variant union
        int64_t longParam() {
            tVariant *param = currentParam();
            switch (param->vt) {
                case VTYPE_I1:
                    index++;
                    return param->i8Val;
                case VTYPE_UI1:
                    index++;
                    return param->ui8Val;
                case VTYPE_I2:
                    index++;
                    return param->shortVal;
                case VTYPE_UI2:
                    index++;
                    return param->ushortVal;
                case VTYPE_I4:
                    index++;
                    return param->lVal;
                case VTYPE_UI4:
                    index++;
                    return param->ulVal;
                case VTYPE_I8:
                    index++;
                    return param->llVal;
                case VTYPE_UI8:
                    index++;
                    return static_cast<int64_t>(param->ullVal);
                default:
                    return static_cast<int64_t>(doubleParam());
            }
        }

        int intParam() {
            return static_cast<int>(longParam());
        }

        /**
         * Optional trailing parameter: absent or empty value gives the default.
         */
        bool optBoolParam(bool defaultValue) {
            if (optionalMissing()) {
                return defaultValue;
            }
            return boolParam();
        }

        // Skip an optional parameter of any type
        void skipOptional() {
            if (index < paramsCount) {
                index++;
            }
        }

        std::string optStringParamUtf8(const std::string& defaultValue = std::string()) {
            if (optionalMissing()) {
                return defaultValue;
            }
            return stringParamUtf8();
        }

        int64_t optLongParam(int64_t defaultValue) {
            if (optionalMissing()) {
                return defaultValue;
            }
            return longParam();
        }

        bool boolParam() {
            tVariant *param = currentParam();
            if (param->vt == VTYPE_BOOL) {
                index++;
                return param->bVal;
            }
            throw TypeError(index, "bool", param->vt);
        }

    public:
        inline void setEmptyResult(tVariant *param = nullptr) {
            checkResultParam(param);
        }

        void setBoolResult(bool value, tVariant *param = nullptr) {
            param = checkResultParam(param);
            param->vt = VTYPE_BOOL;
            param->bVal = value;
        }

        void setIntResult(int value, tVariant *param = nullptr) {
            param = checkResultParam(param);
            param->vt = VTYPE_I4;
            param->lVal = value;
        }

        void setStringResult(std::u16string value, tVariant *param = nullptr, bool nullable = false) {
            param = checkResultParam(param);
            if (!value.length() && nullable) {
                return;
            }
            if (!memManager.variantFromString(param, value)) {
                throw Error("Error saving string result to variant");
            }
        }

        inline void setStringOrEmptyResult(std::u16string value, tVariant *param = nullptr) {
            return setStringResult(value, param, true);
        }

        inline void setLongResult(int64_t value, tVariant *param = nullptr) {
            setDoubleResult((double) value, param);
        }

        void setDoubleResult(double value, tVariant *param = nullptr) {
            param = checkResultParam(param);
            param->vt = VTYPE_R8;
            param->dblVal = value;
        }

        void setDateResult(double value, tVariant *param = nullptr) {
            param = checkResultParam(param);
            param->vt = VTYPE_DATE;
            param->date = value;
        }

    protected:
        bool optionalMissing() {
            if (index >= paramsCount) {
                return true;
            }
            tVariant *param = &params[index];
            if (param->vt == VTYPE_EMPTY || param->vt == VTYPE_NULL) {
                index++;
                return true;
            }
            return false;
        }

        tVariant *checkResultParam(tVariant *param) {
            if (!param) {
                param = retValue;
            }
            if (!param) {
                throw Error("Return value variable is null");
            }
            param->vt = VTYPE_EMPTY;
            return param;
        }

    protected:
        tVariant *params;
        tVariant *retValue;
        int index;
        long paramsCount;
        MemoryManager &memManager;
    };
}

#endif //ANDROID_CALLCONTEXT_HPP
