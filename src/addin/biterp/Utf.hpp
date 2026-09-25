#ifndef BITERP_UTF_HPP
#define BITERP_UTF_HPP

#include <string>
#include <cstdint>
#include <cstddef>

namespace Biterp {
namespace Utf {

    /**
     * UTF-8 -> UTF-16. Invalid byte sequences are replaced by U+FFFD, never throws.
     */
    inline std::u16string toUtf16(const char* data, size_t size) {
        std::u16string out;
        out.reserve(size);
        const unsigned char* p = reinterpret_cast<const unsigned char*>(data);
        size_t i = 0;
        while (i < size) {
            const unsigned char c = p[i];
            if (c < 0x80) {
                out.push_back(static_cast<char16_t>(c));
                ++i;
                continue;
            }
            uint32_t cp = 0;
            size_t len = 0;
            if ((c & 0xE0) == 0xC0) { cp = c & 0x1F; len = 2; }
            else if ((c & 0xF0) == 0xE0) { cp = c & 0x0F; len = 3; }
            else if ((c & 0xF8) == 0xF0) { cp = c & 0x07; len = 4; }
            bool ok = len != 0 && i + len <= size;
            for (size_t k = 1; ok && k < len; ++k) {
                const unsigned char cc = p[i + k];
                if ((cc & 0xC0) != 0x80) {
                    ok = false;
                }
                cp = (cp << 6) | (cc & 0x3F);
            }
            if (ok) {
                ok = !((len == 2 && cp < 0x80) || (len == 3 && cp < 0x800) ||
                       (len == 4 && (cp < 0x10000 || cp > 0x10FFFF)) || (cp >= 0xD800 && cp <= 0xDFFF));
            }
            if (!ok) {
                out.push_back(static_cast<char16_t>(0xFFFD));
                ++i;
                continue;
            }
            if (cp >= 0x10000) {
                cp -= 0x10000;
                out.push_back(static_cast<char16_t>(0xD800 + (cp >> 10)));
                out.push_back(static_cast<char16_t>(0xDC00 + (cp & 0x3FF)));
            }
            else {
                out.push_back(static_cast<char16_t>(cp));
            }
            i += len;
        }
        return out;
    }

    inline std::u16string toUtf16(const std::string& value) {
        return toUtf16(value.data(), value.size());
    }

    /**
     * UTF-16 -> UTF-8. Unpaired surrogates are replaced by U+FFFD, never throws.
     */
    inline std::string toUtf8(const char16_t* data, size_t size) {
        std::string out;
        out.reserve(size + size / 2);
        size_t i = 0;
        while (i < size) {
            uint32_t cp = data[i];
            ++i;
            if (cp >= 0xD800 && cp <= 0xDBFF) {
                if (i < size && data[i] >= 0xDC00 && data[i] <= 0xDFFF) {
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (data[i] - 0xDC00);
                    ++i;
                }
                else {
                    cp = 0xFFFD;
                }
            }
            else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                cp = 0xFFFD;
            }
            if (cp < 0x80) {
                out.push_back(static_cast<char>(cp));
            }
            else if (cp < 0x800) {
                out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            }
            else if (cp < 0x10000) {
                out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            }
            else {
                out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            }
        }
        return out;
    }

    inline std::string toUtf8(const std::u16string& value) {
        return toUtf8(value.data(), value.size());
    }

}
}

#endif //BITERP_UTF_HPP
