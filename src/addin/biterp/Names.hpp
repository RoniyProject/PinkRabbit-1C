#ifndef NAMES_HPP
#define NAMES_HPP

#include <string>
#include <map>
#include <utility>
#include "Utf.hpp"


namespace Biterp{

    struct Name{
        std::u16string en;
        std::u16string ru;
        std::string utf8;
        inline const std::u16string& nameEn() const {return en;}
        inline const std::u16string& nameRu() const {return ru;}
        inline const std::string& nameUtf8() const {return utf8;}
    };

    /**
     * Method and property names. All derived values are computed in the constructor,
     * so the tables are read-only after static initialization and can be used from
     * several sessions (threads) of one process at the same time.
     */
    struct Names{
        std::map<int, Name> names;

        Names(std::map<int, Name> source) : names(std::move(source)) {
            for (auto& pair : names) {
                if (pair.second.ru.empty()) {
                    pair.second.ru = pair.second.en;
                }
                pair.second.utf8 = Utf::toUtf8(pair.second.en);
            }
        }

        const std::u16string& empty() const {
            static const std::u16string _empty;
            return _empty;
        }
        const std::string& emptyUtf8() const {
            static const std::string _emptyutf;
            return _emptyutf;
        }

        size_t size() const {
            return names.size();
        }

        const std::u16string& name(int id, int alias) const {
            auto it = names.find(id);
            if (it == names.end() || alias > 1){
                return empty();
            }
            return alias == 1 ? it->second.nameRu() : it->second.nameEn();
        }

        const std::string& utf8(int id) const {
            auto it = names.find(id);
            if (it == names.end()){
                return emptyUtf8();
            }
            return it->second.nameUtf8();
        }

        long find(const std::u16string& name) const {
            for (auto& pair: names){
                if (pair.second.nameEn() == name || pair.second.nameRu() == name){
                    return pair.first;
                }
            }
            return -1;
        }
    };

}

#endif //NAMES_HPP
