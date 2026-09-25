#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <ctime>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <mutex>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <cstdlib>
#include <sys/stat.h>
#include <filesystem>

#include <nlohmann/json.hpp>
#include "../AddInDefBase.h"

using json = nlohmann::json;
using namespace std::chrono;
namespace fs = std::filesystem;

#if defined(__ANDROID__)

#include <android/log.h>
#include <addin/IAndroidComponentHelper.h>
#include "../jni/jnienv.h"

#define LOCALTIME(V, T) tm V; localtime_r(T, &V)

#endif

#if defined(__linux__)

#include <unistd.h>
#include <pwd.h>

#define GETPID  getpid
#define LOCALTIME(V, T) tm V; localtime_r(T, &V)

#endif

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#include <Shlobj.h>
#include <process.h>

#define GETPID  _getpid
#define LOCALTIME(V, T) tm V; localtime_s(&V, T)

#endif


namespace Biterp {

    /**
     * Log to file and logcat. Rotate logs with level filtering.
     * The logger never throws: a failure to format or to write a record is ignored.
     * Default level is WARNING, it can be changed by setLevel() or by the environment
     * variable PINKRABBITMQ_LOG_LEVEL (D, I, W, E, F).
     */
    class Logging {

    public:
        struct Logger{
            enum Level {
                LDEBUG = 0,
                LINFO,
                LWARNING,
                LERROR,
                LFATAL
            };
            virtual ~Logger(){}
            std::string subname;
            std::string version;
            std::string instance;
            inline bool enabled(int level) const { return Logging::isEnabled(level); }
            inline void debug(const std::string &text) const {Logging::debug(text, *this);}
            inline void info(const std::string &text) const {Logging::info(text, *this);}
            inline void warning(const std::string &text) const {Logging::warning(text, *this);}
            inline void error(const std::string &text) const {Logging::error(text, *this);}
        };


    public:
        inline static Logger getLogger(const std::string &name, const std::string &version, IAddInDefBase *addin, void* compinst) {
            instance()._init(addin, name, version);
            Logger logger;
            logger.subname = name;
            logger.version = version;
            uintptr_t instance = reinterpret_cast<uintptr_t>(compinst);
            logger.instance = std::to_string(instance);
            return logger;
        }

        Logging& getInstance(){
            return instance();
        }

        /**
         * Value of an environment variable of the process (current, not the CRT snapshot).
         */
        static std::string environment(const char* name) {
#if defined(_WIN32) || defined(_WIN64)
            std::vector<char> buffer(512);
            DWORD length = GetEnvironmentVariableA(name, buffer.data(), static_cast<DWORD>(buffer.size()));
            if (length >= buffer.size()) {
                buffer.resize(static_cast<size_t>(length) + 1);
                length = GetEnvironmentVariableA(name, buffer.data(), static_cast<DWORD>(buffer.size()));
            }
            if (length == 0 || length >= buffer.size()) {
                return std::string();
            }
            return std::string(buffer.data(), length);
#else
            const char* value = std::getenv(name);
            return value ? std::string(value) : std::string();
#endif
        }

        inline static bool isEnabled(int level) {
            return level >= instance().minlevel.load(std::memory_order_relaxed);
        }

        inline static void setLevel(int level) {
            if (level < Logger::Level::LDEBUG) {
                level = Logger::Level::LDEBUG;
            }
            if (level > Logger::Level::LFATAL) {
                level = Logger::Level::LFATAL;
            }
            instance().minlevel.store(level, std::memory_order_relaxed);
        }

        /**
         * Parse level name: D/I/W/E/F or debug/info/warning/error/fatal (case insensitive).
         * @return level or -1 if the name is unknown
         */
        static int levelFromName(const std::string& name) {
            if (name.empty()) {
                return -1;
            }
            const char c = static_cast<char>(toupper(static_cast<unsigned char>(name[0])));
            switch (c) {
            case 'D': return Logger::Level::LDEBUG;
            case 'I': return Logger::Level::LINFO;
            case 'W': return Logger::Level::LWARNING;
            case 'E': return Logger::Level::LERROR;
            case 'F': return Logger::Level::LFATAL;
            default: return -1;
            }
        }

        inline static void log(int level, const std::string &text, const Logger& logger) {
            if (!isEnabled(level)) {
                return;
            }
            instance()._log(level, text, logger);
        }

        inline static void debug(const std::string &text, const Logger& logger=defaultLogger()) { log(Logger::Level::LDEBUG, text, logger); }
        inline static void info(const std::string &text, const Logger& logger=defaultLogger()) { log(Logger::Level::LINFO, text, logger); }
        inline static void warning(const std::string &text, const Logger& logger=defaultLogger()) { log(Logger::Level::LWARNING, text, logger); }
        inline static void error(const std::string &text, const Logger& logger=defaultLogger()) { log(Logger::Level::LERROR, text, logger); }

        Logging& setAppName(const std::string& appname){
            std::lock_guard<std::mutex> lock(_mutex);
            record["Appname"] = appname;
            return *this;
        }
        Logging& setDeviceid(const std::string& deviceid){
            std::lock_guard<std::mutex> lock(_mutex);
            record["Deviceid"] = deviceid;
            return *this;
        }

        Logging& setClientid(const std::string& clientid){
            std::lock_guard<std::mutex> lock(_mutex);
            record["Clientid"] = clientid;
            return *this;
        }

        Logging& setLoglevel(const std::string& loglevel){
            int level = levelFromName(loglevel);
            if (level >= 0) {
                setLevel(level);
            }
            return *this;
        }

    private:
        static constexpr int CLEAN_INTERVAL = 600;
        static constexpr int KEEP_TIME = 3 * 24 * 60 * 60;
        static constexpr int FLUSH_INTERVAL = 1;
        static constexpr char FILE_FMT[] = "%Y-%m-%d-%H-%M";
        static constexpr char ISO_FMT[] = "%FT%H:%M:%S";
        static constexpr char PREFIX[] = "comc1c";
        static constexpr char LEVEL_ENV[] = "PINKRABBITMQ_LOG_LEVEL";

        /**
         * Set logs filename, open current logfile.
         * @param name
         * @param addin
         */
        void _init(IAddInDefBase *addin, const std::string& name, const std::string& version) {
            try {
                std::lock_guard<std::mutex> lock(_mutex);
                if (!_fname.empty()) {
                    // already inited
                    return;
                }
                defaultLogger().subname = name;
                defaultLogger().version = version;
                const std::string envLevel = environment(LEVEL_ENV);
                if (!envLevel.empty()) {
                    int level = levelFromName(envLevel);
                    if (level >= 0) {
                        setLevel(level);
                    }
                }
                uint32_t pid = (uint32_t)GETPID();
                _path = getFilePath(addin);
                _fname = _path + PREFIX + std::to_string(pid);
            }
            catch (...) {
            }
        }

        std::string formatTime(const char* fmt, const std::tm* tm, int ms=-1){
            std::ostringstream oss;
            oss << std::put_time(tm, fmt);
            if (ms>=0){
                oss << '.' << std::setfill('0') << std::setw(3) << ms;
            }
            return oss.str();
        }


        /**
         * Save log to file. Multithread, never throws.
         * @param level
         * @param text
         */
        void _log(int level, const std::string &text, const Logger& logger) noexcept {
            try {
                logNative(level, text);
                auto now = system_clock::now();
                auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
                time_t timer = system_clock::to_time_t(now);
                LOCALTIME(tm, &timer);
                std::lock_guard<std::mutex> lock(_mutex);
                if (_fname.empty()) {
                    return;
                }
                std::ofstream& file = getFile(formatTime(FILE_FMT, &tm));
                if (!file) {
                    return;
                }
                std::string line = buildRecord(logger, text, level, formatTime(ISO_FMT, &tm, (int)ms.count()));
                file << line << '\n';
                if (level >= Logger::Level::LWARNING || duration_cast<seconds>(now - flushTime).count() >= FLUSH_INTERVAL) {
                    file.flush();
                    flushTime = now;
                }
                if ((int)duration_cast<seconds>(now - cleanTime).count() < CLEAN_INTERVAL){
                    return;
                }
                cleanTime = now;
                cleanOld(_path);
            }
            catch (...) {
            }
        }

        std::string buildRecord(const Logger& logger, const std::string& message, int level, const std::string& time){
            record["Subsystemname"] = logger.subname;
            record["Version"] = logger.version;
            record["Instance"] = logger.instance;
            record["Description"] = message;
            record["Loglevel"] = levels[level];
            record["Date"] = time;
            return record.dump(-1, ' ', false, json::error_handler_t::replace);
        }

        void cleanOld(std::string path){
            try{
                auto now = system_clock::now();
                std::tm tm = {};
                std::error_code err;
                if (path.empty() || !fs::exists(path, err)){
                    return;
                }
                for (const auto & entry : fs::directory_iterator(path, err)){
                    if (!entry.is_regular_file(err) || entry.path().extension() != ".txt"){
                        continue;
                    }
                    std::string nm = entry.path().stem().string();
                    if (nm.find(Logging::PREFIX) != 0){
                        continue;
                    }
                    size_t pos = nm.find("-");
                    if (pos == std::string::npos){
                        continue;
                    }
                    std::string dt = nm.substr(pos + 1);
                    if (dt.length() != 16 || dt[4]!='-' || dt[7]!='-' || dt[10]!='-' || dt[13]!='-'){
                        continue;
                    }
                    std::istringstream ss(dt);
                    ss >> std::get_time(&tm, Logging::FILE_FMT);
                    if (ss.fail()){
                        continue;
                    }
                    auto diff = now - system_clock::from_time_t(mktime(&tm));
                    if (duration_cast<seconds>(diff).count() > KEEP_TIME){
                        fs::remove(entry.path(), err);
                    }
                }
            }catch(...){
            }
        }

        /**
         * Get log filename with OS specific path.
         * @param addin
         * @return
         */
        std::string getFilePath(IAddInDefBase *addin) {
            std::string path;
#if defined(__ANDROID__)
            IAddInDefBaseEx *addinex = static_cast<IAddInDefBaseEx *>(addin);
            IAndroidComponentHelper *helper = (IAndroidComponentHelper *) addinex->GetInterface(
                    eIAndroidComponentHelper);
            jobject activity = helper->GetActivity();
            JNIEnv *env = JNI::getEnv();
            jclass cls = env->GetObjectClass(activity);
            jmethodID meth = env->GetMethodID(cls, "getExternalFilesDir",
                                              "(Ljava/lang/String;)Ljava/io/File;");
            jobject jfile = env->CallObjectMethod(activity, meth, nullptr);
            env->DeleteLocalRef(cls);
            if (!jfile) {
                logNative(Logger::Level::LERROR, "ExternalFilesDir failed for logger");
                return "";
            }
            cls = env->GetObjectClass(jfile);
            meth = env->GetMethodID(cls, "getAbsolutePath", "()Ljava/lang/String;");
            jstring jpath = (jstring) env->CallObjectMethod(jfile, meth);
            env->DeleteLocalRef(jfile);
            env->DeleteLocalRef(cls);
            if (!jpath) {
                logNative(Logger::Level::LERROR, "AbsolutePath failed for logger");
                return "";
            }
            const char *chars = env->GetStringUTFChars(jpath, nullptr);
            path = chars;
            env->ReleaseStringUTFChars(jpath, chars);
            env->DeleteLocalRef(jpath);
#elif (defined(_WIN32) || defined(_WIN64))
            // create logfile in LOCAL_APPDATA/biterp/logs/<name>.log
            char buf[MAX_PATH] = {0};
            if (SHGetFolderPathA(NULL, CSIDL_LOCAL_APPDATA, NULL, 0, buf) != S_OK) {
                buf[0] = 0;
            }
            path = buf;
            if (path.empty()){
                return "./";
            }
            path += "/biterp";
#elif defined(__linux__)
            struct passwd* pw = getpwuid(getuid());
            if (pw && pw->pw_dir) {
                path = pw->pw_dir;
            }
            else {
                const char* home = std::getenv("HOME");
                path = home ? home : "";
            }
            if (path.empty()){
                return "./";
            }
            path += "/.biterp";
#else
#error "Unsupported OS"
#endif
            if (path.empty()){
                return "./";
            }
            path += "/logs";
            std::error_code err;
            fs::create_directories(path, err);
            return path + "/";
        }

        /**
         * Duplicate log to OS native (logcat).
         * @param level
         * @param text
         */
        void logNative(int level, const std::string &text) {
#if defined(__ANDROID__)
            level += ANDROID_LOG_DEBUG - Logger::Level::LDEBUG;
            __android_log_write(level, defaultLogger().subname.c_str(), text.c_str());
#else
            (void) level;
            (void) text;
#endif
        }

        std::ofstream& getFile(const std::string& date){
            std::string fname = _fname + "-" + date + ".txt";
            if (fname == _current_file && _file.is_open()){
                return _file;
            }
            if (_file.is_open()){
                _file.close();
            }
            _file.clear();
            _file.open(fname, std::ios::out | std::ios::app | std::ios::ate);
            _current_file = fname;
            return _file;
        }

    private:
        std::string _path;
        std::string _fname;
        std::string _current_file;
        std::ofstream _file;
        std::mutex _mutex;
        system_clock::time_point cleanTime;
        system_clock::time_point flushTime;
        json record;
        std::atomic<int> minlevel;
        std::vector<std::string> levels;

    private:
        // Singleton
        inline static Logging &instance() {
            static Logging _inst;
            return _inst;
        }

        inline static Logger &defaultLogger() {
            static Logger _defaultLogger;
            return _defaultLogger;
        }

        Logging(): minlevel(Logger::Level::LWARNING) , levels{"D","I","W","E","F"}{
            record = json({
                {"Appname", ""},
                {"Subsystemname", ""},
                {"Version", ""},
                {"Description", ""},
                {"Loglevel", ""},
                {"Date", ""},
                {"Deviceid", ""},
                {"Clientid", ""},
                {"Instance", ""},
            });
        }

        ~Logging() {
            try {
                if (_file.is_open()) {
                    _file.close();
                }
            }
            catch (...) {
            }
        }

        Logging(const Logging &) = delete;

        Logging(const Logging &&) = delete;

        void operator=(const Logging &) = delete;

        void operator=(const Logging &&) = delete;

    };

}


#endif //LOGGER_HPP
