/**
 * Copyright © 2021 NVIDIA CORPORATION & AFFILIATES.
 *
 * See file LICENSE for terms.
 */

#ifndef IODEMO_H_
#define IODEMO_H_

#include <sys/time.h>


class Utils {
public:
    static void get_time(struct timeval &tv)
    {
        gettimeofday(&tv, NULL);
    }

    static const char* get_time_str(const struct timeval &tv, std::string &str)
    {
        struct tm tm;

        str.resize(32);
        if (UcxLog::use_human_time) {
            strftime(&str[0], str.size(), "[%a %b %d %T] ",
                     localtime_r(&tv.tv_sec, &tm));
        } else {
            snprintf(&str[0], str.size(), "[%lu.%06lu] ", tv.tv_sec,
                     tv.tv_usec);
        }

        return str.c_str();
    }

    static const char* get_time_str(std::string &str)
    {
        struct timeval tv;
        get_time(tv);
        return get_time_str(tv, str);
    }
};

#endif
