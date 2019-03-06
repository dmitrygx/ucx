/**
* Copyright (C) Mellanox Technologies Ltd. 2001-2013.  ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#include "test.h"

#include <ucs/config/parser.h>
#include <ucs/config/global_opts.h>
#include <ucs/sys/sys.h>

namespace ucs {

const time_t test_timeout_default = 900; // 15 sec

pthread_mutex_t test_base::m_logger_mutex = PTHREAD_MUTEX_INITIALIZER;
unsigned test_base::m_total_warnings = 0;
unsigned test_base::m_total_errors   = 0;
std::vector<std::string> test_base::m_errors;
std::vector<std::string> test_base::m_warnings;

test_base::test_base() :
                m_state(NEW),
                m_initialized(false),
                m_num_threads(1),
                m_test_timeout(test_timeout_default),
                m_num_valgrind_errors_before(0),
                m_num_errors_before(0),
                m_num_warnings_before(0),
                m_num_log_handlers_before(0)
{
    m_comp_threads = new unsigned[m_num_threads];

    pthread_mutex_init(&m_thread_mutex, NULL);
    pthread_cond_init(&m_thread_cv, NULL);

    push_config();
}

test_base::~test_base() {
    while (!m_config_stack.empty()) {
        pop_config();
    }

    pthread_mutex_destroy(&m_thread_mutex);
    pthread_cond_destroy(&m_thread_cv);

    delete[] m_comp_threads;

    ucs_assertv_always(m_state == FINISHED ||
                       m_state == SKIPPED ||
                       m_state == NEW ||    /* can be skipped from a class constructor */
                       m_state == ABORTED,
                       "state=%d", m_state);
}

void test_base::set_num_threads(unsigned num_threads) {
    if (m_state != NEW) {
        GTEST_FAIL() << "Cannot modify number of threads after test is started, "
                     << "it must be done in the constructor.";
    }

    if (num_threads != m_num_threads) {
        delete[] m_comp_threads;

        m_comp_threads = new unsigned[num_threads];
    }

    m_num_threads = num_threads;
}

unsigned test_base::num_threads() const {
    return m_num_threads;
}

void test_base::set_test_timeout(time_t sec) {
    if (m_state != NEW) {
        GTEST_FAIL() << "Cannot modify test timeout after test is started, "
                     << "it must be done in the constructor.";
    }

    m_test_timeout = sec;
}

time_t test_base::test_timeout() const {
    return m_test_timeout;
}

void test_base::set_config(const std::string& config_str)
{
    std::string::size_type pos = config_str.find("=");
    std::string name, value;
    bool optional;

    if (pos == std::string::npos) {
        name  = config_str;
        value = "";
    } else {
        name  = config_str.substr(0, pos);
        value = config_str.substr(pos + 1);
    }

    optional = false;
    if ((name.length() > 0) && name.at(name.length() - 1) == '?') {
        name = name.substr(0, name.length() - 1);
        optional = true;
    }

    modify_config(name, value, optional);
}

void test_base::get_config(const std::string& name, std::string& value, size_t max)
{
    ucs_status_t status;

    value.resize(max, '\0');
    status = ucs_global_opts_get_value(name.c_str(),
                                       const_cast<char*>(value.c_str()),
                                       max);
    if (status != UCS_OK) {
        GTEST_FAIL() << "Invalid UCS configuration for " << name
                     << ", error message: " << ucs_status_string(status)
                     << "(" << status << ")";
    }
}

void test_base::modify_config(const std::string& name, const std::string& value,
                              bool optional)
{
    ucs_status_t status = ucs_global_opts_set_value(name.c_str(), value.c_str());
    if ((status == UCS_ERR_NO_ELEM) && optional) {
        m_env_stack.push_back(new scoped_setenv(("UCX_" + name).c_str(),
                                                value.c_str()));
    } else if (status != UCS_OK) {
        GTEST_FAIL() << "Invalid UCS configuration for " << name << " : "
                     << value << ", error message: "
                     << ucs_status_string(status) << "(" << status << ")";
    }
}

void test_base::push_config()
{
    ucs_global_opts_t new_opts;
    /* save current options to the vector
     * it is important to keep the first original global options at the first
     * vector element to release it at the end. Otherwise, memtrack will not work
     */
    m_config_stack.push_back(ucs_global_opts);
    ucs_global_opts_clone(&new_opts);
    ucs_global_opts = new_opts;
}

void test_base::pop_config()
{
    ucs_global_opts_release();
    ucs_global_opts = m_config_stack.back();
    m_config_stack.pop_back();
}

ucs_log_func_rc_t
test_base::count_warns_logger(const char *file, unsigned line, const char *function,
                              ucs_log_level_t level, const char *message, va_list ap)
{
    pthread_mutex_lock(&m_logger_mutex);
    if (level == UCS_LOG_LEVEL_ERROR) {
        ++m_total_errors;
    } else if (level == UCS_LOG_LEVEL_WARN) {
        ++m_total_warnings;
    }
    pthread_mutex_unlock(&m_logger_mutex);
    return UCS_LOG_FUNC_RC_CONTINUE;
}

std::string test_base::format_message(const char *message, va_list ap)
{
    const size_t buffer_size = ucs_log_get_buffer_size();
    char buf[buffer_size];
    vsnprintf(buf, buffer_size, message, ap);
    return std::string(buf);
}

ucs_log_func_rc_t
test_base::hide_errors_logger(const char *file, unsigned line, const char *function,
                              ucs_log_level_t level, const char *message, va_list ap)
{
    if (level == UCS_LOG_LEVEL_ERROR) {
        pthread_mutex_lock(&m_logger_mutex);
        va_list ap2;
        va_copy(ap2, ap);
        m_errors.push_back(format_message(message, ap2));
        va_end(ap2);
        level = UCS_LOG_LEVEL_DEBUG;
        pthread_mutex_unlock(&m_logger_mutex);
    }

    ucs_log_default_handler(file, line, function, level, message, ap);
    return UCS_LOG_FUNC_RC_STOP;
}

ucs_log_func_rc_t
test_base::hide_warns_logger(const char *file, unsigned line, const char *function,
                             ucs_log_level_t level, const char *message, va_list ap)
{
    if (level == UCS_LOG_LEVEL_WARN) {
        pthread_mutex_lock(&m_logger_mutex);
        va_list ap2;
        va_copy(ap2, ap);
        m_warnings.push_back(format_message(message, ap2));
        va_end(ap2);
        level = UCS_LOG_LEVEL_DEBUG;
        pthread_mutex_unlock(&m_logger_mutex);
    }

    ucs_log_default_handler(file, line, function, level, message, ap);
    return UCS_LOG_FUNC_RC_STOP;
}

ucs_log_func_rc_t
test_base::wrap_errors_logger(const char *file, unsigned line, const char *function,
                              ucs_log_level_t level, const char *message, va_list ap)
{
    /* Ignore warnings about empty memory pool */
    if (level == UCS_LOG_LEVEL_ERROR) {
        pthread_mutex_lock(&m_logger_mutex);
        std::istringstream iss(format_message(message, ap));
        std::string text;
        while (getline(iss, text, '\n')) {
            m_errors.push_back(text);
            UCS_TEST_MESSAGE << "< " << text << " >";
        }
        pthread_mutex_unlock(&m_logger_mutex);
        return UCS_LOG_FUNC_RC_STOP;
    }

    return UCS_LOG_FUNC_RC_CONTINUE;
}

void test_base::SetUpProxy() {
    ucs_assert(m_state == NEW);
    m_num_valgrind_errors_before = VALGRIND_COUNT_ERRORS;
    m_num_warnings_before        = m_total_warnings;
    m_num_errors_before          = m_total_errors;

    m_errors.clear();
    m_num_log_handlers_before    = ucs_log_num_handlers();
    ucs_log_push_handler(count_warns_logger);

    try {
        init();
        m_initialized = true;
        m_state = RUNNING;
    } catch (test_skip_exception& e) {
        skipped(e);
    } catch (test_abort_exception&) {
        m_state = ABORTED;
    }
}

void test_base::TearDownProxy() {
    ucs_assertv_always(m_state == FINISHED ||
                       m_state == SKIPPED ||
                       m_state == ABORTED,
                       "state=%d", m_state);


    if (m_initialized) {
        cleanup();
    }

    m_errors.clear();

    ucs_log_pop_handler();

    unsigned num_not_removed = ucs_log_num_handlers() - m_num_log_handlers_before;
    if (num_not_removed != 0) {
         ADD_FAILURE() << num_not_removed << " log handlers were not removed";
    }

    int num_valgrind_errors = VALGRIND_COUNT_ERRORS - m_num_valgrind_errors_before;
    if (num_valgrind_errors > 0) {
        ADD_FAILURE() << "Got " << num_valgrind_errors << " valgrind errors during the test";
    }
    int num_errors = m_total_errors - m_num_errors_before;
    if (num_errors > 0) {
        ADD_FAILURE() << "Got " << num_errors << " errors during the test";
    }
    int num_warnings = m_total_warnings - m_num_warnings_before;
    if (num_warnings > 0) {
        ADD_FAILURE() << "Got " << num_warnings << " warnings during the test";
    }
}

void test_base::comp_thread_signal(void *arg)
{
    thread_info *info = reinterpret_cast<thread_info*>(arg);

    pthread_mutex_lock(&info->self->m_thread_mutex);

    info->self->m_comp_threads[info->my_id] = 1;
    info->self->m_num_comp_threads++;
    pthread_cond_signal(&info->self->m_thread_cv);

    pthread_mutex_unlock(&info->self->m_thread_mutex);
}
    
test_status_t test_base::wait_comp_threads(pthread_t *threads,
                                           std::string &reason)
{
    test_status_t cur_status = OK;
    int ret = 0;
    struct timeval start, cur;
    struct timespec timeout;
    void *ret_val;
    time_t time_spent_sec = 0;

    gettimeofday(&start, 0);

    timeout.tv_sec  = start.tv_sec + test_timeout();
    timeout.tv_nsec = start.tv_usec * 1000;

    pthread_mutex_lock(&m_thread_mutex);

    while ((m_num_comp_threads != num_threads()) && !ret) {
        ret = pthread_cond_timedwait(&m_thread_cv, &m_thread_mutex,
                                     &timeout);
        if (!ret) {
            gettimeofday(&cur, 0);

            time_spent_sec += cur.tv_sec - start.tv_sec;

            timeout.tv_sec  = cur.tv_sec + test_timeout() - time_spent_sec;
            timeout.tv_nsec = cur.tv_usec * 1000;
        }
    }

    for (unsigned i = 0; i < num_threads(); i++) {
        if (!m_comp_threads[i]) {
            pthread_cancel(threads[i]);
        } else {
            pthread_join(threads[i], &ret_val);

            thread_info *info = reinterpret_cast<thread_info*>(ret_val);

            if (cur_status < info->status) {
                cur_status = info->status;
                reason     = info->reason;
            }
        }
    }

    cur_status = (m_num_comp_threads == num_threads() || (cur_status > TIMEOUT)) ?
                 cur_status : TIMEOUT;

    pthread_mutex_unlock(&m_thread_mutex);

    return cur_status;
}

void test_base::run()
{
    pthread_t threads[num_threads()];
    test_status_t status;
    thread_info *info;
    std::string reason;

    if (num_threads() == 1) {
        info = new thread_info;

        info->self  = this;
        info->my_id = 0;

        m_num_comp_threads = 0;
        m_comp_threads[0]  = 0;

        pthread_create(&threads[0], NULL, thread_func, reinterpret_cast<void*>(info));
        status = wait_comp_threads(threads, reason);

        delete info;
    } else {
        info = new thread_info[num_threads()];

        m_num_comp_threads = 0;

        pthread_barrier_init(&m_barrier, NULL, num_threads());
        for (unsigned i = 0; i < num_threads(); ++i) {

            info[i].self  = this;
            info[i].my_id = i;

            m_comp_threads[i] = 0;

            pthread_create(&threads[i], NULL, thread_func, reinterpret_cast<void*>(info));
        }

        status = wait_comp_threads(threads, reason);
        pthread_barrier_destroy(&m_barrier);

        delete[] info;
    }

    switch (status) {
    case OK:
        m_state = FINISHED;
        break;
    case SKIP:
        skipped(reason);
        break;
    case ABORT:
        m_state = ABORTED;
        break;
    case EXIT:
        if (RUNNING_ON_VALGRIND) {
            /* When running with valgrind, exec true/false instead of just
             * exiting, to avoid warnings about memory leaks of objects
             * allocated inside gtest run loop.
             */
            execlp(reason.c_str(), reason.c_str(), NULL);
        }

        /* If not running on valgrind / execp failed, use exit() */
        exit(reason == "true" ? 1 : 0);
        break;
    case TIMEOUT:
        m_state = ABORTED;
        break;
    case UNKNOWN:
        throw;
    }
}

void *test_base::thread_func(void *arg)
{
    thread_info *info = reinterpret_cast<thread_info*>(arg);

    info->status = OK;

    if (info->self->num_threads() > 1) {
        info->self->barrier(); // Let all threads start in the same time
    }

    try {
        info->self->test_body();
    } catch (test_skip_exception& e) {
        info->status = SKIP;
        info->reason = e.what();
    } catch (test_abort_exception&) {
        info->status = ABORT;
    } catch (exit_exception& e) {
        info->status = EXIT;
        info->reason = e.failed() ? "true" : "false";
    } catch (...) {
        info->status = UNKNOWN;
    }

    comp_thread_signal(reinterpret_cast<void*>(info));

    return reinterpret_cast<void*>(info);
}

void test_base::TestBodyProxy() {
    if (m_state == RUNNING) {
        run();
    } else if (m_state == SKIPPED) {
    } else if (m_state == ABORTED) {
    }
}
    
void test_base::skipped(const std::string &reason) {
    if (reason.empty()) {
        detail::message_stream("SKIP");
    } else {
        detail::message_stream("SKIP") << "(" << reason << ")";
    }
    m_state = SKIPPED;
}

void test_base::skipped(const test_skip_exception& e) {
    skipped(e.what());
}
void test_base::init() {
}

void test_base::cleanup() {
}

bool test_base::barrier() {
    int ret = pthread_barrier_wait(&m_barrier);
    if (ret == 0) {
        return false;
    } else if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
        return true;
    } else {
        UCS_TEST_ABORT("pthread_barrier_wait() failed");
    }

}

}
