/**
* Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
* Copyright (c) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#include "test_ucp_memheap.h"
#include <ucs/sys/sys.h>
#include <common/mem_buffer.h>
extern "C" {
#include <ucp/core/ucp_mm.h>
#include <ucp/core/ucp_ep.inl>
#include <ucp/rma/rma.h>
}

#define TEST_UCP_RMA_BASIC_LENGTH 1000


static std::string rma_err_exp_str;

class test_ucp_rma_basic : public test_ucp_memheap {
public:
    void init() {
        m_local_mem_type  = mem_type_pairs[GetParam().variant][0];
        m_remote_mem_type = mem_type_pairs[GetParam().variant][1];

        test_ucp_memheap::init();
        m_remote_mem_buf = new mem_buffer(TEST_UCP_RMA_BASIC_LENGTH,
                                          m_remote_mem_type);
        m_remote_mem_buf->pattern_fill(m_remote_mem_buf->ptr(),
                                       m_remote_mem_buf->size(), 0,
                                       m_remote_mem_buf->mem_type());
        m_local_mem_buf = new mem_buffer(TEST_UCP_RMA_BASIC_LENGTH,
                                         m_local_mem_type);
        m_local_mem_buf->pattern_fill(m_local_mem_buf->ptr(),
                                      m_local_mem_buf->size(), 0,
                                      m_local_mem_buf->mem_type());
        sender().connect(&receiver(), get_ep_params());

        ucp_mem_map_params_t params;
        params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                            UCP_MEM_MAP_PARAM_FIELD_LENGTH;
        params.address = m_remote_mem_buf->ptr();
        params.length  = m_remote_mem_buf->size();
        mem_map_and_rkey_exchange(receiver(), sender(), params,
                                  m_remote_mem_buf_memh,
                                  m_remote_mem_buf_rkey);
    }

    void cleanup() {
        ucs_status_t status;
        ucp_rkey_destroy(m_remote_mem_buf_rkey);
        status = ucp_mem_unmap(receiver().ucph(), m_remote_mem_buf_memh);
        ASSERT_UCS_OK(status);
        disconnect(sender());
        delete m_local_mem_buf;
        delete m_remote_mem_buf;
        test_ucp_memheap::cleanup();
    }

    static ucp_params_t get_ctx_params() {
        ucp_params_t params = ucp_test::get_ctx_params();
        params.features |= UCP_FEATURE_RMA;
        return params;
    }

    std::vector<ucp_test_param>
    static enum_test_params(const ucp_params_t& ctx_params,
                            const std::string& name,
                            const std::string& test_case_name,
                            const std::string& tls)
    {
        std::vector<ucp_test_param> result;
        int count = 0;

        for (std::vector<std::vector<ucs_memory_type_t> >::const_iterator iter =
                 mem_type_pairs.begin(); iter != mem_type_pairs.end(); ++iter) {
            generate_test_params_variant(ctx_params, name, test_case_name + "/" +
                                         std::string(ucs_memory_type_names[(*iter)[0]]) +
                                         "<->" + std::string(ucs_memory_type_names[(*iter)[1]]),
                                         tls, count++, result);
        }

        return result;
    }

protected:
    static ucs_log_func_rc_t
    rma_error_handler(const char *file, unsigned line, const char *function,
                      ucs_log_level_t level, const char *message, va_list ap)
    {
        // Ignore errors that invalid input parameters as it is expected
        if (level == UCS_LOG_LEVEL_ERROR) {
            std::string err_str = format_message(message, ap);

            if ((err_str.find(rma_err_exp_str) != std::string::npos) ||
                /* the error below occurs when RMA lane can be configured for
                 * current TL (i.e. RMA emulation over AM lane is not used) and
                 * UCT is unable to do registration for the current memory type
                 * (e.g. SHM TLs or IB TLs w/o GPUDirect support)*/
                (err_str.find("remote memory is unreachable") != std::string::npos)) {
                UCS_TEST_MESSAGE << err_str;
                return UCS_LOG_FUNC_RC_STOP;
            }
        }

        return UCS_LOG_FUNC_RC_CONTINUE;
    }

    bool check_gpu_direct_support(ucs_memory_type_t mem_type) {
        return ((m_remote_mem_buf_rkey->cache.rma_lane != UCP_NULL_LANE) &&
                !strcmp(m_remote_mem_buf_rkey->cache.rma_proto->name,
                        "basic_rma") &&
                (ucp_ep_md_attr(sender().ep(),
                                m_remote_mem_buf_rkey->cache.rma_lane)->
                 cap.reg_mem_types & UCS_BIT(mem_type)));
    }

    void check_rma_mem_type_op_status(ucs_status_t status) {
        if ((!UCP_MEM_IS_ACCESSIBLE_FROM_CPU(m_local_mem_type) &&
             !check_gpu_direct_support(m_local_mem_type)) ||
            (!UCP_MEM_IS_ACCESSIBLE_FROM_CPU(m_remote_mem_type) &&
             !check_gpu_direct_support(m_remote_mem_type))) {
            EXPECT_TRUE((status == UCS_ERR_UNSUPPORTED) ||
                        (status == UCS_ERR_UNREACHABLE));
        } else {
            ASSERT_UCS_OK_OR_INPROGRESS(status);
        }
    }

    mem_buffer        *m_remote_mem_buf;
    mem_buffer        *m_local_mem_buf;
    ucp_mem_h         m_remote_mem_buf_memh;
    ucp_rkey_h        m_remote_mem_buf_rkey;
    ucs_memory_type_t m_local_mem_type;
    ucs_memory_type_t m_remote_mem_type;

public:
    static std::vector<std::vector<ucs_memory_type_t> > mem_type_pairs;
};

std::vector<std::vector<ucs_memory_type_t> >
test_ucp_rma_basic::mem_type_pairs = ucs::mem_type_pairs();

UCS_TEST_P(test_ucp_rma_basic, check_mem_type) {
    rma_err_exp_str = "UCP doesn't support RMA for \"" +
                      std::string(ucs_memory_type_names[m_local_mem_type]) +
                      "\"<->\"" +
                      std::string(ucs_memory_type_names[m_remote_mem_type]) +
                      "\" memory types";
    scoped_log_handler log_handler(rma_error_handler);

    ucs_status_t status;
    status = ucp_put_nbi(sender().ep(), m_local_mem_buf->ptr(),
                         m_local_mem_buf->size(),
                         (uintptr_t)m_remote_mem_buf->ptr(),
                         m_remote_mem_buf_rkey);
    check_rma_mem_type_op_status(status);
    flush_ep(sender());

    status = ucp_get_nbi(sender().ep(), m_local_mem_buf->ptr(),
                         m_local_mem_buf->size(),
                         (uintptr_t)m_remote_mem_buf->ptr(),
                         m_remote_mem_buf_rkey);
    check_rma_mem_type_op_status(status);
    flush_ep(sender());
}

UCP_INSTANTIATE_TEST_CASE_CUDA_AWARE(test_ucp_rma_basic)


class test_ucp_rma : public test_ucp_memheap {
private:
    static void send_completion(void *request, ucs_status_t status){}
public:
    void init() {        
        // TODO: need to investigate the slowness of the disabled tests
        if ((GetParam().transports.front().compare("dc_x") == 0) &&
            (GetParam().variant == UCP_MEM_MAP_NONBLOCK)) {
            UCS_TEST_SKIP_R("skipping this test until the slowness is resolved");
        }

        ucp_test::init();
    }

    static ucp_params_t get_ctx_params() {
        ucp_params_t params = ucp_test::get_ctx_params();
        params.features |= UCP_FEATURE_RMA;
        return params;
    }

    std::vector<ucp_test_param>
    static enum_test_params(const ucp_params_t& ctx_params,
                            const std::string& name,
                            const std::string& test_case_name,
                            const std::string& tls)
    {
        std::vector<ucp_test_param> result;
        generate_test_params_variant(ctx_params, name, test_case_name, tls, 0,
                                     result);
        generate_test_params_variant(ctx_params, name, test_case_name + "/map_nb",
                                     tls, UCP_MEM_MAP_NONBLOCK, result);
        return result;
    }

    void nonblocking_put_nbi(entity *e, size_t max_size,
                             void *memheap_addr,
                             ucp_rkey_h rkey,
                             std::string& expected_data)
    {
        ucs_status_t status;
        status = ucp_put_nbi(e->ep(), &expected_data[0], expected_data.length(),
                             (uintptr_t)memheap_addr, rkey);
        ASSERT_UCS_OK_OR_INPROGRESS(status);
    }

    void nonblocking_put_nb(entity *e, size_t max_size,
                            void *memheap_addr,
                            ucp_rkey_h rkey,
                            std::string& expected_data)
    {
        void *status;

        status = ucp_put_nb(e->ep(), &expected_data[0], expected_data.length(),
                            (uintptr_t)memheap_addr, rkey, send_completion);
        ASSERT_UCS_PTR_OK(status);
        if (UCS_PTR_IS_PTR(status)) {
            wait(status);
        }
    }

    void nonblocking_get_nbi(entity *e, size_t max_size,
                             void *memheap_addr,
                             ucp_rkey_h rkey,
                             std::string& expected_data)
    {
        ucs_status_t status;

        ucs::fill_random(memheap_addr, ucs_min(max_size, 16384U));
        status = ucp_get_nbi(e->ep(), (void *)&expected_data[0], expected_data.length(),
                             (uintptr_t)memheap_addr, rkey);
        ASSERT_UCS_OK_OR_INPROGRESS(status);
    }

    void nonblocking_get_nb(entity *e, size_t max_size,
                            void *memheap_addr,
                            ucp_rkey_h rkey,
                            std::string& expected_data)
    {
        void *status;

        ucs::fill_random(memheap_addr, ucs_min(max_size, 16384U));
        status = ucp_get_nb(e->ep(), &expected_data[0], expected_data.length(),
                            (uintptr_t)memheap_addr, rkey, send_completion);
        ASSERT_UCS_PTR_OK(status);
        if (UCS_PTR_IS_PTR(status)) {
            wait(status);
        }
    }

    void test_message_sizes(blocking_send_func_t func, size_t *msizes, int iters, int is_nbi);
};

void test_ucp_rma::test_message_sizes(blocking_send_func_t func, size_t *msizes, int iters, int is_nbi)
{
   int i;

   for (i = 0; msizes[i] > 0; i++) {
       if (is_nbi) {
           test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(func),
                                                 msizes[i], i, 1, false, false);
       } else {
           test_blocking_xfer(func, msizes[i], iters, 1, false, false);
       }
   }
}

UCS_TEST_P(test_ucp_rma, nbi_small) {
    size_t sizes[] = { 8, 24, 96, 120, 250, 0};

    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       sizes, 1000, 1);
    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi), 
                       sizes, 1000, 1);
}

UCS_TEST_P(test_ucp_rma, nbi_med) {
    size_t sizes[] = { 1000, 3000, 9000, 17300, 31000, 99000, 130000, 0};

    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       sizes, 100, 1);
    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi), 
                       sizes, 100, 1);
}

UCS_TEST_SKIP_COND_P(test_ucp_rma, nbi_large, RUNNING_ON_VALGRIND) {
    size_t sizes[] = { 1 * UCS_MBYTE, 3 * UCS_MBYTE, 9 * UCS_MBYTE,
                       17 * UCS_MBYTE, 32 * UCS_MBYTE, 0};

    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       sizes, 3, 1);
    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi), 
                       sizes, 3, 1);
}

UCS_TEST_P(test_ucp_rma, nb_small) {
    size_t sizes[] = { 8, 24, 96, 120, 250, 0};

    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       sizes, 1000, 1);
    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       sizes, 1000, 1);
}

UCS_TEST_P(test_ucp_rma, nb_med) {
    size_t sizes[] = { 1000, 3000, 9000, 17300, 31000, 99000, 130000, 0};

    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       sizes, 100, 1);
    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       sizes, 100, 1);
}

UCS_TEST_SKIP_COND_P(test_ucp_rma, nb_large, RUNNING_ON_VALGRIND) {
    size_t sizes[] = { 1 * UCS_MBYTE, 3 * UCS_MBYTE, 9 * UCS_MBYTE,
                       17 * UCS_MBYTE, 32 * UCS_MBYTE, 0};

    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       sizes, 3, 1);
    test_message_sizes(static_cast<blocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       sizes, 3, 1);
}

UCS_TEST_P(test_ucp_rma, nonblocking_put_nbi_flush_worker) {
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, false);
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, false);
}

UCS_TEST_P(test_ucp_rma, nonblocking_put_nbi_flush_ep) {
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, true);
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, true);
}

UCS_TEST_P(test_ucp_rma, nonblocking_stream_put_nbi_flush_worker) {
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, false);
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, false);
}

UCS_TEST_P(test_ucp_rma, nonblocking_stream_put_nbi_flush_ep) {
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, true);
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, true);
}

UCS_TEST_P(test_ucp_rma, nonblocking_put_nb_flush_worker) {
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, false);
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, false);
}

UCS_TEST_P(test_ucp_rma, nonblocking_put_nb_flush_ep) {
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, true);
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, true);
}

UCS_TEST_P(test_ucp_rma, nonblocking_stream_put_nb_flush_worker) {
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, false);
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, false);
}

UCS_TEST_P(test_ucp_rma, nonblocking_stream_put_nb_flush_ep) {
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, true);
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_put_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, true);
}

UCS_TEST_P(test_ucp_rma, nonblocking_get_nbi_flush_worker) {
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, false);
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, false);
}

UCS_TEST_P(test_ucp_rma, nonblocking_get_nbi_flush_ep) {
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, true);
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, true);
}

UCS_TEST_P(test_ucp_rma, nonblocking_stream_get_nbi_flush_worker) {
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, false);
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, false);
}

UCS_TEST_P(test_ucp_rma, nonblocking_stream_get_nbi_flush_ep) {
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, true);
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nbi),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, true);
}

UCS_TEST_P(test_ucp_rma, nonblocking_get_nb_flush_worker) {
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, false);
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, false);
}

UCS_TEST_P(test_ucp_rma, nonblocking_get_nb_flush_ep) {
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, true);
    test_blocking_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, true);
}

UCS_TEST_P(test_ucp_rma, nonblocking_stream_get_nb_flush_worker) {
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, false);
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, false);
}

UCS_TEST_P(test_ucp_rma, nonblocking_stream_get_nb_flush_ep) {
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, false, true);
    test_nonblocking_implicit_stream_xfer(static_cast<nonblocking_send_func_t>(&test_ucp_rma::nonblocking_get_nb),
                       DEFAULT_SIZE, DEFAULT_ITERS,
                       1, true, true);
}

UCP_INSTANTIATE_TEST_CASE_CUDA_AWARE(test_ucp_rma)
