/**
* Copyright (C) Mellanox Technologies Ltd. 2001-2014.  ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/

#ifndef UCP_TEST_H_
#define UCP_TEST_H_

#include <ucp/api/ucp.h>
#include <ucs/time/time.h>
#include <common/mem_buffer.h>

/* ucp version compile time test */
#if (UCP_API_VERSION != UCP_VERSION(UCP_API_MAJOR,UCP_API_MINOR))
#error possible bug in UCP version
#endif

#include <common/test.h>

#include <queue>

#define MT_TEST_NUM_THREADS       4


namespace ucp {
extern const uint32_t MAGIC;
}


struct ucp_test_param {
    ucp_params_t              ctx_params;
    std::vector<std::string>  transports;
    int                       variant;
    int                       thread_type;
};

class ucp_test_base : public ucs::test_base {
public:
    enum {
        SINGLE_THREAD = 42,
        MULTI_THREAD_CONTEXT, /* workers are single-threaded, context is mt-shared */
        MULTI_THREAD_WORKER   /* workers are multi-threaded, cotnext is mt-single */
    };

    class entity {
        typedef std::vector<ucs::handle<ucp_ep_h, entity *> > ep_vec_t;
        typedef std::vector<std::pair<ucs::handle<ucp_worker_h>,
                                      ep_vec_t> > worker_vec_t;

    public:
        typedef enum {
            LISTEN_CB_EP,       /* User's callback accepts ucp_ep_h */
            LISTEN_CB_CONN,     /* User's callback accepts ucp_conn_request_h */
            LISTEN_CB_REJECT    /* User's callback rejects ucp_conn_request_h */
        } listen_cb_type_t;

        entity(const ucp_test_param& test_param, ucp_config_t* ucp_config,
               const ucp_worker_params_t& worker_params);

        ~entity();

        void connect(const entity* other, const ucp_ep_params_t& ep_params,
                     int ep_idx = 0, int do_set_ep = 1);

        ucp_ep_h accept(ucp_worker_h worker, ucp_conn_request_h conn_request);

        void* modify_ep(const ucp_ep_params_t& ep_params, int worker_idx = 0,
                       int ep_idx = 0);

        void* flush_ep_nb(int worker_index = 0, int ep_index = 0) const;

        void* flush_worker_nb(int worker_index = 0) const;

        void fence(int worker_index = 0) const;

        void* disconnect_nb(int worker_index = 0, int ep_index = 0,
                            enum ucp_ep_close_mode mode = UCP_EP_CLOSE_MODE_FLUSH) const;

        void destroy_worker(int worker_index = 0);

        ucs_status_t listen(listen_cb_type_t cb_type,
                            const struct sockaddr *saddr, socklen_t addrlen,
                            int worker_index = 0);

        ucp_ep_h ep(int worker_index = 0, int ep_index = 0) const;

        ucp_ep_h revoke_ep(int worker_index = 0, int ep_index = 0) const;

        ucp_worker_h worker(int worker_index = 0) const;

        ucp_context_h ucph() const;

        ucp_listener_h listenerh() const;

        unsigned progress(int worker_index = 0);

        int get_num_workers() const;

        int get_num_eps(int worker_index = 0) const;

        void inc_rejected_cntr();

        size_t get_rejected_cntr() const;

        void warn_existing_eps() const;

        double set_ib_ud_timeout(double timeout_sec);

        void cleanup();

        static void ep_destructor(ucp_ep_h ep, entity *e);

    protected:
        ucs::handle<ucp_context_h>      m_ucph;
        worker_vec_t                    m_workers;
        ucs::handle<ucp_listener_h>     m_listener;
        std::queue<ucp_conn_request_h>  m_conn_reqs;
        size_t                          m_rejected_cntr;

    private:
        static void empty_send_completion(void *r, ucs_status_t status);
        static void accept_ep_cb(ucp_ep_h ep, void *arg);
        static void accept_conn_cb(ucp_conn_request_h conn_req, void *arg);
        static void reject_conn_cb(ucp_conn_request_h conn_req, void *arg);

        void set_ep(ucp_ep_h ep, int worker_index, int ep_index);
    };
};

/**
 * UCP test
 */
class ucp_test : public ucp_test_base,
                 public ::testing::TestWithParam<ucp_test_param>,
                 public ucs::entities_storage<ucp_test_base::entity> {

    friend class ucp_test_base::entity;

public:
    enum {
        DEFAULT_PARAM_VARIANT = 0
    };

    UCS_TEST_BASE_IMPL;

    ucp_test();
    virtual ~ucp_test();

    ucp_config_t* m_ucp_config;

    static std::vector<ucp_test_param>
    enum_test_params(const ucp_params_t& ctx_params,
                     const std::string& name,
                     const std::string& test_case_name,
                     const std::string& tls);

    static ucp_params_t get_ctx_params();
    virtual ucp_worker_params_t get_worker_params();
    virtual ucp_ep_params_t get_ep_params();

    static void
    generate_test_params_variant(const ucp_params_t& ctx_params,
                                 const std::string& name,
                                 const std::string& test_case_name,
                                 const std::string& tls,
                                 int variant,
                                 std::vector<ucp_test_param>& test_params,
                                 int thread_type = SINGLE_THREAD);

    virtual void modify_config(const std::string& name, const std::string& value,
                               bool optional = false);
    void stats_activate();
    void stats_restore();

private:
    static void set_ucp_config(ucp_config_t *config,
                               const ucp_test_param& test_param);
    static bool check_test_param(const std::string& name,
                                 const std::string& test_case_name,
                                 const ucp_test_param& test_param);

protected:
    virtual void init();
    bool is_self() const;
    virtual void cleanup();
    virtual bool has_transport(const std::string& tl_name) const;
    bool has_only_transports(const std::vector<std::string>& tl_names) const;
    entity* create_entity(bool add_in_front = false);
    entity* create_entity(bool add_in_front, const ucp_test_param& test_param);
    entity* get_entity_by_ep(ucp_ep_h ep);
    unsigned progress(int worker_index = 0) const;
    void short_progress_loop(int worker_index = 0) const;
    void flush_ep(const entity &e, int worker_index = 0, int ep_index = 0);
    void flush_worker(const entity &e, int worker_index = 0);
    void disconnect(const entity& entity);
    void wait(void *req, int worker_index = 0);
    void set_ucp_config(ucp_config_t *config);
    int max_connections();

    static void err_handler_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
        ucp_test *self = reinterpret_cast<ucp_test*>(arg);
        self->m_err_handler_count++;
    }

    template <typename T>
    void wait_for_flag(volatile T *flag, double timeout = 10.0) {
        ucs_time_t loop_end_limit = ucs_get_time() + ucs_time_from_sec(timeout);
        while ((ucs_get_time() < loop_end_limit) && (!(*flag))) {
            short_progress_loop();
        }
    }

    volatile int m_err_handler_count;
    static const ucp_datatype_t DATATYPE;
    static const ucp_datatype_t DATATYPE_IOV;

protected:
    class mapped_buffer : public mem_buffer {
    public:
        mapped_buffer(size_t size, const entity& entity, int flags = 0,
                      ucs_memory_type_t mem_type = UCS_MEMORY_TYPE_HOST);
        virtual ~mapped_buffer();

        ucs::handle<ucp_rkey_h> rkey(const entity& entity) const;

        ucp_mem_h memh() const;

    private:
        const entity& m_entity;
        ucp_mem_h     m_memh;
        void*         m_rkey_buffer;
    };
};


std::ostream& operator<<(std::ostream& os, const ucp_test_param& test_param);


#define UCP_TEST_NO_SELF_TLS_NAMES \
    dcx, udx, ud, rcx, rc, shm_ib, ugni, tcp


#define UCP_TEST_NO_SELF_TLS \
    "dc_x", "ud_x", "ud_v", "rc_x", "rc_v", "shm,ib", "ugni", "tcp"


#define UCP_TEST_TLS_NAMES \
    UCP_TEST_NO_SELF_TLS_NAMES, \
    self


#define UCP_TEST_TLS \
    UCP_TEST_NO_SELF_TLS, \
    "self"


#define _UCP_INSTANTIATE_VECTOR(_vector, _entry) \
    (_vector)->push_back(UCS_PP_QUOTE(_entry));


static inline std::string
_ucp_test_get_tls_by_name(const std::string &name)
{
    static std::map<std::string, std::string> name_tls_map;

    if (name_tls_map.empty()) {
        std::vector<std::string> tls_names, tls;

        UCS_PP_FOREACH(_UCP_INSTANTIATE_VECTOR, &tls_names, UCP_TEST_TLS_NAMES);
        UCS_PP_FOREACH(_UCP_INSTANTIATE_VECTOR, &tls      , UCP_TEST_TLS);

        ucs_assert(tls_names.size() == tls.size());

        for (size_t i = 0; i < tls_names.size(); i++) {
            name_tls_map[tls_names[i]] = tls[i];
        }
    }

    return name_tls_map.find(name)->second;
}


/**
 * Helper macro to Instantiate the parameterized test case a combination
 * of transports.
 *
 * @param _test_case    Test case class, derived from ucp_test.
 * @param _name         Instantiation name.
 * @param _extra_tls    Extra transport names.
 */
#define _UCP_INSTANTIATE_TEST_CASE_TLS_HELPER(_test_case, _name, _extra_tls) \
    INSTANTIATE_TEST_CASE_P(_name, _test_case, \
                            testing::ValuesIn(_test_case::enum_test_params(_test_case::get_ctx_params(), \
                                                                           UCS_PP_QUOTE(_name), \
                                                                           UCS_PP_QUOTE(_test_case), \
                                                                           _ucp_test_get_tls_by_name( \
                                                                               UCS_PP_QUOTE(_name)) + \
                                                                           _extra_tls \
                                                                           )));


/**
 * Instantiate the parameterized test case a combination of transports.
 *
 * @param _test_case    Test case class, derived from ucp_test.
 * @param _name         Instantiation name.
 */
#define _UCP_INSTANTIATE_TEST_CASE_TLS(_test_case, _name) \
    _UCP_INSTANTIATE_TEST_CASE_TLS_HELPER(_test_case, _name, "")


/**
 * Instantiate the parameterized test case a combination of transports with
 * CUDA awareness..
 *
 * @param _test_case    Test case class, derived from ucp_test.
 * @param _name         Instantiation name.
 */
#define _UCP_INSTANTIATE_TEST_CASE_TLS_CUDA_AWARE(_test_case, _name) \
    _UCP_INSTANTIATE_TEST_CASE_TLS_HELPER(_test_case, _name ## _cuda, "cuda_copy")


/**
 * Instantiate the parameterized test case a combination of transports with
 * ROCm awareness.
 *
 * @param _test_case    Test case class, derived from ucp_test.
 * @param _name         Instantiation name.
 */
#define _UCP_INSTANTIATE_TEST_CASE_TLS_ROCM_AWARE(_test_case, _name) \
    _UCP_INSTANTIATE_TEST_CASE_TLS_HELPER(_test_case, _name ## _rocm, "rocm_copy")


/**
 * Instantiate the parameterized test case a combination of transports.
 *
 * @param _test_case   Test case class, derived from ucp_test.
 * @param _name        Instantiation name.
 * @param _tls         Transport names.
 */
#define UCP_INSTANTIATE_TEST_CASE_TLS(_test_case, _name, _tls) \
    INSTANTIATE_TEST_CASE_P(_name, _test_case, \
                            testing::ValuesIn(_test_case::enum_test_params(_test_case::get_ctx_params(), \
                                                                           #_name, \
                                                                           #_test_case, \
                                                                           _tls)));


/**
 * Instantiate the parameterized test case for all transport combinations.
 *
 * @param _test_case  Test case class, derived from ucp_test.
 */
#define UCP_INSTANTIATE_TEST_CASE(_test_case) \
    UCS_PP_FOREACH(_UCP_INSTANTIATE_TEST_CASE_TLS, _test_case, UCP_TEST_TLS_NAMES)


/**
 * Instantiate the parameterized test case for all transport combinations
 * with GPU memory awareness
 *
 * @param _test_case  Test case class, derived from ucp_test.
 */
#define UCP_INSTANTIATE_TEST_CASE_GPU_AWARE(_test_case) \
    UCS_PP_FOREACH(_UCP_INSTANTIATE_TEST_CASE_TLS_CUDA_AWARE, \
                   _test_case, UCP_TEST_NO_SELF_TLS_NAMES) \
    UCP_INSTANTIATE_TEST_CASE_TLS(_test_case, shm_ib_ipc_cuda, \
                                  "shm,ib,cuda_copy,cuda_ipc") \
    UCS_PP_FOREACH(_UCP_INSTANTIATE_TEST_CASE_TLS_ROCM_AWARE, \
                   _test_case, UCP_TEST_NO_SELF_TLS_NAMES) \
    UCP_INSTANTIATE_TEST_CASE_TLS(_test_case, shm_ib_ipc_rocm, \
                                  "shm,ib,rocm_copy,rocm_ipc")


#endif
