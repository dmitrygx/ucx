/**
 * Copyright (C) NVIDIA 2022.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include <ucp/api/ucp.h>
#include <ucs/debug/log.h>
#include <tools/perf/api/libperf.h>

#include <cstdlib>
#include <cstring>
#include <csignal>
#include <netinet/in.h>
#include <unistd.h>
#include <list>
#include <set>
#include <algorithm>


typedef struct ucp_perf ucp_perf_t;


typedef struct ucp_perf_thread_context {
    ucp_perf_t              *ucp;
    ucp_worker_h            worker;
    ucp_listener_h          listener;
    std::set<ucp_ep_h>      unmatched_eps;
    ucp_ep_h                ep;
    ucp_ep_h                daemon_ep;
    struct sockaddr_storage daemon_addr;

    ucp_perf_thread_context() : ucp(NULL), worker(NULL), listener(NULL),
                                ep(NULL), daemon_ep(NULL)
    {
    }
} ucp_perf_thread_context_t;

struct ucp_perf {
    ucp_context_h               context;
    ucp_perf_thread_context_t   *tctx;
    std::list<ucs_status_ptr_t> reqs;
};

typedef struct {
    int completed;
} request_t;


static unsigned thread_count         = 1;
static ucs_thread_mode_t thread_mode = UCS_THREAD_MODE_SINGLE;
static uint16_t port                 = 1338;
static int terminated                = 0;

static void request_init(void *request)
{
    request_t *context = (request_t*)request;

    context->completed = 0;
}

static void workers_destroy(ucp_perf_thread_context_t *tctx, unsigned count)
{
    unsigned i;

    for (i = 0; i < count; i++) {
        ucp_worker_destroy(tctx[i].worker);
    }
}

static void ep_close(ucp_perf_t *ucp, ucp_ep_h ep, int force)
{
    unsigned mode = force ? UCP_EP_CLOSE_MODE_FORCE : UCP_EP_CLOSE_MODE_FLUSH;
    ucs_status_ptr_t req;

    req = ucp_ep_close_nb(ep, mode);
    ucp->reqs.push_back(req);
}

static void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status)
{
    ucp_perf_thread_context_t *tctx = (ucp_perf_thread_context_t*)arg;
    std::set<ucp_ep_h>::iterator it;

    if (tctx->ep == ep) {
        tctx->ep = NULL;
        printf("closed ep %p connected to a perftest\n", ep);
    } else if (tctx->daemon_ep == ep) {
        tctx->daemon_ep = NULL;
        printf("closed ep %p connected to a daemon\n", ep);
    } else {
        it = tctx->unmatched_eps.find(ep);
        if (it != tctx->unmatched_eps.end()) {
            tctx->unmatched_eps.erase(it);
        }
    }

    ep_close(tctx->ucp, ep, 1);
}

static void server_conn_handle_cb(ucp_conn_request_h conn_request, void *arg)
{
    ucp_perf_thread_context_t *tctx = (ucp_perf_thread_context_t*)arg;
    ucp_ep_params_t ep_params;
    ucs_status_t status;
    ucp_ep_h ep;

    ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                UCP_EP_PARAM_FIELD_CONN_REQUEST |
                                UCP_EP_PARAM_FIELD_FLAGS |
                                UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
    ep_params.err_mode        = UCP_ERR_HANDLING_MODE_PEER;
    ep_params.flags           = UCP_EP_PARAMS_FLAGS_SHARED_MKEY;
    ep_params.conn_request    = conn_request;
    ep_params.err_handler.cb  = err_cb;
    ep_params.err_handler.arg = tctx;

    status = ucp_ep_create(tctx->worker, &ep_params, &ep);
    if (status != UCS_OK) {
        ucs_error("failed to create an endpoint on the daemon: %s",
                  ucs_status_string(status));
    } else {
        printf("created ep %p to accept connection\n", ep);
    }

    tctx->unmatched_eps.insert(ep);
}

static inline void progress_reqs(ucp_perf_t *ucp)
{
    std::list<void*>::iterator itr;
    void *req;

    itr = ucp->reqs.begin();
    while (itr != ucp->reqs.end()) {
        req = *itr;
        if (req == NULL) {
            itr = ucp->reqs.erase(itr);
        } else if (UCS_PTR_IS_PTR(req)) {
            if (ucp_request_check_status(req) != UCS_INPROGRESS) {
                itr = ucp->reqs.erase(itr);
                ucp_request_release(req);
                continue;
            }
        } else if (UCS_PTR_STATUS(req) != UCS_OK) {
            ucs_warn("failed to complete req %p: %s", req,
                     ucs_status_string(UCS_PTR_STATUS(req)));
        }

        ++itr;
    }
}

static void progress(ucp_perf_t *ucp)
{
    unsigned i;

    for (i = 0; i < thread_count; i++) {
        ucp_worker_progress(ucp->tctx[i].worker);
    }

    progress_reqs(ucp);
}

static int set_am_recv_handler(ucp_worker_h worker, unsigned id,
                               ucp_am_recv_callback_t cb, void *arg)
{
    ucp_am_handler_param_t param;
    ucs_status_t status;

    param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                       UCP_AM_HANDLER_PARAM_FIELD_CB |
                       UCP_AM_HANDLER_PARAM_FIELD_ARG;
    param.id         = id;
    param.cb         = cb;
    param.arg        = arg;
    status           = ucp_worker_set_am_recv_handler(worker, &param);
    if (status != UCS_OK) {
        return -1;
    }

    return 0;
}

static void
am_recv_data(ucp_perf_thread_context_t *tctx, void *desc, size_t length,
             ucp_am_recv_data_nbx_callback_t recv_cb)
{
    ucp_request_param_t params;
    void *req;

    params.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_DATATYPE |
                          UCP_OP_ATTR_FIELD_USER_DATA |
                          UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
    params.datatype     = ucp_dt_make_contig(1);
    params.user_data    = tctx;
    params.cb.recv_am   = recv_cb;

    req = ucp_am_recv_data_nbx(tctx->worker, desc, (void*)&tctx->daemon_addr,
                               length, &params);
    tctx->ucp->reqs.push_back(req);
}

static void am_recv_perf_daemon_init_cb(void *request, ucs_status_t am_status,
                                        size_t length, void *user_data)
{
    ucp_perf_thread_context_t *tctx = (ucp_perf_thread_context_t*)user_data;
    ucp_ep_params_t ep_params;
    ucp_request_param_t request_params;
    ucs_status_t status;
    ucs_status_ptr_t req;

    if (am_status != UCS_OK) {
        ucs_error("failed to receive daemon initialization information from "
                  " a perftest: %s", ucs_status_string(am_status));
        return;
    }

    ep_params.field_mask       = UCP_EP_PARAM_FIELD_FLAGS       |
                                 UCP_EP_PARAM_FIELD_SOCK_ADDR   |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
    ep_params.err_mode         = UCP_ERR_HANDLING_MODE_PEER;
    ep_params.err_handler.cb   = err_cb;
    ep_params.err_handler.arg  = tctx;
    ep_params.flags            = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER |
                                 UCP_EP_PARAMS_FLAGS_SHARED_MKEY;
    ep_params.sockaddr.addr    = (struct sockaddr*)&tctx->daemon_addr;
    ep_params.sockaddr.addrlen = sizeof(tctx->daemon_addr);

    status = ucp_ep_create(tctx->worker, &ep_params, &tctx->daemon_ep);
    if (status != UCS_OK) {
        ucs_error("failed to create an endpoint on the daemon: %s",
                  ucs_status_string(status));
    }

    printf("created ep %p to communicate with daemon\n", tctx->daemon_ep);

    request_params.op_attr_mask = UCP_OP_ATTR_FLAG_NO_IMM_CMPL |
                                  UCP_OP_ATTR_FIELD_FLAGS;
    request_params.flags        = UCP_AM_SEND_FLAG_REPLY;

    req = ucp_am_send_nbx(tctx->daemon_ep, UCP_PERF_DAEMON_AM_ID_PEER_INIT,
                          NULL, 0, NULL, 0, &request_params);
    tctx->ucp->reqs.push_back(req);
}

static ucs_status_t
am_cb_init_ep(ucp_perf_thread_context_t *tctx, ucp_ep_h *ep_storage,
              const ucp_am_recv_param_t *param)
{
    std::set<ucp_ep_h>::iterator it;
    ucp_ep_h ep;

    ucs_assert(param->recv_attr & UCP_AM_RECV_ATTR_FIELD_REPLY_EP);

    ep = param->reply_ep;
    it = tctx->unmatched_eps.find(ep);
    if (it == tctx->unmatched_eps.end()) {
        ucs_error("no ep %p contains in the unmatched endpoints", ep);
        goto err_ep_close;
    } else {
        tctx->unmatched_eps.erase(it);
    }

    if (*ep_storage != NULL) {
        ucs_error("ep %p to the perftest client has already been created on "
                  "the daemon, ep %p won't be used", *ep_storage, ep);
        goto err_ep_close;
    }

    *ep_storage = ep;
    return UCS_OK;

err_ep_close:
    ep_close(tctx->ucp, ep, 1);
    return UCS_ERR_NOT_CONNECTED;
}

static ucs_status_t
am_perf_daemon_init_cb(void *arg, const void *header, size_t header_length,
                       void *data, size_t length,
                       const ucp_am_recv_param_t *param)
{
    ucp_perf_thread_context_t *tctx = (ucp_perf_thread_context_t*)arg;
    ucs_status_t status;

    ucs_assertv(header_length == 0, "header_length=%lu", header_length);

    status = am_cb_init_ep(tctx, &tctx->ep, param);
    if (status != UCS_OK) {
        status = UCS_OK;
        goto out;
    }

    if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) {
        am_recv_data(tctx, data, length,
                     am_recv_perf_daemon_init_cb);
        status = UCS_INPROGRESS;
        goto out;
    }

    memcpy(&tctx->daemon_addr, data,
           std::min(length, sizeof(tctx->daemon_addr)));
    status = UCS_OK;

    am_recv_perf_daemon_init_cb(NULL, UCS_OK, length, tctx);

out:
    return status;
}

static ucs_status_t
am_perf_daemon_peer_init_cb(void *arg, const void *header,
                            size_t header_length, void *data, size_t length,
                            const ucp_am_recv_param_t *param)
{
    ucp_perf_thread_context_t *tctx = (ucp_perf_thread_context_t*)arg;
    ucs_status_t status;

    ucs_assertv(header_length == 0, "header_length=%lu", header_length);
    ucs_assertv(length == 0, "length=%lu", length);

    status = am_cb_init_ep(tctx, &tctx->daemon_ep, param);
    ucs_assertv((status == UCS_OK) || (status == UCS_ERR_NOT_CONNECTED), "%s",
                ucs_status_string(status));

    return UCS_OK;
}

static void cleanup(ucp_perf_t *ucp)
{
    unsigned i;
    std::set<ucp_ep_h>::iterator it;

    for (i = 0; i < thread_count; i++) {
        ucp_listener_destroy(ucp->tctx[i].listener);

        if (ucp->tctx[i].ep != NULL) {
            ep_close(ucp, ucp->tctx[i].ep, 0);
        }

        if (ucp->tctx[i].daemon_ep != NULL) {
            ep_close(ucp, ucp->tctx[i].daemon_ep, 0);
        }

        it = ucp->tctx[i].unmatched_eps.begin();
        while (it != ucp->tctx[i].unmatched_eps.end()) {
            ucp_ep_h ep = *it;

            ep_close(ucp, ep, 0);
            it = ucp->tctx[i].unmatched_eps.erase(it);
        }
    }

    while (!ucp->reqs.empty()) {
        progress(ucp);
    }

    for (i = 0; i < thread_count; i++) {
        ucp_worker_destroy(ucp->tctx[i].worker);
    }

    delete [] ucp->tctx;

    ucp_cleanup(ucp->context);
}

static int init(ucp_perf_t *ucp)
{
    ucp_params_t ucp_params;
    ucp_worker_params_t worker_params;
    struct sockaddr_in listen_addr;
    ucp_listener_params_t listen_params;
    ucp_config_t *config;
    ucs_status_t status;
    unsigned i;
    int ret;

    memset(&listen_addr, 0, sizeof(listen_addr));
    listen_addr.sin_family      = AF_INET;
    listen_addr.sin_addr.s_addr = INADDR_ANY;
    listen_addr.sin_port        = htons(port);

    ucp_params.field_mask   = UCP_PARAM_FIELD_FEATURES |
                              UCP_PARAM_FIELD_REQUEST_SIZE |
                              UCP_PARAM_FIELD_REQUEST_INIT;
    ucp_params.features     = UCP_FEATURE_AM;
    ucp_params.request_size = sizeof(request_t);
    ucp_params.request_init = request_init;

    if (thread_count > 1) {
        /* when there is more than one thread, a ucp_worker would be created for
         * each. all of them will share the same ucp_context */
        ucp_params.field_mask       |= UCP_PARAM_FIELD_MT_WORKERS_SHARED;
        ucp_params.mt_workers_shared = 1;
    }

    status = ucp_config_read(NULL, NULL, &config);
    if (status != UCS_OK) {
        goto err;
    }

    status = ucp_init(&ucp_params, config, &ucp->context);
    ucp_config_release(config);
    if (status != UCS_OK) {
        ucs_error("failed to init UCP: %s", ucs_status_string(status));
        goto err;
    }

    ucp->tctx = new ucp_perf_thread_context_t[thread_count];
    if (ucp->tctx == NULL) {
        ucs_error("failed to allocate memory for thread context");
        goto err_cleanup;
    }

    for (i = 0; i < thread_count; i++) {
        ucp->tctx[i].ucp = ucp;
    }

    worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = thread_mode;

    for (i = 0; i < thread_count; i++) {
        status = ucp_worker_create(ucp->context, &worker_params,
                                   &ucp->tctx[i].worker);
        if (status != UCS_OK) {
            ucs_error("failed to create worker: %s",
                      ucs_status_string(status));
            workers_destroy(ucp->tctx, i);
            goto err_free_tctx;
        }

        ret = set_am_recv_handler(ucp->tctx[i].worker,
                                  UCP_PERF_DAEMON_AM_ID_INIT,
                                  am_perf_daemon_init_cb, &ucp->tctx[i]);
        if (ret != 0) {
            workers_destroy(ucp->tctx, i + 1);
            goto err_free_tctx;
        }

        ret = set_am_recv_handler(ucp->tctx[i].worker,
                                  UCP_PERF_DAEMON_AM_ID_PEER_INIT,
                                  am_perf_daemon_peer_init_cb, &ucp->tctx[i]);
        if (ret != 0) {
            workers_destroy(ucp->tctx, i + 1);
            goto err_free_tctx;
        }
    }

    for (i = 0; i < thread_count; i++) {    
        listen_params.field_mask       = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                                         UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
        listen_params.sockaddr.addr    = (const struct sockaddr*)&listen_addr;
        listen_params.sockaddr.addrlen = sizeof(listen_addr);
        listen_params.conn_handler.cb  = server_conn_handle_cb;
        listen_params.conn_handler.arg = &ucp->tctx[i];

        status = ucp_listener_create(ucp->tctx[i].worker, &listen_params,
                                     &ucp->tctx[i].listener);
        if (status != UCS_OK) {
            ucs_error("failed to listen: %s", ucs_status_string(status));
            goto err_workers_destroy;
        }
    }

    return 0;

err_workers_destroy:
    workers_destroy(ucp->tctx, thread_count);
err_free_tctx:
    delete [] ucp->tctx;
err_cleanup:
    ucp_cleanup(ucp->context);
err:
    return -1;
}

static void signal_terminate_handler(int signo)
{
    char msg[64];
    ssize_t ret __attribute__((unused));

    snprintf(msg, sizeof(msg), "Run-time signal handling: %d\n", signo);
    ret = write(STDOUT_FILENO, msg, strlen(msg) + 1);

    terminated = 1;
}

int main()
{
    ucp_perf_t ucp;
    struct sigaction new_sigaction;

    new_sigaction.sa_handler = signal_terminate_handler;
    new_sigaction.sa_flags   = 0;
    sigemptyset(&new_sigaction.sa_mask);

    if (sigaction(SIGINT, &new_sigaction, NULL) != 0) {
        ucs_error("failed to set signal handler for SIGINT");
        abort();
    }

    if (init(&ucp) != 0) {
        abort();
    }

    while (!terminated) {
        progress(&ucp);
    }

    cleanup(&ucp);

    return 0;
}