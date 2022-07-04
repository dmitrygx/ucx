/**
* Copyright (C) Mellanox Technologies Ltd. 2018.  ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

/*
 * UCP client - server example utility
 * -----------------------------------------------
 *
 * Server side:
 *
 *    ./ucp_client_server
 *
 * Client side:
 *
 *    ./ucp_client_server -a <server-ip>
 *
 * Notes:
 *
 *    - The server will listen to incoming connection requests on INADDR_ANY.
 *    - The client needs to pass the IP address of the server side to connect to
 *      as an argument to the test.
 *    - Currently, the passed IP needs to be an IPoIB or a RoCE address.
 *    - The port which the server side would listen on can be modified with the
 *      '-p' option and should be used on both sides. The default port to use is
 *      13337.
 */

#include "hello_world_util.h"
#include "ucp_client_server_util.h"

#include <ucp/api/ucp.h>

#include <string.h>    /* memset */
#include <arpa/inet.h> /* inet_addr */
#include <unistd.h>    /* getopt */
#include <stdlib.h>    /* atoi */
#include <assert.h>    /* assert */

#define TAG            0xCAFE
#define PRINT_INTERVAL 2000


struct am_data_desc {
    volatile int completed;
    int          is_rndv;
    void         *desc;
    void         *buf;
    size_t       size;
} am_data_desc = { 0, 0, NULL, NULL, 0 };


typedef struct shared_mem_req {
    uint64_t size;
    uint64_t send_address;
    uint64_t recv_address;
    uint64_t send_shared_mkey_buf_size;
    uint64_t recv_shared_mkey_buf_size;
    /* Shared mkey buffers follow in the order:
     * - send
     * - receive
     */
} shared_mem_req_t;


static void *send_address  = NULL;
static void *recv_address  = NULL;
static ucp_mem_h send_memh = NULL;
static ucp_mem_h recv_memh = NULL;


static int shared_mem_import(ucp_context_h ucp_context, void *address,
                             size_t length, void *shared_mkey_buf,
                             ucp_mem_h *memh_p)
{
    ucp_mem_map_params_t params;
    ucs_status_t status;
    ucp_mem_h memh;

    params.field_mask         = UCP_MEM_MAP_PARAM_FIELD_FLAGS |
                                UCP_MEM_MAP_PARAM_FIELD_SHARED_MKEY_BUFFER |
                                UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                                UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    params.flags              = UCP_MEM_MAP_SHARED;
    params.shared_mkey_buffer = shared_mkey_buf;
    params.address            = address;
    params.length             = length;
    status                    = ucp_mem_map(ucp_context, &params, &memh);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to import memory (%s)\n",
                ucs_status_string(status));
        return -1;
    }

    *memh_p = memh;
    return 0;
}

static void shared_mem_import_release(ucp_context_h ucp_context,
                                      ucp_mem_h memh)
{
    ucp_mem_unmap(ucp_context, memh);
}

static void am_request_param_common_init(ucp_request_param_t *params,
                                         ucx_context_t *ctx)
{
    request_init(ctx);

    params->op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                           UCP_OP_ATTR_FIELD_DATATYPE |
                           UCP_OP_ATTR_FIELD_USER_DATA;
    params->datatype     = ucp_dt_make_contig(1);
    params->user_data    = ctx;
}

static int am_send(ucp_ep_h ep, ucp_worker_h ucp_worker, void *buf,
                   size_t buf_size)
{
    ucp_request_param_t params;
    ucx_context_t ctx;
    void *request;
    ucs_status_t status;

    am_request_param_common_init(&params, &ctx);

    params.cb.send = send_cb;
    request        = ucp_am_send_nbx(ep, TEST_AM_ID, NULL, 0ul, buf, buf_size,
                                     &params);

    status = request_wait(ucp_worker, request, &ctx);
    if (status != UCS_OK) {
        fprintf(stderr, "AM send request failed (%s)\n",
                ucs_status_string(status));
        return -1;
    }

    return 0;
}

static int am_recv(ucp_worker_h ucp_worker, ucp_mem_h memh,
                   void **buf_p, size_t *buf_size_p)
{
    ucp_request_param_t params;
    ucx_context_t ctx;
    void *request;
    ucs_status_t status;

    am_request_param_common_init(&params, &ctx);

    /* Waiting for AM callback with  has been called */
    while (!am_data_desc.completed) {
        ucp_worker_progress(ucp_worker);
    }

    am_data_desc.completed = 0;

    if (am_data_desc.is_rndv) {
        /* Rendezvous request has arrived, need to invoke receive operation
         * to confirm data transfer from the sender to the "recv_message"
         * buffer. */
        params.op_attr_mask |= UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
        params.cb.recv_am    = am_recv_cb;

        if (memh != NULL) {
            params.op_attr_mask |= UCP_OP_ATTR_FIELD_MEMH;
            params.memh          = memh;
        }

        request = ucp_am_recv_data_nbx(ucp_worker, am_data_desc.desc,
                                       am_data_desc.buf, am_data_desc.size,
                                       &params);
    } else {
        /* Data has arrived eagerly and is ready for use, no need to
         * initiate receive operation. */
        request = NULL;
    }

    status = request_wait(ucp_worker, request, &ctx);
    if (status != UCS_OK) {
        fprintf(stderr, "AM receive request failed (%s)\n",
                ucs_status_string(status));
        if (memh == NULL) {
            free(am_data_desc.buf);
        }
        return -1;
    }

    *buf_p           = am_data_desc.buf;
    *buf_size_p      = am_data_desc.size;
    am_data_desc.buf = NULL;

    return 0;
}

static int shared_mem_do_operation(ucp_context_h ucp_context,
                                   ucp_worker_h ucp_worker, ucp_ep_h self_ep,
                                   shared_mem_req_t *shared_mem_req_buf)
{
    void *send_shared_mem_buf = (void*)(shared_mem_req_buf + 1);
    void *recv_shared_mem_buf =
            (void*)((char*)(shared_mem_req_buf + 1) +
                    shared_mem_req_buf->send_shared_mkey_buf_size);
    ucp_request_param_t params;
    ucx_context_t send_ctx;
    void *send_request;
    void *buf;
    size_t size;
    ucs_status_t status;
    int ret;
    
    ret = shared_mem_import(ucp_context,
                            (void*)shared_mem_req_buf->send_address,
                            shared_mem_req_buf->size, send_shared_mem_buf,
                            &send_memh);
    if (ret != 0) {
        goto out;
    }

    ret = shared_mem_import(ucp_context,
                            (void*)shared_mem_req_buf->recv_address,
                            shared_mem_req_buf->size, recv_shared_mem_buf,
                            &recv_memh);
    if (ret != 0) {
        goto out_send_shared_mem_import_release;
    }

    am_data_desc.completed = 0;
    am_data_desc.buf       = (void*)shared_mem_req_buf->recv_address;

    /* Send */
    am_request_param_common_init(&params, &send_ctx);
    params.op_attr_mask |= UCP_OP_ATTR_FIELD_MEMH;
    params.cb.send       = send_cb;
    params.memh          = send_memh;
    send_request         =
            ucp_am_send_nbx(self_ep, TEST_AM_ID, NULL, 0ul,
                            (void*)shared_mem_req_buf->send_address,
                            shared_mem_req_buf->size, &params);

    /* Receive */
    ret = am_recv(ucp_worker, recv_memh, &buf, &size);

    status = request_wait(ucp_worker, send_request, &send_ctx);
    if (status != UCS_OK) {
        fprintf(stderr, "AM send request failed (%s)\n",
                ucs_status_string(status));
        return -1;
    }

    shared_mem_import_release(ucp_context, recv_memh);
out_send_shared_mem_import_release:
    shared_mem_import_release(ucp_context, send_memh);
out:
    return ret;
}

ucs_status_t ucp_am_data_cb(void *arg, const void *header, size_t header_length,
                            void *data, size_t length,
                            const ucp_am_recv_param_t *param)
{
    if (header_length != 0) {
        fprintf(stderr, "received unexpected header, length %ld", header_length);
    }

    assert(am_data_desc.completed == 0);

    am_data_desc.completed = 1;
    am_data_desc.size      = length;

    if (am_data_desc.buf == NULL) {
        am_data_desc.buf       = malloc(length);
        if (am_data_desc.buf == NULL) {
            fprintf(stderr, "failed to allocate memory to hold buffer");
            return UCS_ERR_NO_MEMORY;
        }
    }

    if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) {
        /* Rendezvous request arrived, data contains an internal UCX descriptor,
         * which has to be passed to ucp_am_recv_data_nbx function to confirm
         * data transfer.
         */
        am_data_desc.is_rndv = 1;
        am_data_desc.desc    = data;
        return UCS_INPROGRESS;
    }

    /* Message delivered with eager protocol, data should be available
     * immediately
     */
    am_data_desc.is_rndv = 0;
    memcpy(am_data_desc.buf, data, length);

    return UCS_OK;
}

static int
send_recv_am(ucp_context_h ucp_context, ucp_worker_h ucp_worker, ucp_ep_h ep,
             ucp_ep_h self_ep, shared_mem_req_t *shared_mem_req_buf,
             size_t shared_mem_req_buf_size, int current_iter)
{
    int ret                = 0;
    int is_server          = (shared_mem_req_buf == NULL);
    void *am_ack_buf       = NULL;
    size_t am_ack_buf_size = 0;

    if (!is_server) {
        /* Client sends a message to the server using the AM API */
        ret = am_send(ep, ucp_worker, (void*)shared_mem_req_buf,
                      shared_mem_req_buf_size);
        if (ret != 0) {
            return ret;
        }

        ret = am_recv(ucp_worker, NULL, &am_ack_buf, &am_ack_buf_size);
        if (ret != 0) {
            return ret;
        }

        assert(am_ack_buf_size == 0);
    } else {
        ret = am_recv(ucp_worker, NULL, (void**)&shared_mem_req_buf,
                      &shared_mem_req_buf_size);
        if (ret != 0) {
            free(shared_mem_req_buf);
            return ret;
        }

        ret = shared_mem_do_operation(ucp_context, ucp_worker, self_ep,
                                      shared_mem_req_buf);
        if (ret != 0) {
            free(shared_mem_req_buf);
            return ret;
        }

        ret = am_send(ep, ucp_worker, am_ack_buf, am_ack_buf_size);
        if (ret != 0) {
            return ret;
        }
    }

    return 0;
}

static int
client_server_communication(ucp_context_h context, ucp_worker_h worker,
                            ucp_ep_h ep, ucp_ep_h self_ep,
                            send_recv_type_t send_recv_type,
                            shared_mem_req_t *shared_mem_req_buf,
                            size_t shared_mem_req_buf_size,
                            int current_iter)
{
    int ret;

    switch (send_recv_type) {
    case CLIENT_SERVER_SEND_RECV_STREAM:
        break;
    case CLIENT_SERVER_SEND_RECV_TAG:
        break;
    case CLIENT_SERVER_SEND_RECV_AM:
        /* Client-Server communication via AM API. */
        ret = send_recv_am(context, worker, ep, self_ep, shared_mem_req_buf,
                           shared_mem_req_buf_size,
                           current_iter);
        break;
    default:
        fprintf(stderr, "unknown send-recv type %d\n", send_recv_type);
        return -1;
    }

    return ret;
}

static int client_server_do_work(ucp_context_h ucp_context,
                                 ucp_worker_h ucp_worker, ucp_ep_h ep,
                                 ucp_ep_h self_ep,
                                 send_recv_type_t send_recv_type,
                                 shared_mem_req_t *shared_mem_req_buf,
                                 size_t shared_mem_req_buf_size)
{
    int i, ret = 0;

    for (i = 0; i < num_iterations; i++) {
        ret = client_server_communication(ucp_context, ucp_worker, ep, self_ep,
                                          send_recv_type, shared_mem_req_buf,
                                          shared_mem_req_buf_size, i);
        if (ret != 0) {
            fprintf(stderr, "%s failed on iteration #%d\n",
                    ((shared_mem_req_buf == NULL) ? "server": "client"),
                    i + 1);
            goto out;
        }
    }

out:
    return ret;
}

static int create_self_ep(ucp_worker_h ucp_data_worker, ucp_ep_h *self_ep_p)
{
    ucp_ep_params_t ep_params;
    ucs_status_t status;
    ucp_address_t *local_addr;
    size_t local_addr_len;

    status = ucp_worker_get_address(ucp_data_worker, &local_addr, &local_addr_len);
    if (status != UCS_OK) {
        return -1;
    }

    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
                           UCP_EP_PARAM_FIELD_FLAGS;
    ep_params.flags      = UCP_EP_PARAMS_FLAGS_SHARED_MKEY;
    ep_params.address    = local_addr;

    status = ucp_ep_create(ucp_data_worker, &ep_params, self_ep_p);
    ucp_worker_release_address(ucp_data_worker, local_addr);

    return (status == UCS_OK) ? 0 : -1;
}

static int run_server(ucp_context_h ucp_context, ucp_worker_h ucp_worker,
                      char *listen_addr, send_recv_type_t send_recv_type)
{
    ucx_server_ctx_t context;
    ucp_worker_h ucp_data_worker;
    ucp_ep_h server_ep, self_ep;
    ucs_status_t status;
    int ret;

    /* Create a data worker (to be used for data exchange between the server
     * and the client after the connection between them was established) */
    ret = init_worker(ucp_context, &ucp_data_worker);
    if (ret != 0) {
        goto err;
    }

    ret = create_self_ep(ucp_data_worker, &self_ep);
    if (ret != 0) {
        goto err_worker;
    }

    if (send_recv_type == CLIENT_SERVER_SEND_RECV_AM) {
        ret = set_am_recv_handler(ucp_data_worker, TEST_AM_ID, ucp_am_data_cb);
        if (ret != 0) {
            goto err_self_ep;
        }
    }

    /* Initialize the server's context. */
    context.conn_request = NULL;

    /* Create a listener on the worker created at first. The 'connection
     * worker' - used for connection establishment between client and server.
     * This listener will stay open for listening to incoming connection
     * requests from the client */
    status = start_server(ucp_worker, &context, &context.listener, listen_addr);
    if (status != UCS_OK) {
        ret = -1;
        goto err_self_ep;
    }

    /* Server is always up listening */
    while (1) {
        /* Wait for the server to receive a connection request from the client.
         * If there are multiple clients for which the server's connection request
         * callback is invoked, i.e. several clients are trying to connect in
         * parallel, the server will handle only the first one and reject the rest */
        while (context.conn_request == NULL) {
            ucp_worker_progress(ucp_worker);
        }

        /* Server creates an ep to the client on the data worker.
         * This is not the worker the listener was created on.
         * The client side should have initiated the connection, leading
         * to this ep's creation */
        status = server_create_ep(ucp_data_worker, context.conn_request,
                                  UCP_EP_PARAMS_FLAGS_SHARED_MKEY, &server_ep);
        if (status != UCS_OK) {
            ret = -1;
            goto err_listener;
        }

        /* The server waits for all the iterations to complete before moving on
         * to the next client */
        ret = client_server_do_work(ucp_context, ucp_data_worker, server_ep,
                                    self_ep, send_recv_type, NULL, 0);
        if (ret != 0) {
            goto err_ep;
        }

        /* Close the endpoint to the client */
        ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FORCE);

        /* Reinitialize the server's context to be used for the next client */
        context.conn_request = NULL;

        printf("Waiting for connection...\n");
    }

err_ep:
    ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FORCE);
err_listener:
    ucp_listener_destroy(context.listener);
err_self_ep:
    ep_close(ucp_data_worker, self_ep, UCP_EP_CLOSE_MODE_FORCE);
err_worker:
    ucp_worker_destroy(ucp_data_worker);
err:
    return ret;
}

static int shared_mem_export(ucp_context_h ucp_context, size_t length,
                             void **address_p, ucp_mem_h *memh_p,
                             void **shared_mkey_buf_p,
                             size_t *shared_mkey_buf_size_p)
{
    ucp_mem_map_params_t mem_map_params;
    ucp_mkey_pack_params_t mkey_pack_params;
    ucs_status_t status;
    ucp_mem_h memh;
    void *shared_mkey_buf;
    size_t shared_mkey_buf_size;
    void *address;

    address = malloc(length);
    if (address == NULL) {
        goto err;
    }

    mem_map_params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                                UCP_MEM_MAP_PARAM_FIELD_LENGTH  |
                                UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    mem_map_params.address    = address;
    mem_map_params.length     = test_string_length;
    mem_map_params.flags      = UCP_MEM_MAP_SHARED;

    status = ucp_mem_map(ucp_context, &mem_map_params, &memh);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to register memory (%s)\n",
                ucs_status_string(status));
        goto err_mem_free;
    }

    mkey_pack_params.field_mask = UCP_MKEY_PACK_PARAM_FIELD_FLAGS;
    mkey_pack_params.flags      = UCP_MKEY_PACK_FLAG_SHARED;
    status                      = ucp_mkey_pack(ucp_context, memh,
                                                &mkey_pack_params,
                                                &shared_mkey_buf,
                                                &shared_mkey_buf_size);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to pack memory handle (%s)\n",
                ucs_status_string(status));
        goto err_mem_unmap;
    }

    *address_p              = address;
    *memh_p                 = memh;
    *shared_mkey_buf_p      = shared_mkey_buf;
    *shared_mkey_buf_size_p = shared_mkey_buf_size;

    return 0;

err_mem_unmap:
    ucp_mem_unmap(ucp_context, memh);
err_mem_free:
    free(address);
err:
    return -1;
}

static void shared_mem_export_release(ucp_context_h ucp_context,
                                      ucp_mem_h memh, void *shared_mkey_buf)
{
    ucp_mkey_buffer_release_params_t mkey_release_params;

    mkey_release_params.field_mask = UCP_MKEY_BUFFER_RELEASE_PARAM_FIELD_FLAGS;
    mkey_release_params.flags      = UCP_MKEY_BUFFER_RELEASE_FLAG_SHARED;
    ucp_mkey_buffer_release(&mkey_release_params, shared_mkey_buf);

    ucp_mem_unmap(ucp_context, memh);
}

static int run_client(ucp_context_h ucp_context, ucp_worker_h ucp_worker,
                      char *server_addr, send_recv_type_t send_recv_type)
{
    ucp_ep_h client_ep;
    ucs_status_t status;
    int ret;
    void *send_shared_mkey_buf, *recv_shared_mkey_buf;
    size_t send_shared_mkey_buf_size, recv_shared_mkey_buf_size;
    shared_mem_req_t *shared_mem_req_buf;
    size_t shared_mem_req_buf_size;

    if (send_recv_type == CLIENT_SERVER_SEND_RECV_AM) {
        ret = set_am_recv_handler(ucp_worker, TEST_AM_ID, ucp_am_data_cb);
        if (ret != 0) {
            goto out;
        }
    }

    ret = shared_mem_export(ucp_context, test_string_length, &send_address,
                            &send_memh, &send_shared_mkey_buf,
                            &send_shared_mkey_buf_size);
    if (ret != 0) {
        goto out;
    }

    ret = shared_mem_export(ucp_context, test_string_length, &recv_address,
                            &recv_memh, &recv_shared_mkey_buf,
                            &recv_shared_mkey_buf_size);
    if (ret != 0) {
        goto out_send_shared_mem_export_release;
    }

    shared_mem_req_buf_size = sizeof(*shared_mem_req_buf) +
                              send_shared_mkey_buf_size +
                              recv_shared_mkey_buf_size;
    shared_mem_req_buf      = malloc(shared_mem_req_buf_size);
    if (shared_mem_req_buf == NULL) {
        fprintf(stderr, "failed to allocate memory to hold AM request\n");
        goto out_recv_shared_mem_export_release;
    }

    shared_mem_req_buf->size                      = test_string_length;
    shared_mem_req_buf->send_address              = (uintptr_t)send_address;
    shared_mem_req_buf->recv_address              = (uintptr_t)recv_address;
    shared_mem_req_buf->send_shared_mkey_buf_size = send_shared_mkey_buf_size;
    shared_mem_req_buf->recv_shared_mkey_buf_size = recv_shared_mkey_buf_size;

    memcpy(shared_mem_req_buf + 1, send_shared_mkey_buf,
           send_shared_mkey_buf_size);
    memcpy((char*)(shared_mem_req_buf + 1) + send_shared_mkey_buf_size,
           recv_shared_mkey_buf, recv_shared_mkey_buf_size);

    status = start_client(ucp_worker, server_addr,
                          UCP_EP_PARAMS_FLAGS_SHARED_MKEY, &client_ep);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
        ret = -1;
        goto out_shared_mem_req_buf_free;
    }

    ret = client_server_do_work(ucp_context, ucp_worker, client_ep, NULL,
                                send_recv_type, shared_mem_req_buf,
                                shared_mem_req_buf_size);

out_ep_close:
    /* Close the endpoint to the server */
    ep_close(ucp_worker, client_ep, UCP_EP_CLOSE_MODE_FORCE);
out_shared_mem_req_buf_free:
    free(shared_mem_req_buf);
out_recv_shared_mem_export_release:
    shared_mem_export_release(ucp_context, recv_memh, recv_shared_mkey_buf);
out_send_shared_mem_export_release:
    shared_mem_export_release(ucp_context, send_memh, send_shared_mkey_buf);
out:
    return ret;
}

int main(int argc, char **argv)
{
    send_recv_type_t send_recv_type = CLIENT_SERVER_SEND_RECV_DEFAULT;
    char *server_addr = NULL;
    char *listen_addr = NULL;
    int ret;

    /* UCP objects */
    ucp_context_h ucp_context;
    ucp_worker_h  ucp_worker;

    ret = parse_cmd(argc, argv, &server_addr, &listen_addr, &send_recv_type);
    if (ret != 0) {
        goto err;
    }

    /* Initialize the UCX required objects */
    ret = init_context(&ucp_context, &ucp_worker, send_recv_type);
    if (ret != 0) {
        goto err;
    }

    /* Client-Server initialization */
    if (server_addr == NULL) {
        /* Server side */
        ret = run_server(ucp_context, ucp_worker, listen_addr, send_recv_type);
    } else {
        /* Client side */
        ret = run_client(ucp_context, ucp_worker, server_addr, send_recv_type);
    }

    ucp_worker_destroy(ucp_worker);
    ucp_cleanup(ucp_context);
err:
    return ret;
}
