/**
 * Copyright (C) Mellanox Technologies Ltd. 2001-2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "tcp.h"

#include <ucs/async/async.h>


static void uct_tcp_ep_epoll_ctl(uct_tcp_ep_t *ep, int op)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    struct epoll_event epoll_event;
    int ret;

    memset(&epoll_event, 0, sizeof(epoll_event));
    epoll_event.data.ptr = ep;
    epoll_event.events   = ep->events;
    ret = epoll_ctl(iface->epfd, op, ep->fd, &epoll_event);
    if (ret < 0) {
        ucs_fatal("epoll_ctl(epfd=%d, op=%d, fd=%d) failed: %m",
                  iface->epfd, op, ep->fd);
    }
}

void uct_tcp_ep_change_conn_state(uct_tcp_ep_t *ep,
                                  uct_tcp_ep_conn_state_t new_conn_state)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    char *str_addr, *str_local_addr;

    ep->conn_state = new_conn_state;

    str_addr = uct_tcp_sockaddr_2_string(ep->peer_addr, NULL, NULL);
    if (!str_addr) {
        return;
    }

    str_local_addr = uct_tcp_sockaddr_2_string(&iface->config.ifaddr, NULL, NULL);
    if (!str_local_addr) {
        ucs_free(str_addr);
        return;
    }

    switch(ep->conn_state) {
    case UCT_TCP_EP_CONN_CLOSED:
        ucs_debug("tcp_ep %s (%p): closed connection to %s",
                  str_local_addr, ep, str_addr);
        break;
    case UCT_TCP_EP_CONN_IN_PROGRESS:
        ucs_debug("tcp_ep %s (%p): connection in progress to %s",
                  str_local_addr, ep, str_addr);
        break;
    case UCT_TCP_EP_CONN_CONNECT_ACK:
        ucs_debug("tcp_ep %s (%p): waiting connection ack from %s",
                  str_local_addr, ep, str_addr);
        break;
    case UCT_TCP_EP_CONN_CONNECTED:
        ucs_debug("tcp_ep %s (%p): connected to %s",
                  str_local_addr, ep, str_addr);
        break;
    case UCT_TCP_EP_CONN_REFUSED:
        ucs_debug("tcp_ep %s (%p): connection refused to %s",
                  str_local_addr, ep, str_addr);
        break;
    }

    ucs_free(str_addr);
    ucs_free(str_local_addr);
}

static inline int uct_tcp_ep_can_send(uct_tcp_ep_t *ep)
{
    ucs_assert(ep->tx->offset <= ep->tx->length);
    /* TODO optimize to allow partial sends/message coalescing */
    return ep->tx->length == 0;
}

static ucs_status_t uct_tcp_ep_ctx_init(uct_tcp_iface_t *iface,
                                        uct_tcp_ep_ctx_t **ctx)
{
    ucs_status_t status;

    *ctx = ucs_malloc(iface->config.buf_size, "tcp_ctx");
    if (*ctx == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    (*ctx)->buf = ucs_malloc(iface->config.buf_size, "tcp_buf");
    if ((*ctx)->buf == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto err_free_ctx;
    }

    (*ctx)->offset = 0;
    (*ctx)->length = 0;

    return UCS_OK;

err_free_ctx:
    ucs_free(*ctx);
    *ctx = NULL;
    return status;
}

static void uct_tcp_ep_ctx_cleanup(uct_tcp_ep_ctx_t **ctx)
{
    ucs_free((*ctx)->buf);

    ucs_free(*ctx);
    *ctx = NULL;
}

static ucs_status_t uct_tcp_ep_init(uct_tcp_iface_t *iface,
                                    const struct sockaddr_in *dest_addr,
                                    uct_tcp_ep_t *ep)
{
    ucs_status_t status;

    status = uct_tcp_ep_ctx_init(iface, &ep->tx);
    if (status != UCS_OK) {
        return status;
    }
    ep->tx->progress = uct_tcp_ep_progress_tx;

    status = uct_tcp_ep_ctx_init(iface, &ep->rx);
    if (status != UCS_OK) {
        goto err_tx_cleanup;
    }
    ep->rx->progress = uct_tcp_ep_progress_rx;

    ep->peer_addr = ucs_malloc(sizeof(*ep->peer_addr), "tcp_peer_name");
    if (ep->peer_addr == NULL) {
        goto err_rx_cleanup;
    }

    if (dest_addr == NULL) {
        memset(ep->peer_addr, 0, sizeof(*ep->peer_addr));
    } else {
        *ep->peer_addr = *dest_addr;
    }

    ucs_queue_head_init(&ep->pending_q);
    ep->events = 0;
    ep->fd = -1;

    return UCS_OK;

err_rx_cleanup:
    uct_tcp_ep_ctx_cleanup(&ep->rx);
err_tx_cleanup:
    uct_tcp_ep_ctx_cleanup(&ep->tx);
    return status;
}

static void uct_tcp_ep_cleanup(uct_tcp_ep_t *ep)
{
    ucs_free(ep->peer_addr);
    uct_tcp_ep_ctx_cleanup(&ep->rx);
    uct_tcp_ep_ctx_cleanup(&ep->tx);
}

static UCS_CLASS_INIT_FUNC(uct_tcp_ep_t, uct_tcp_iface_t *iface,
                           int fd, const struct sockaddr_in *dest_addr)
{
    ucs_status_t status;

    UCS_CLASS_CALL_SUPER_INIT(uct_base_ep_t, &iface->super)

    status = uct_tcp_ep_init(iface, dest_addr, self);
    if (status != UCS_OK) {
        return status;
    }

    self->fd = fd;

    status = ucs_sys_fcntl_modfl(self->fd, O_NONBLOCK, 0);
    if (status != UCS_OK) {
        goto err_ep_cleanup;
    }

    status = uct_tcp_iface_set_sockopt(iface, self->fd);
    if (status != UCS_OK) {
        goto err_ep_cleanup;
    }

    ucs_debug("tcp_ep %p: created on iface %p, fd %d", self, iface, self->fd);
    return UCS_OK;

err_ep_cleanup:
    uct_tcp_ep_cleanup(self);
    return status;
}

static UCS_CLASS_CLEANUP_FUNC(uct_tcp_ep_t)
{
    uct_tcp_iface_t *iface = ucs_derived_of(self->super.super.iface,
                                            uct_tcp_iface_t);

    ucs_debug("tcp_ep %p: destroying", self);

    UCS_ASYNC_BLOCK(iface->super.worker->async);
    ucs_list_del(&self->list);
    UCS_ASYNC_UNBLOCK(iface->super.worker->async);

    uct_tcp_ep_cleanup(self);

    /* Don't remove this check, CM sets -1 to `fd` to keep
     * this socket `fd` opened for further re-use */
    if (self->fd != -1) {
        close(self->fd);
    }
}

UCS_CLASS_DEFINE(uct_tcp_ep_t, uct_base_ep_t);

UCS_CLASS_DEFINE_NAMED_NEW_FUNC(uct_tcp_ep_create, uct_tcp_ep_t, uct_tcp_ep_t,
                                uct_tcp_iface_t*, int,
                                const struct sockaddr_in*)
UCS_CLASS_DEFINE_NAMED_DELETE_FUNC(uct_tcp_ep_destroy, uct_tcp_ep_t, uct_ep_t)

unsigned uct_tcp_ep_connect_req_rx_progress(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    uct_tcp_ep_t *pair_ep  = NULL, *iter_ep;
    uct_tcp_ep_conn_pkt_t conn_pkt;

    if (uct_tcp_recv_blocking(ep->fd, &conn_pkt, sizeof(conn_pkt)) != UCS_OK) {
        ucs_debug("Blocking recv failed on fd %d: %m. Perhaps the peer "
                  "closed the connection. Destroy %p", ep->fd, ep);

        uct_tcp_ep_mod_events(ep, 0, EPOLLIN);
        UCS_ASYNC_BLOCK(iface->super.worker->async);
        ucs_list_add_tail(&iface->ep_list, &ep->list);
        UCS_ASYNC_UNBLOCK(iface->super.worker->async);
        uct_tcp_ep_destroy(&ep->super.super);
        return 0;
    }

    ucs_assertv(conn_pkt.event == UCT_TCP_EP_CONN_REQ, "ep=%p", ep);

    *ep->peer_addr = conn_pkt.data.req.iface_addr;

    uct_tcp_ep_change_conn_state(ep, UCT_TCP_EP_CONN_IN_PROGRESS);

    UCS_ASYNC_BLOCK(iface->super.worker->async);
    ucs_list_for_each(iter_ep, &iface->ep_list, list) {
        if (uct_tcp_sockaddr_cmp(iter_ep->peer_addr, ep->peer_addr) == 0) {
            pair_ep = iter_ep;
            break;
        }
    }
    UCS_ASYNC_UNBLOCK(iface->super.worker->async);

    ucs_assert(!pair_ep || uct_tcp_sockaddr_cmp(pair_ep->peer_addr,
                                                &iface->config.ifaddr) != 0);

    if (pair_ep && pair_ep->conn_state != UCT_TCP_EP_CONN_CONNECTED &&
	uct_tcp_sockaddr_cmp(pair_ep->peer_addr, &iface->config.ifaddr) < 0) {
        /* We must accept this connection and close our one */
        /* 1. Close the allocated socket `fd` to avoid reading any
         *    events for this socket and assign the socket `fd` returned
         *    from `accept()` to the found EP */
        uct_tcp_ep_mod_events(pair_ep, 0, EPOLLOUT | EPOLLIN);
        ucs_assertv(pair_ep->events == 0,
                    "Requsted epoll events must be 0-ed for ep=%p", pair_ep);
        close(pair_ep->fd);
        pair_ep->fd = ep->fd;

        /* 2. Destroy the EP allocated during accepting connection
         *    (set its socket `fd` to -1 prior to avoid closing this socket,
         *     add to `iface::ep_list` to then delete it from this list
         *     inside destroying routine) */
        uct_tcp_ep_mod_events(ep, 0, EPOLLIN);
        ep->fd = -1;
        UCS_ASYNC_BLOCK(iface->super.worker->async);
        ucs_list_add_tail(&iface->ep_list, &ep->list);
        UCS_ASYNC_UNBLOCK(iface->super.worker->async);
        uct_tcp_ep_destroy(&ep->super.super);

        ep = pair_ep;
        pair_ep = NULL;

        uct_tcp_ep_mod_events(ep, EPOLLIN | EPOLLOUT, 0);

        /* 3. Send ACK to the peer */
        conn_pkt.event = UCT_TCP_EP_CONN_ACK;
        if (uct_tcp_send_blocking(ep->fd, &conn_pkt, sizeof(conn_pkt)) != UCS_OK) {
            ucs_error("Blocking send failed on fd %d: %m", ep->fd);
            goto err;
        }

        /* 4. Ok, now we fully connected to the peer. Set appropriate callbacks to
         *    receive EPOLLIN/EPOLLOUT (read/write) events from progress engine.
         *    Note: EPOLLIN has already been requested */
        ep->tx->progress = uct_tcp_ep_progress_tx;
        ep->rx->progress = uct_tcp_ep_progress_rx;

        uct_tcp_ep_change_conn_state(ep, UCT_TCP_EP_CONN_CONNECTED);
    } else if (pair_ep &&
               (pair_ep->conn_state == UCT_TCP_EP_CONN_CONNECTED ||
                uct_tcp_sockaddr_cmp(pair_ep->peer_addr, &iface->config.ifaddr) > 0)) {
        /* We must live with our connection and reject this one */
        /* 1. Destroy the EP allocated during accepting connection
         *    (add to `iface::ep_list` to then delete it from this list
         *     inside destroying routine) */
        uct_tcp_ep_mod_events(ep, 0, EPOLLIN);
        UCS_ASYNC_BLOCK(iface->super.worker->async);
        ucs_list_add_tail(&iface->ep_list, &ep->list);
        UCS_ASYNC_UNBLOCK(iface->super.worker->async);
        uct_tcp_ep_destroy(&ep->super.super);

        ep = pair_ep;
        pair_ep = NULL;
        /* 2. Don't set anything. The connection must be established soon */
    } else {
        /* Just accept this connection and make it operational for RX events only */
        conn_pkt.event = UCT_TCP_EP_CONN_ACK;
        if (uct_tcp_send_blocking(ep->fd, &conn_pkt, sizeof(conn_pkt)) != UCS_OK) {
            ucs_error("Blocking send failed on fd %d: %m", ep->fd);
            goto err;
        }

        ep->tx->progress = uct_tcp_ep_empty_progress;
        ep->rx->progress = uct_tcp_ep_progress_rx;

        uct_tcp_ep_change_conn_state(ep, UCT_TCP_EP_CONN_CONNECTED);

        UCS_ASYNC_BLOCK(iface->super.worker->async);
        ucs_list_add_tail(&iface->ep_list, &ep->list);
        UCS_ASYNC_UNBLOCK(iface->super.worker->async);
    }

    return 0;

err:
    uct_tcp_ep_change_conn_state(ep, UCT_TCP_EP_CONN_REFUSED);
    uct_set_ep_failed(&UCS_CLASS_NAME(uct_tcp_ep_t),
                      &ep->super.super, &iface->super.super,
                      UCS_ERR_UNREACHABLE);
    return 0;
}

static unsigned uct_tcp_ep_connect_ack_rx_progress(uct_tcp_ep_t *ep)
{
    uct_tcp_ep_conn_pkt_t conn_pkt;

    if (uct_tcp_recv_blocking(ep->fd, &conn_pkt, sizeof(conn_pkt)) != UCS_OK) {
        ucs_debug("Blocking recv failed on fd %d: %m. Perhaps the peer "
                  "closed the connection. Nothing to do with %p, just "
                  "wait when it will be re-used", ep->fd, ep);
        return 0;
    }

    ucs_assertv(conn_pkt.event == UCT_TCP_EP_CONN_ACK, "ep=%p", ep);

    /* Don't remove EPOLLIN event and set `ep::rx::progress` to be able
     * receive RX events from progress engine
     * i.e. this EP will be re-used for RX operations
     * as well as for TX operations, because we rejected an incoming
     * connection request from peer */
    ep->tx->progress = uct_tcp_ep_progress_tx;
    ep->rx->progress = uct_tcp_ep_progress_rx;

    uct_tcp_ep_change_conn_state(ep, UCT_TCP_EP_CONN_CONNECTED);

    uct_tcp_ep_mod_events(ep, EPOLLOUT, 0);

    ucs_assertv(ep->events & EPOLLIN, "EPOLLIN must be set for ep=%p", ep);

    /* Progress possible pending operations */
    return ep->tx->progress(ep);
}

static unsigned uct_tcp_ep_connect_progress(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface           = ucs_derived_of(ep->super.super.iface,
                                                      uct_tcp_iface_t);
    socklen_t conn_status_sz         = sizeof(int);
    uct_tcp_ep_conn_pkt_t conn_pkt = {
        .event                       = UCT_TCP_EP_CONN_REQ,
        .data.req.iface_addr         = iface->config.ifaddr,
    };
    int ret, conn_status;

    ret = getsockopt(ep->fd, SOL_SOCKET, SO_ERROR,
                     &conn_status, &conn_status_sz);
    if (ret < 0) {
        ucs_error("Failed to get SO_ERROR on fd %d: %m", ep->fd);
        goto err;
    }

    if (conn_status == EINPROGRESS || conn_status == EWOULDBLOCK) {
        return 0;
    }

    if (conn_status != 0) {
        ucs_error("SO_ERROR returns on fd %d: %d", ep->fd, conn_status);
        goto err;
    }

    if (uct_tcp_send_blocking(ep->fd, &conn_pkt, sizeof(conn_pkt)) != UCS_OK) {
        ucs_debug("Blocking send failed on fd %d: %m", ep->fd);
        return 0;
    }

    ep->tx->progress = uct_tcp_ep_empty_progress;
    ep->rx->progress = uct_tcp_ep_connect_ack_rx_progress;

    uct_tcp_ep_change_conn_state(ep, UCT_TCP_EP_CONN_CONNECT_ACK);

    uct_tcp_ep_mod_events(ep, EPOLLIN, EPOLLOUT);

    return 0;

 err:
    uct_tcp_ep_change_conn_state(ep, UCT_TCP_EP_CONN_REFUSED);
    ucs_error("Non-blocking connect(%s:%d) failed",
              inet_ntoa(ep->peer_addr->sin_addr),
              ntohs(ep->peer_addr->sin_port));
    uct_set_ep_failed(&UCS_CLASS_NAME(uct_tcp_ep_t),
                      &ep->super.super, &iface->super.super,
                      UCS_ERR_UNREACHABLE);
    return 0;
}

unsigned uct_tcp_ep_empty_progress(uct_tcp_ep_t *ep)
{
    return 0;
}

static ucs_status_t uct_tcp_ep_connect_start(uct_tcp_ep_t *ep,
                                             const struct sockaddr_in *dest_addr)
{
    ucs_status_t status;

    status = uct_tcp_socket_connect(ep->fd, dest_addr);
    if (status == UCS_INPROGRESS) {
        ep->tx->progress = uct_tcp_ep_connect_progress;
        ep->rx->progress = uct_tcp_ep_empty_progress;

        uct_tcp_ep_change_conn_state(ep, UCT_TCP_EP_CONN_IN_PROGRESS);

        uct_tcp_ep_mod_events(ep, EPOLLOUT, 0);

        status = UCS_OK;
    } else if (status != UCS_OK) {
        return status;
    } else {
        ep->tx->progress = uct_tcp_ep_progress_tx;
        ep->rx->progress = uct_tcp_ep_empty_progress;

        uct_tcp_ep_change_conn_state(ep, UCT_TCP_EP_CONN_CONNECTED);

        uct_tcp_ep_mod_events(ep, EPOLLOUT, 0);
    }

    return status;
}

ucs_status_t uct_tcp_ep_create_connected(const uct_ep_params_t *params,
                                         uct_ep_h *ep_p)
{
    uct_tcp_iface_t *iface = ucs_derived_of(params->iface, uct_tcp_iface_t);
    uct_tcp_ep_t *ep       = NULL;
    uct_tcp_ep_t *pair_ep  = NULL;
    int fd                 = -1;
    int pair_fd            = -1;
    uct_tcp_ep_t *iter_ep;
    struct sockaddr_in dest_addr;
    ucs_status_t status;

    UCT_EP_PARAMS_CHECK_DEV_IFACE_ADDRS(params);
    memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port   = *(in_port_t*)params->iface_addr;
    dest_addr.sin_addr   = *(struct in_addr*)params->dev_addr;

    UCS_ASYNC_BLOCK(iface->super.worker->async);
    ucs_list_for_each(iter_ep, &iface->ep_list, list) {
        if (uct_tcp_sockaddr_cmp(iter_ep->peer_addr, &dest_addr) == 0) {
            ep = iter_ep;
            break;
        }
    }
    UCS_ASYNC_UNBLOCK(iface->super.worker->async);

    ucs_assert(!ep || uct_tcp_sockaddr_cmp(&iface->config.ifaddr,
                                           ep->peer_addr) != 0);

    if (ep) {
        ucs_assertv(ep->conn_state == UCT_TCP_EP_CONN_CONNECTED, "ep=%p", ep);

        ucs_debug("tcp_ep %p: re-used connection to %s:%d", ep,
                  inet_ntoa(dest_addr.sin_addr), ntohs(dest_addr.sin_port));

        /* Ask progress engine to provide us write events */
        ep->tx->progress = uct_tcp_ep_progress_tx;
    } else if (uct_tcp_sockaddr_cmp(&iface->config.ifaddr,
                                    &dest_addr) == 0) {
        /* Connecting to itself - create an UNIX domain socketpair */
        status = ucs_unix_socketpair_create(&fd, &pair_fd);
        if (status != UCS_OK) {
            return status;
        }

        status = uct_tcp_ep_create(iface, fd, &dest_addr, &ep);
        if (status == UCS_OK) {
            ucs_debug("tcp_ep %p: created EP to %s:%d", ep,
                      inet_ntoa(dest_addr.sin_addr), ntohs(dest_addr.sin_port));
        } else {
            goto err_close_fd;
        }

        status = uct_tcp_ep_create(iface, pair_fd, &dest_addr, &pair_ep);
        if (status == UCS_OK) {
            ucs_debug("tcp_ep %p: created EP to %s:%d", pair_ep,
                      inet_ntoa(dest_addr.sin_addr), ntohs(dest_addr.sin_port));
        } else {
            goto err_ep_destroy;
        }

        ep->tx->progress = uct_tcp_ep_progress_tx;
        ep->rx->progress = uct_tcp_ep_empty_progress;

        pair_ep->tx->progress = uct_tcp_ep_empty_progress;
        pair_ep->rx->progress = uct_tcp_ep_progress_rx;

        /* Note: EPOLLOUT will be set to `ep` (TX endpoint) when it is needed */
        uct_tcp_ep_mod_events(pair_ep, EPOLLIN, 0);

        UCS_ASYNC_BLOCK(iface->super.worker->async);
        ucs_list_add_tail(&iface->ep_list, &ep->list);
        ucs_list_add_tail(&iface->ep_list, &pair_ep->list);
        UCS_ASYNC_UNBLOCK(iface->super.worker->async);
    } else {
        status = ucs_tcpip_socket_create(&fd);
        if (status != UCS_OK) {
            return status;
        }

        status = uct_tcp_ep_create(iface, fd, &dest_addr, &ep);
        if (status == UCS_OK) {
            ucs_debug("tcp_ep %p: created EP to %s:%d", ep,
                      inet_ntoa(dest_addr.sin_addr), ntohs(dest_addr.sin_port));
        } else {
            goto err_close_fd;
        }

        status = uct_tcp_ep_connect_start(ep, &dest_addr);
        if (status != UCS_OK) {
            goto err_ep_destroy;
        }

        UCS_ASYNC_BLOCK(iface->super.worker->async);
        ucs_list_add_tail(&iface->ep_list, &ep->list);
        UCS_ASYNC_UNBLOCK(iface->super.worker->async);
    }

    *ep_p = &ep->super.super;

    return status;

err_ep_destroy:
    uct_tcp_ep_destroy(&ep->super.super);
err_close_fd:
    if (fd != -1) {
        close(fd);
    }
    if (pair_fd != -1) {
        close(pair_fd);
    }
    return status;
}

void uct_tcp_ep_mod_events(uct_tcp_ep_t *ep, uint32_t add, uint32_t remove)
{
    uint32_t old_events = ep->events;
    uint32_t new_events = (ep->events | add) & ~remove;

    if (new_events != ep->events) {
        ep->events = new_events;
        ucs_trace("tcp_ep %p: set events to %c%c", ep,
                  (new_events & EPOLLIN)  ? 'i' : '-',
                  (new_events & EPOLLOUT) ? 'o' : '-');
        if (new_events == 0) {
            uct_tcp_ep_epoll_ctl(ep, EPOLL_CTL_DEL);
        } else if (old_events != 0) {
            uct_tcp_ep_epoll_ctl(ep, EPOLL_CTL_MOD);
        } else {
            uct_tcp_ep_epoll_ctl(ep, EPOLL_CTL_ADD);
        }
    }
}

static unsigned uct_tcp_ep_send(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface, uct_tcp_iface_t);
    size_t send_length;
    ucs_status_t status;

    send_length = ep->tx->length - ep->tx->offset;
    ucs_assert(send_length > 0);

    status = uct_tcp_send(ep->fd, ep->tx->buf + ep->tx->offset, &send_length);
    if (status < 0) {
        return 0;
    }

    ucs_trace_data("tcp_ep %p: sent %zu bytes", ep, send_length);

    iface->outstanding -= send_length;
    ep->tx->offset     += send_length;

    if (ep->tx->offset == ep->tx->length) {
        ep->tx->offset = 0;
        ep->tx->length = 0;
    }

    return send_length > 0;
}

unsigned uct_tcp_ep_progress_tx(uct_tcp_ep_t *ep)
{
    unsigned                     count = 0;
    uct_pending_req_priv_queue_t *priv;

    ucs_trace_func("ep=%p", ep);

    if (ep->tx->length > 0) {
        count += uct_tcp_ep_send(ep);
    }

    uct_pending_queue_dispatch(priv, &ep->pending_q, uct_tcp_ep_can_send(ep));

    if (uct_tcp_ep_can_send(ep)) {
        ucs_assert(ucs_queue_is_empty(&ep->pending_q));
        uct_tcp_ep_mod_events(ep, 0, EPOLLOUT);
    }

    return count;
}

unsigned uct_tcp_ep_progress_rx(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    uct_tcp_am_hdr_t *hdr;
    ucs_status_t status;
    size_t recv_length;
    ssize_t remainder;

    ucs_trace_func("ep=%p", ep);

    /* Receive next chunk of data */
    recv_length = iface->config.buf_size - ep->rx->length;
    ucs_assertv(recv_length > 0, "ep=%p", ep);

    status = uct_tcp_recv(ep->fd, ep->rx->buf + ep->rx->length, &recv_length);
    if (status != UCS_OK) {
        if (status == UCS_ERR_CANCELED) {
            ucs_debug("tcp_ep %p: remote disconnected", ep);
            uct_tcp_ep_mod_events(ep, 0, EPOLLIN);
        }
        return 0;
    }

    ep->rx->length += recv_length;
    ucs_trace_data("tcp_ep %p: recvd %zu bytes", ep, recv_length);

    /* Parse received active messages */
    while ((remainder = ep->rx->length - ep->rx->offset) >= sizeof(*hdr)) {
        hdr = ep->rx->buf + ep->rx->offset;
        ucs_assert(hdr->length <= (iface->config.buf_size - sizeof(uct_tcp_am_hdr_t)));

        if (remainder < sizeof(*hdr) + hdr->length) {
            break;
        }

        /* Full message was received */
        ep->rx->offset += sizeof(*hdr) + hdr->length;

        if (hdr->am_id >= UCT_AM_ID_MAX) {
            ucs_error("invalid am id: %d", hdr->am_id);
            continue;
        }

        uct_iface_trace_am(&iface->super, UCT_AM_TRACE_TYPE_RECV, hdr->am_id,
                           hdr + 1, hdr->length, "RECV fd %d", ep->fd);
        uct_iface_invoke_am(&iface->super, hdr->am_id, hdr + 1,
                            hdr->length, 0);
    }

    /* Move the remaining data to the beginning of the buffer
     * TODO avoid extra copy on partial receive
     */
    ucs_assert(remainder >= 0);
    memmove(ep->rx->buf, ep->rx->buf + ep->rx->offset, remainder);
    ep->rx->offset = 0;
    ep->rx->length = remainder;

    return recv_length > 0;
}

ssize_t uct_tcp_ep_am_bcopy(uct_ep_h uct_ep, uint8_t am_id,
                            uct_pack_callback_t pack_cb, void *arg,
                            unsigned flags)
{
    uct_tcp_ep_t *ep = ucs_derived_of(uct_ep, uct_tcp_ep_t);
    uct_tcp_iface_t *iface = ucs_derived_of(uct_ep->iface, uct_tcp_iface_t);
    uct_tcp_am_hdr_t *hdr;
    size_t packed_length;

    UCT_CHECK_AM_ID(am_id);

    if (!uct_tcp_ep_can_send(ep)) {
        UCS_STATS_UPDATE_COUNTER(ep->super.stats, UCT_EP_STAT_NO_RES, 1);
        return UCS_ERR_NO_RESOURCE;
    }

    hdr            = ep->tx->buf;
    hdr->am_id     = am_id;
    hdr->length    = packed_length = pack_cb(hdr + 1, arg);
    ep->tx->length = sizeof(*hdr) + packed_length;

    UCT_CHECK_LENGTH(hdr->length, 0,
                     iface->config.buf_size - sizeof(uct_tcp_am_hdr_t),
                     "am_bcopy");
    UCT_TL_EP_STAT_OP(&ep->super, AM, BCOPY, hdr->length);
    uct_iface_trace_am(&iface->super, UCT_AM_TRACE_TYPE_SEND, hdr->am_id,
                       hdr + 1, hdr->length, "SEND fd %d", ep->fd);
    iface->outstanding += ep->tx->length;

    if (ucs_likely(ep->conn_state == UCT_TCP_EP_CONN_CONNECTED)) {
        uct_tcp_ep_send(ep);
        if (ep->tx->length > 0) {
            uct_tcp_ep_mod_events(ep, EPOLLOUT, 0);
        }
    } else if (ep->conn_state == UCT_TCP_EP_CONN_REFUSED) {
        ep->tx->length = 0;
        ep->tx->offset = 0;

        uct_set_ep_failed(&UCS_CLASS_NAME(uct_tcp_ep_t),
                          &ep->super.super, &iface->super.super,
                          UCS_ERR_UNREACHABLE);

        return UCS_ERR_UNREACHABLE;
    }

    return packed_length;
}

ucs_status_t uct_tcp_ep_pending_add(uct_ep_h tl_ep, uct_pending_req_t *req,
                                    unsigned flags)
{
    uct_tcp_ep_t *ep = ucs_derived_of(tl_ep, uct_tcp_ep_t);

    if (uct_tcp_ep_can_send(ep)) {
        return UCS_ERR_BUSY;
    }

    uct_pending_req_queue_push(&ep->pending_q, req);
    UCT_TL_EP_STAT_PEND(&ep->super);
    return UCS_OK;
}

void uct_tcp_ep_pending_purge(uct_ep_h tl_ep, uct_pending_purge_callback_t cb,
                              void *arg)
{
    uct_tcp_ep_t                 *ep = ucs_derived_of(tl_ep, uct_tcp_ep_t);
    uct_pending_req_priv_queue_t *priv;

    uct_pending_queue_purge(priv, &ep->pending_q, 1, cb, arg);
}

ucs_status_t uct_tcp_ep_flush(uct_ep_h tl_ep, unsigned flags,
                              uct_completion_t *comp)
{
    uct_tcp_ep_t *ep = ucs_derived_of(tl_ep, uct_tcp_ep_t);

    if (!uct_tcp_ep_can_send(ep)) {
        return UCS_ERR_NO_RESOURCE;
    }

    UCT_TL_EP_STAT_FLUSH(&ep->super);
    return UCS_OK;
}

