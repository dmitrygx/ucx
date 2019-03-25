/**
 * Copyright (C) Mellanox Technologies Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "tcp.h"

#include <ucs/async/async.h>


void uct_tcp_cm_change_conn_state(uct_tcp_ep_t *ep, uct_tcp_ep_ctx_type_t ctx_type,
                                  uct_tcp_ep_conn_state_t new_conn_state)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface, uct_tcp_iface_t);
    char str_local_addr[UCS_SOCKADDR_STRING_LEN], str_remote_addr[UCS_SOCKADDR_STRING_LEN];
    uct_tcp_ep_conn_state_t old_conn_state;

    old_conn_state = ep->conn_state;
    ep->conn_state = new_conn_state;

    if (!ucs_log_is_enabled(UCS_LOG_LEVEL_DEBUG)) {
        return;
    }

    ucs_sockaddr_str((const struct sockaddr*)&iface->config.ifaddr,
                     str_local_addr, UCS_SOCKADDR_STRING_LEN);
    ucs_sockaddr_str(ep->peer_addr.addr, str_remote_addr, UCS_SOCKADDR_STRING_LEN);

    ucs_debug("[%s -> %s] tcp_ep %p: %s ([%s]<->[%s])",
              uct_tcp_ep_cm_state[old_conn_state].name,
              uct_tcp_ep_cm_state[ep->conn_state].name,
              ep, uct_tcp_ep_cm_state[ep->conn_state].description,
              str_local_addr, str_remote_addr);
}

static ucs_status_t uct_tcp_cm_send_conn_req(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface         = ucs_derived_of(ep->super.super.iface,
                                                    uct_tcp_iface_t);
    uct_tcp_ep_conn_pkt_t conn_pkt = {
        .event                     = UCT_TCP_EP_CONN_REQ,
    };
    char str_remote_addr[UCS_SOCKADDR_STRING_LEN];
    size_t addr_size;
    ucs_status_t status;

    status = ucs_sockaddr_sizeof((const struct sockaddr*)&iface->config.ifaddr, &addr_size);
    if (status != UCS_OK) {
        return status;
    }

    memcpy(&conn_pkt.data.req.iface_addr, &iface->config.ifaddr, addr_size);

    status = uct_tcp_send_blocking(ep->fd, &conn_pkt, sizeof(conn_pkt));
    if (status != UCS_OK) {
        ucs_error("blocking send failed on fd %d: %m", ep->fd);
        return status;
    } else {
        ucs_debug("tcp_ep %p: conection request sent to %s",
                  ep, ucs_sockaddr_str(ep->peer_addr.addr, str_remote_addr,
                                       UCS_SOCKADDR_STRING_LEN));
        return UCS_OK;
    }
}

static ucs_status_t uct_tcp_cm_recv_conn_req(uct_tcp_ep_t *ep,
                                             struct sockaddr *peer_addr)
{
    char str_remote_addr[UCS_SOCKADDR_STRING_LEN];
    uct_tcp_ep_conn_pkt_t conn_pkt;
    ucs_status_t status;

    status = uct_tcp_recv_blocking(ep->fd, &conn_pkt, sizeof(conn_pkt));
    if (status == UCS_OK) {
        ucs_assertv(conn_pkt.event == UCT_TCP_EP_CONN_REQ, "ep=%p", ep);
    } else {
        ucs_debug("blocking recv failed on fd %d: %m", ep->fd);
        return status;
    }

    *peer_addr = conn_pkt.data.req.iface_addr;
    ucs_debug("tcp_ep %p: received the connection request from %s",
              ep, ucs_sockaddr_str(peer_addr, str_remote_addr,
                                   UCS_SOCKADDR_STRING_LEN));
    return UCS_OK;
}

static ucs_status_t uct_tcp_cm_send_conn_ack(uct_tcp_ep_t *ep)
{
    uct_tcp_ep_conn_pkt_t conn_pkt = {
        .event                     = UCT_TCP_EP_CONN_ACK,
    };
    char str_remote_addr[UCS_SOCKADDR_STRING_LEN];
    ucs_status_t status;

    status = uct_tcp_send_blocking(ep->fd, &conn_pkt, sizeof(conn_pkt));
    if (status != UCS_OK) {
        ucs_error("blocking send failed on fd %d: %m", ep->fd);
        return status;
    } else {
        ucs_debug("tcp_ep %p: conection ack sent to %s",
                  ep, ucs_sockaddr_str(ep->peer_addr.addr, str_remote_addr,
                                       UCS_SOCKADDR_STRING_LEN));
        return UCS_OK;
    }
}

unsigned uct_tcp_cm_conn_progress(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    char str_remote_addr[UCS_SOCKADDR_STRING_LEN];
    ucs_status_t status;

    status = ucs_socket_connect_nb_get_status(ep->fd);
    if (status == UCS_INPROGRESS) {
        return 0;
    } else if (status != UCS_OK) {
        goto err;
    }

    if (uct_tcp_cm_send_conn_req(ep) != UCS_OK) {
        ucs_debug("tcp_ep %p: unable to send connection request to %s",
                  ep, ucs_sockaddr_str(ep->peer_addr.addr, str_remote_addr,
                                       UCS_SOCKADDR_STRING_LEN));
        return 0;
    }

    uct_tcp_cm_change_conn_state(ep, UCT_TCP_EP_CTX_TYPE_TX,
                                 UCT_TCP_EP_CONN_WAIT_ACK);
    uct_tcp_ep_mod_events(ep, EPOLLIN, EPOLLOUT);

    ucs_assertv((ep->tx.length == 0) && (ep->tx.offset == 0) &&
                (ep->tx.buf == NULL), "ep=%p", ep);
    return 0;

err:
    iface->outstanding--;
    uct_tcp_ep_set_failed(ep, UCT_TCP_EP_CTX_TYPE_TX);
    return 0;
}

unsigned uct_tcp_cm_conn_ack_rx_progress(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    uct_tcp_ep_conn_pkt_t conn_pkt;

    if (uct_tcp_recv_blocking(ep->fd, &conn_pkt, sizeof(conn_pkt)) != UCS_OK) {
        uct_tcp_ep_mod_events(ep, 0, EPOLLIN);
        return 0;
    }

    ucs_assertv(conn_pkt.event == UCT_TCP_EP_CONN_ACK, "ep=%p", ep);

    iface->outstanding--;

    uct_tcp_cm_change_conn_state(ep, UCT_TCP_EP_CTX_TYPE_TX,
                                 UCT_TCP_EP_CONN_CONNECTED);
    uct_tcp_ep_mod_events(ep, EPOLLOUT, EPOLLIN);

    ucs_assertv(ep->tx.buf == NULL, "ep=%p", ep);

    /* Progress possibly pending TX operations */
    return uct_tcp_ep_progress(ep, UCT_TCP_EP_CTX_TYPE_TX);
}

unsigned uct_tcp_cm_conn_req_rx_progress(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    struct sockaddr peer_addr;
    ucs_status_t status;

    status = uct_tcp_cm_recv_conn_req(ep, &peer_addr);
    if (status != UCS_OK) {
        goto err;
    }

    uct_tcp_ep_addr_cleanup(&ep->peer_addr);

    status = uct_tcp_ep_addr_init(&ep->peer_addr, &peer_addr);
    if (status != UCS_OK) {
        goto err;
    }

    status = uct_tcp_cm_send_conn_ack(ep);
    if (status != UCS_OK) {
        goto err;
    }

    iface->outstanding--;

    uct_tcp_cm_change_conn_state(ep, UCT_TCP_EP_CTX_TYPE_RX,
                                 UCT_TCP_EP_CONN_CONNECTED);

    UCS_ASYNC_BLOCK(iface->super.worker->async);
    ucs_list_add_tail(&iface->ep_list, &ep->list);
    UCS_ASYNC_UNBLOCK(iface->super.worker->async);

    return 0;

 err:
    uct_tcp_ep_mod_events(ep, 0, EPOLLIN);
    uct_tcp_ep_destroy(&ep->super.super);
    return 0;
}

ucs_status_t uct_tcp_cm_conn_start(uct_tcp_ep_t *ep)
{
    uct_tcp_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                            uct_tcp_iface_t);
    char str_remote_addr[UCS_SOCKADDR_STRING_LEN];
    uct_tcp_ep_conn_state_t new_conn_state;
    uint32_t req_events;
    ucs_status_t status;

    status = ucs_socket_connect(ep->fd, ep->peer_addr.addr);
    if (status == UCS_INPROGRESS) {
        iface->outstanding++;

        new_conn_state  = UCT_TCP_EP_CONN_CONNECTING;
        req_events      = EPOLLOUT;
        status          = UCS_OK;
    } else if (status == UCS_OK) {
        status = uct_tcp_cm_send_conn_req(ep);
        if (status != UCS_OK) {
            ucs_error("tcp_ep %p: failed to initiate the connection with the peer (%s)",
                      ep, ucs_sockaddr_str(ep->peer_addr.addr, str_remote_addr,
                                           UCS_SOCKADDR_STRING_LEN));
            return status;
        }

        new_conn_state  = UCT_TCP_EP_CONN_WAIT_ACK;
        req_events      = EPOLLIN;
    } else {
        new_conn_state  = UCT_TCP_EP_CONN_CLOSED;
        req_events      = 0;
    }

    uct_tcp_cm_change_conn_state(ep, UCT_TCP_EP_CTX_TYPE_TX,
                                 new_conn_state);
    uct_tcp_ep_mod_events(ep, req_events, 0);
    return status;
}

ucs_status_t uct_tcp_cm_handle_incoming_conn(uct_tcp_iface_t *iface,
                                             const struct sockaddr *peer_addr, int fd)
{
    char str_local_addr[UCS_SOCKADDR_STRING_LEN], str_remote_addr[UCS_SOCKADDR_STRING_LEN];
    ucs_status_t status;
    uct_tcp_ep_t *ep;

    status = uct_tcp_ep_init(iface, fd, peer_addr, &ep);
    if (status != UCS_OK) {
        return status;
    }

    iface->outstanding++;

    uct_tcp_cm_change_conn_state(ep, UCT_TCP_EP_CTX_TYPE_RX,
                                 UCT_TCP_EP_CONN_ACCEPTING);
    uct_tcp_ep_mod_events(ep, EPOLLIN, 0);

    ucs_debug("tcp_iface %p: accepted connection from %s on %s to tcp_ep %p (fd %d)", iface,
              ucs_sockaddr_str(peer_addr, str_remote_addr, UCS_SOCKADDR_STRING_LEN),
              ucs_sockaddr_str((const struct sockaddr*)&iface->config.ifaddr,
                               str_local_addr, UCS_SOCKADDR_STRING_LEN), ep, fd);
    return UCS_OK;
}
