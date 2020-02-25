/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCT_SCOPY_IFACE_H
#define UCT_SCOPY_IFACE_H

#include <uct/base/uct_iface.h>
#include <uct/sm/base/sm_iface.h>
#include <uct/sm/scopy/base/scopy_ep.h>


extern ucs_config_field_t uct_scopy_iface_config_table[];


typedef enum uct_scopy_tx_op {
    UCT_SCOPY_TX_PUT_ZCOPY,
    UCT_SCOPY_TX_GET_ZCOPY,
    UCT_SCOPY_TX_LAST
} uct_scopy_tx_op_t;

typedef struct uct_scopy_tx {
    ucs_queue_elem_t              queue_elem;
    uct_ep_h                      tl_ep;
    uct_scopy_tx_op_t             op;
    uct_completion_t              *comp;
    uint64_t                      remote_addr;
    uct_rkey_t                    rkey;
    size_t                        total_length;
    size_t                        iovcnt;
    uct_iov_t                     iov[];
} uct_scopy_tx_t;

typedef ucs_status_t (*uct_scopy_ep_tx_fn_t)(uct_scopy_tx_t *);

typedef struct uct_scopy_iface_config {
    uct_sm_iface_config_t         super;
    size_t                        max_iov;           /* Maximum supported IOVs */
    uct_iface_mpool_config_t      tx_mpool;          /* TX memory pool configuration */
} uct_scopy_iface_config_t;

typedef struct uct_scopy_iface {
    uct_sm_iface_t                super;
    ucs_mpool_t                   tx_mpool;                 /* TX memory pool */
    ucs_queue_head_t              tx_queue;                 /* TX queue */
    uct_scopy_ep_tx_fn_t          tx_fn[UCT_SCOPY_TX_LAST]; /* Cached TX operations */
    struct {
        size_t                    max_iov;                  /* Maximum supported IOVs limited by
                                                             * user configuration and system
                                                             * settings */
    } config;
} uct_scopy_iface_t;

typedef struct uct_scopy_iface_ops {
    uct_iface_ops_t               super;
    uct_scopy_ep_tx_fn_t          put_zcopy;
    uct_scopy_ep_tx_fn_t          get_zcopy;
} uct_scopy_iface_ops_t;


#define uct_scopy_trace_data(_remote_addr, _rkey, _fmt, ...) \
     ucs_trace_data(_fmt " to %"PRIx64"(%+ld)", ## __VA_ARGS__, \
                    (_remote_addr), (_rkey))


UCS_CLASS_DECLARE(uct_scopy_iface_t, uct_scopy_iface_ops_t*, uct_md_h, uct_worker_h,
                  const uct_iface_params_t*, const uct_iface_config_t*);

void uct_scopy_iface_query(uct_iface_h tl_iface, uct_iface_attr_t *iface_attr);

unsigned uct_scopy_iface_progress(uct_iface_h tl_iface);

ucs_status_t uct_scopy_iface_flush(uct_iface_h tl_iface, unsigned flags,
                                   uct_completion_t *comp);

#endif
