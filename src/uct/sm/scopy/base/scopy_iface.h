/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCT_SCOPY_IFACE_H
#define UCT_SCOPY_IFACE_H

#include <uct/base/uct_iface.h>
#include <uct/sm/base/sm_iface.h>
#include <uct/sm/scopy/base/scopy_ep.h>


#define UCT_SCOPY_IFACE_SEG_SIZE 65536


extern ucs_config_field_t uct_scopy_iface_config_table[];


typedef enum uct_scopy_tx_op {
    UCT_SCOPY_TX_PUT_ZCOPY,
    UCT_SCOPY_TX_GET_ZCOPY,
    UCT_SCOPY_TX_LAST
} uct_scopy_tx_op_t;

typedef size_t
(*uct_scopy_iface_fill_iov_func_t)(void *tl_iov, const uct_iov_t *iov,
                                   size_t iovcnt, size_t *total_length_p);

typedef ucs_status_t
(*uct_scopy_ep_tx_func_t)(uct_ep_h tl_ep, void *local_iov_ptr,
                          size_t local_iov_cnt, uint64_t remote_addr,
                          size_t length, uct_rkey_t rkey,
                          uct_scopy_tx_op_t tx_op);

typedef struct uct_scopy_tx {
    ucs_queue_elem_t              queue_elem;
    uct_ep_h                      tl_ep;
    uct_scopy_tx_op_t             op;
    uct_completion_t              *comp;
    uint64_t                      remote_addr;
    uct_rkey_t                    rkey;
    size_t                        total_length;
    size_t                        consumed_length;
    size_t                        iov_idx_offset;
    size_t                        iovcnt;
    uct_iov_t                     iov[];
} uct_scopy_tx_t;

typedef struct uct_scopy_iface_config {
    uct_sm_iface_config_t         super;
    size_t                        max_iov;           /* Maximum supported IOVs */
    size_t                        seg_size;          /* Segment size that is used to perfrom
                                                      * data transfer for RMA operations */
    uct_iface_mpool_config_t      tx_mpool;          /* TX memory pool configuration */
} uct_scopy_iface_config_t;

typedef struct uct_scopy_iface {
    uct_sm_iface_t                  super;
    ucs_mpool_t                     tx_mpool;                 /* TX memory pool */
    ucs_queue_head_t                tx_queue;                 /* TX queue */
    uct_scopy_iface_fill_iov_func_t fill_iov;                 /* Fill IOV function */
    uct_scopy_ep_tx_func_t          tx;                       /* TX function */
    struct {
        size_t                      iov_elem_size;            /* Size of a single IOV element */
        size_t                      max_iov_cnt;              /* How much IOV elements can be
                                                               * allocated using ucs_alloca() */
    } tl_attr;
    struct {
        size_t                      max_iov;                  /* Maximum supported IOVs limited by
                                                               * user configuration and system
                                                               * settings */
        size_t                      seg_size;                 /* The maximal size of the segments
                                                               * that has to be used in GET/PUT
                                                               * Zcopy transfers */
    } config;
} uct_scopy_iface_t;

typedef struct uct_scopy_iface_ops {
    uct_iface_ops_t                 super;
    uct_scopy_iface_fill_iov_func_t fill_iov;
    uct_scopy_ep_tx_func_t          tx;
} uct_scopy_iface_ops_t;

typedef struct uct_scopy_iface_attr {
    size_t                        iov_elem_size;      /* Size of a single IOV element */
} uct_scopy_iface_attr_t;


#define uct_scopy_trace_data(_remote_addr, _rkey, _fmt, ...) \
     ucs_trace_data(_fmt " to %"PRIx64"(%+ld)", ## __VA_ARGS__, \
                    (_remote_addr), (_rkey))


UCS_CLASS_DECLARE(uct_scopy_iface_t, uct_scopy_iface_ops_t *ops,
                  uct_scopy_iface_attr_t *attr, uct_md_h md,
                  uct_worker_h worker, const uct_iface_params_t *params,
                  const uct_iface_config_t *tl_config);

void uct_scopy_iface_query(uct_iface_h tl_iface, uct_iface_attr_t *iface_attr);

unsigned uct_scopy_iface_progress(uct_iface_h tl_iface);

ucs_status_t uct_scopy_iface_flush(uct_iface_h tl_iface, unsigned flags,
                                   uct_completion_t *comp);

static UCS_F_ALWAYS_INLINE ucs_status_t
uct_scopy_do_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov, size_t iovcnt,
                   uint64_t remote_addr, uct_rkey_t rkey,
                   size_t consume_length, uct_scopy_tx_op_t tx_op)
{
    uct_scopy_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_scopy_iface_t);
    size_t iov_idx           = 0;
    size_t consumed_length   = 0;
    void *local_iov_ptr      = ucs_alloca(iface->tl_attr.max_iov_cnt *
                                          iface->tl_attr.iov_elem_size);
    size_t local_iov_cnt, cur_iov_cnt, length;
    ucs_status_t status;

    while ((iov_idx < iovcnt) && (consumed_length < consume_length)) {
        length        = consume_length - consumed_length;
        cur_iov_cnt   = ucs_min(iovcnt - iov_idx, UCT_SM_MAX_IOV);
        local_iov_cnt = iface->fill_iov(local_iov_ptr, &iov[iov_idx],
                                        cur_iov_cnt, &length);
        ucs_assert(length != 0);
        ucs_assert(local_iov_cnt <= cur_iov_cnt);

        iov_idx += cur_iov_cnt;
        ucs_assert(iov_idx <= iovcnt);

        status = iface->tx(tl_ep, local_iov_ptr, local_iov_cnt,
                           remote_addr + consumed_length,
                           length, rkey, tx_op);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }

        consumed_length += length;
    }

    return UCS_OK;
}

#endif
