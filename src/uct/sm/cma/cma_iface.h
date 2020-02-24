/**
 * Copyright (c) UT-Battelle, LLC. 2014-2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCT_CMA_IFACE_H
#define UCT_CMA_IFACE_H

#include <uct/base/uct_iface.h>
#include <uct/sm/base/sm_iface.h>
#include <uct/sm/cma/cma_ep.h>


#define UCT_CMA_IFACE_ADDR_FLAG_PID_NS UCS_BIT(31) /* use PID NS in address */


typedef ssize_t (*uct_cma_iface_zcopy_fn_t)(pid_t, const struct iovec *,
                                            unsigned long, const struct iovec *,
                                            unsigned long, unsigned long);

typedef struct uct_cma_iface_config {
    uct_sm_iface_config_t         super;
    uct_iface_mpool_config_t      tx_mpool;
} uct_cma_iface_config_t;


typedef struct uct_cma_iface {
    uct_sm_iface_t                super;
    ucs_queue_head_t              tx_queue;
    ucs_mpool_t                   tx_mpool;
} uct_cma_iface_t;

typedef struct uct_cma_tx {
    ucs_queue_elem_t              queue_elem;
    uct_cma_ep_t                  *ep;
    uct_completion_t              *comp;
    uct_cma_iface_zcopy_fn_t      fn;
    struct iovec                  remote_iov;
    size_t                        local_iov_cnt;
    struct iovec                  local_iov[];
} uct_cma_tx_t;


ucs_status_t uct_cma_ep_flush(uct_ep_h tl_ep, unsigned flags,
                              uct_completion_t *comp);

#endif
