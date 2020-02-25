/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCT_SCOPY_EP_H
#define UCT_SCOPY_EP_H

#include <uct/base/uct_iface.h>
#include <uct/sm/base/sm_ep.h>
#include <uct/sm/scopy/base/scopy_iface.h>


extern const char* uct_scopy_tx_op_str[];


typedef struct uct_scopy_ep {
    uct_base_ep_t                   super;
    size_t                          tx_cnt;
} uct_scopy_ep_t;


UCS_CLASS_DECLARE(uct_scopy_ep_t, const uct_ep_params_t *);

ucs_status_t uct_scopy_ep_put_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov,
                                    size_t iovcnt, uint64_t remote_addr,
                                    uct_rkey_t rkey, uct_completion_t *comp);

ucs_status_t uct_scopy_ep_get_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov,
                                    size_t iovcnt, uint64_t remote_addr,
                                    uct_rkey_t rkey, uct_completion_t *comp);

ucs_status_t uct_scopy_ep_flush(uct_ep_h tl_ep, unsigned flags,
                                uct_completion_t *comp);

#endif
