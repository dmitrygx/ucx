/**
 * Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCT_KNEM_EP_H
#define UCT_KNEM_EP_H

#include <uct/sm/scopy/base/scopy_ep.h>
#include <uct/sm/scopy/knem/knem_iface.h>


typedef struct uct_knem_ep {
    uct_scopy_ep_t super;
} uct_knem_ep_t;


UCS_CLASS_DECLARE_NEW_FUNC(uct_knem_ep_t, uct_ep_t, const uct_ep_params_t *);
UCS_CLASS_DECLARE_DELETE_FUNC(uct_knem_ep_t, uct_ep_t);

ucs_status_t uct_knem_ep_tx(uct_ep_h tl_ep, void *local_iov_ptr,
                            size_t local_iov_cnt, uint64_t remote_addr,
                            size_t length, uct_rkey_t rkey,
                            uct_scopy_tx_op_t tx_op);

#endif
