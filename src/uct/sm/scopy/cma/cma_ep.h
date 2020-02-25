/**
* Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
* Copyright (C) Mellanox Technologies Ltd. 2001-2019.  ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/

#ifndef UCT_CMA_EP_H
#define UCT_CMA_EP_H

#include <uct/base/uct_log.h>
#include <uct/sm/scopy/base/scopy_ep.h>
#include <uct/sm/scopy/cma/cma_iface.h>

typedef struct uct_cma_ep {
    uct_scopy_ep_t super;
    pid_t          remote_pid;
} uct_cma_ep_t;


UCS_CLASS_DECLARE_NEW_FUNC(uct_cma_ep_t, uct_ep_t, const uct_ep_params_t *);
UCS_CLASS_DECLARE_DELETE_FUNC(uct_cma_ep_t, uct_ep_t);

ucs_status_t uct_cma_ep_put_zcopy(uct_scopy_tx_t *tx);
ucs_status_t uct_cma_ep_get_zcopy(uct_scopy_tx_t *tx);

#endif
