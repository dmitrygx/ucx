/**
* Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
* Copyright (C) Mellanox Technologies Ltd. 2001-2019.  ALL RIGHTS RESERVED.
* Copyright (C) ARM Ltd. 2016.  ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif
#include <sys/uio.h>

#include <ucs/debug/log.h>
#include <ucs/sys/iovec.h>
#include <uct/sm/cma/cma_iface.h>

static UCS_CLASS_INIT_FUNC(uct_cma_ep_t, const uct_ep_params_t *params)
{
    uct_cma_iface_t *iface = ucs_derived_of(params->iface, uct_cma_iface_t);

    UCT_CHECK_PARAM(params->field_mask & UCT_EP_PARAM_FIELD_IFACE_ADDR,
                    "UCT_EP_PARAM_FIELD_IFACE_ADDR and UCT_EP_PARAM_FIELD_DEV_ADDR are not defined");

    UCS_CLASS_CALL_SUPER_INIT(uct_base_ep_t, &iface->super.super);
    self->remote_pid = *(const pid_t*)params->iface_addr &
                       ~UCT_CMA_IFACE_ADDR_FLAG_PID_NS;
    self->tx_cnt     = 0;
    return UCS_OK;
}

static UCS_CLASS_CLEANUP_FUNC(uct_cma_ep_t)
{
    /* No op */
}

UCS_CLASS_DEFINE(uct_cma_ep_t, uct_base_ep_t)
UCS_CLASS_DEFINE_NEW_FUNC(uct_cma_ep_t, uct_ep_t, const uct_ep_params_t *);
UCS_CLASS_DEFINE_DELETE_FUNC(uct_cma_ep_t, uct_ep_t);


#define uct_cma_trace_data(_remote_addr, _rkey, _fmt, ...) \
     ucs_trace_data(_fmt " to %"PRIx64"(%+ld)", ## __VA_ARGS__, (_remote_addr), \
                    (_rkey))


static UCS_F_ALWAYS_INLINE
ucs_status_t uct_cma_ep_prepare_tx(uct_ep_h tl_ep, const uct_iov_t *iov,
                                   size_t iovcnt, uint64_t remote_addr,
                                   uct_completion_t *comp,
                                   uct_cma_iface_zcopy_fn_t fn,
                                   const char UCS_V_UNUSED *op_name)
{
    uct_cma_ep_t *ep       = ucs_derived_of(tl_ep, uct_cma_ep_t);
    uct_cma_iface_t *iface = ucs_derived_of(tl_ep->iface,
                                            uct_cma_iface_t);
    uct_cma_tx_t *tx;

    UCT_CHECK_IOV_SIZE(iovcnt, iface->super.config.max_iov, op_name);

    tx = ucs_mpool_get_inline(&iface->tx_mpool);
    if (ucs_unlikely(tx == NULL)) {
        return UCS_ERR_NO_RESOURCE;
    }

    tx->ep                  = ep;
    tx->comp                = comp;
    tx->fn                  = fn;
    tx->remote_iov.iov_base = (void*)(uintptr_t)remote_addr;
    tx->local_iov_cnt       = uct_iovec_fill_iov(tx->local_iov, iov, iovcnt,
                                                 &tx->remote_iov.iov_len);

    ep->tx_cnt++;

    ucs_queue_push(&iface->tx_queue, &tx->queue_elem);

    return UCS_OK;
}

ucs_status_t uct_cma_ep_put_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov,
                                  size_t iovcnt, uint64_t remote_addr,
                                  uct_rkey_t rkey, uct_completion_t *comp)
{
    ucs_status_t status = uct_cma_ep_prepare_tx(tl_ep, iov, iovcnt,
                                                remote_addr, comp,
                                                process_vm_writev,
                                                "uct_cma_ep_put_zcopy");
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    UCT_TL_EP_STAT_OP(ucs_derived_of(tl_ep, uct_base_ep_t), PUT, ZCOPY,
                      uct_iov_total_length(iov, iovcnt));
    uct_cma_trace_data(remote_addr, rkey, "PUT_ZCOPY [length %zu]",
                       uct_iov_total_length(iov, iovcnt));

    return UCS_INPROGRESS;
}

ucs_status_t uct_cma_ep_get_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov, size_t iovcnt,
                                  uint64_t remote_addr, uct_rkey_t rkey,
                                  uct_completion_t *comp)
{
    ucs_status_t status = uct_cma_ep_prepare_tx(tl_ep, iov, iovcnt,
                                                remote_addr, comp,
                                                process_vm_readv,
                                                "uct_cma_ep_get_zcopy");
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    UCT_TL_EP_STAT_OP(ucs_derived_of(tl_ep, uct_base_ep_t), GET, ZCOPY,
                      uct_iov_total_length(iov, iovcnt));
    uct_cma_trace_data(remote_addr, rkey, "GET_ZCOPY [length %zu]",
                       uct_iov_total_length(iov, iovcnt));

    return UCS_INPROGRESS;
}

ucs_status_t uct_cma_ep_flush(uct_ep_h tl_ep, unsigned flags,
                              uct_completion_t *comp)
{
    uct_cma_ep_t *ep = ucs_derived_of(tl_ep, uct_cma_ep_t);

    if (ep->tx_cnt != 0) {
        return UCS_INPROGRESS;
    }

    UCT_TL_EP_STAT_FLUSH(ucs_derived_of(tl_ep, uct_base_ep_t));
    return UCS_OK;
}
