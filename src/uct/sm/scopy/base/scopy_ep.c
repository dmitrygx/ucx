/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <uct/sm/scopy/base/scopy_ep.h>


const char* uct_scopy_tx_op_str[] = {
    [UCT_SCOPY_TX_PUT_ZCOPY] = "uct_scopy_ep_put_zcopy",
    [UCT_SCOPY_TX_GET_ZCOPY] = "uct_scopy_ep_get_zcopy"
};


UCS_CLASS_INIT_FUNC(uct_scopy_ep_t, const uct_ep_params_t *params)
{
    uct_scopy_iface_t *iface = ucs_derived_of(params->iface, uct_scopy_iface_t);

    UCS_CLASS_CALL_SUPER_INIT(uct_base_ep_t, &iface->super.super);

    self->tx_cnt = 0;

    return UCS_OK;
}

static UCS_CLASS_CLEANUP_FUNC(uct_scopy_ep_t)
{
    /* No op */
}

UCS_CLASS_DEFINE(uct_scopy_ep_t, uct_base_ep_t)

static UCS_F_ALWAYS_INLINE ucs_status_t
uct_scopy_ep_prepare_tx(uct_ep_h tl_ep, const uct_iov_t *iov,
                        size_t iovcnt, uint64_t remote_addr,
                        uct_rkey_t rkey, uct_completion_t *comp,
                        uct_scopy_tx_op_t tx_op)
{
    uct_scopy_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_scopy_iface_t);
    uct_scopy_ep_t *ep       = ucs_derived_of(tl_ep, uct_scopy_ep_t);
    uct_scopy_tx_t *tx;
    size_t iov_it;

    ucs_assert((tx_op == UCT_SCOPY_TX_PUT_ZCOPY) ||
               (tx_op == UCT_SCOPY_TX_GET_ZCOPY));

    UCT_CHECK_IOV_SIZE(iovcnt, iface->config.max_iov, uct_scopy_tx_op_str[tx_op]);

    tx = ucs_mpool_get_inline(&iface->tx_mpool);
    if (ucs_unlikely(tx == NULL)) {
        return UCS_ERR_NO_RESOURCE;
    }

    tx->tl_ep       = tl_ep;
    tx->comp        = comp;
    tx->rkey        = rkey;
    tx->remote_addr = remote_addr;
    tx->op          = tx_op;
    tx->iovcnt      = 0;
    for (iov_it = 0; iov_it < iovcnt; iov_it++) {
        if (iov[iov_it].length == 0) {
            continue;
        }

        tx->iov[tx->iovcnt] = iov[iov_it];
        tx->total_length   += tx->iov[tx->iovcnt].length;
        tx->iovcnt++;
    }

    if (tx_op == UCT_SCOPY_TX_PUT_ZCOPY) {
        UCT_TL_EP_STAT_OP(ucs_derived_of(tl_ep, uct_base_ep_t), PUT, ZCOPY,
                          tx->total_length);
    } else {
        UCT_TL_EP_STAT_OP(ucs_derived_of(tl_ep, uct_base_ep_t), GET, ZCOPY,
                          tx->total_length);
    }

    ucs_queue_push(&iface->tx_queue, &tx->queue_elem);
    ep->tx_cnt++;

    return UCS_OK;
}

ucs_status_t uct_scopy_ep_put_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov,
                                    size_t iovcnt, uint64_t remote_addr,
                                    uct_rkey_t rkey, uct_completion_t *comp)
{
    ucs_status_t status;

    status = uct_scopy_ep_prepare_tx(tl_ep, iov, iovcnt, remote_addr,
                                     rkey, comp, UCT_SCOPY_TX_PUT_ZCOPY);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    return UCS_INPROGRESS;
}

ucs_status_t uct_scopy_ep_get_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov,
                                    size_t iovcnt, uint64_t remote_addr,
                                    uct_rkey_t rkey, uct_completion_t *comp)
{
    ucs_status_t status;

    status = uct_scopy_ep_prepare_tx(tl_ep, iov, iovcnt, remote_addr,
                                     rkey, comp, UCT_SCOPY_TX_GET_ZCOPY);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    return UCS_INPROGRESS;
}

ucs_status_t uct_scopy_ep_flush(uct_ep_h tl_ep, unsigned flags,
                                uct_completion_t *comp)
{
    uct_scopy_ep_t *ep = ucs_derived_of(tl_ep, uct_scopy_ep_t);

    if (ep->tx_cnt != 0) {
        UCT_TL_EP_STAT_FLUSH_WAIT(ucs_derived_of(tl_ep, uct_base_ep_t));
        return UCS_INPROGRESS;
    }

    UCT_TL_EP_STAT_FLUSH(ucs_derived_of(tl_ep, uct_base_ep_t));
    return UCS_OK;
}
