/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "scopy_iface.h"
#include "scopy_ep.h"

#include <uct/base/uct_iov.inl>


const char* uct_scopy_tx_op_str[] = {
    [UCT_SCOPY_TX_PUT_ZCOPY] = "uct_scopy_ep_put_zcopy",
    [UCT_SCOPY_TX_GET_ZCOPY] = "uct_scopy_ep_get_zcopy"
};

UCS_CLASS_INIT_FUNC(uct_scopy_ep_t, const uct_ep_params_t *params)
{
    uct_scopy_iface_t *iface = ucs_derived_of(params->iface, uct_scopy_iface_t);

    UCS_CLASS_CALL_SUPER_INIT(uct_base_ep_t, &iface->super.super);

    ucs_arbiter_group_init(&self->arb_group);

    return UCS_OK;
}

static UCS_CLASS_CLEANUP_FUNC(uct_scopy_ep_t)
{
    ucs_arbiter_group_cleanup(&self->arb_group);
}

UCS_CLASS_DEFINE(uct_scopy_ep_t, uct_base_ep_t)

static UCS_F_ALWAYS_INLINE ucs_status_t
uct_scopy_ep_prepare_tx(uct_ep_h tl_ep, const uct_iov_t *iov,
                        size_t iov_cnt, uint64_t remote_addr,
                        uct_rkey_t rkey, uct_completion_t *comp,
                        uct_scopy_tx_op_t tx_op)
{
    uct_scopy_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_scopy_iface_t);
    uct_scopy_ep_t *ep       = ucs_derived_of(tl_ep, uct_scopy_ep_t);
    uct_scopy_tx_t *tx;
    size_t iov_it;

    ucs_assert((tx_op == UCT_SCOPY_TX_PUT_ZCOPY) ||
               (tx_op == UCT_SCOPY_TX_GET_ZCOPY));

    UCT_CHECK_IOV_SIZE(iov_cnt, iface->config.max_iov, uct_scopy_tx_op_str[tx_op]);

    tx = ucs_mpool_get_inline(&iface->tx_mpool);
    if (ucs_unlikely(tx == NULL)) {
        return UCS_ERR_NO_MEMORY;
    }

    tx->comp            = comp;
    tx->rkey            = rkey;
    tx->remote_addr     = remote_addr;
    tx->op              = tx_op;
    tx->consumed_length = 0;
    tx->iov_cnt         = 0;
    tx->total_length    = 0;
    ucs_iov_iter_init(&tx->iov_iter);
    for (iov_it = 0; iov_it < iov_cnt; iov_it++) {
        if (uct_iov_get_length(&iov[iov_it]) == 0) {
            /* Avoid zero-length IOV elements */
            continue;
        }

        tx->iov[tx->iov_cnt] = iov[iov_it];
        tx->total_length    += tx->iov[tx->iov_cnt].length;
        tx->iov_cnt++;
    }

    ucs_list_head_init(&tx->flush_comp_list);

    if (tx_op == UCT_SCOPY_TX_PUT_ZCOPY) {
        UCT_TL_EP_STAT_OP(ucs_derived_of(tl_ep, uct_base_ep_t), PUT, ZCOPY,
                          tx->total_length);
    } else {
        UCT_TL_EP_STAT_OP(ucs_derived_of(tl_ep, uct_base_ep_t), GET, ZCOPY,
                          tx->total_length);
    }

    if (tx->total_length == 0) {
        uct_scopy_trace_data(tx);
        ucs_mpool_put_inline(tx);
        return UCS_OK;
    }

    ep->last_tx = tx;
    iface->outstanding++;
    ucs_arbiter_elem_init(&tx->arb_elem);
    ucs_arbiter_group_push_elem(&ep->arb_group, &tx->arb_elem);
    ucs_arbiter_group_schedule(&iface->arbiter, &ep->arb_group);

    return UCS_INPROGRESS;
}

ucs_status_t uct_scopy_ep_put_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov,
                                    size_t iov_cnt, uint64_t remote_addr,
                                    uct_rkey_t rkey, uct_completion_t *comp)
{
    return uct_scopy_ep_prepare_tx(tl_ep, iov, iov_cnt, remote_addr,
                                   rkey, comp, UCT_SCOPY_TX_PUT_ZCOPY);
}

ucs_status_t uct_scopy_ep_get_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov,
                                    size_t iov_cnt, uint64_t remote_addr,
                                    uct_rkey_t rkey, uct_completion_t *comp)
{
    return uct_scopy_ep_prepare_tx(tl_ep, iov, iov_cnt, remote_addr,
                                   rkey, comp, UCT_SCOPY_TX_GET_ZCOPY);
}

static UCS_F_ALWAYS_INLINE ucs_status_t
uct_scopy_ep_comp_tx(uct_scopy_ep_t *ep, uct_scopy_tx_t *tx,
                     ucs_status_t status, size_t comp_size)
{
    uct_scopy_iface_t *iface = ucs_derived_of(ep->super.super.iface,
                                              uct_scopy_iface_t);
    uct_scopy_comp_t *flush_comp, *tmp_flush_comp;

    tx->consumed_length += comp_size;

    uct_scopy_trace_data(tx);

    if ((status != UCS_OK) || (tx->consumed_length == tx->total_length)) {
        if (tx->comp != NULL) {
            uct_invoke_completion(tx->comp, status);
        }

        ucs_list_for_each_safe(flush_comp, tmp_flush_comp,
                               &tx->flush_comp_list, list_elem) {
            uct_invoke_completion(flush_comp->comp, status);
            ucs_list_del(&flush_comp->list_elem);
            ucs_mpool_put_inline(flush_comp);
        }

        iface->outstanding--;

        ucs_mpool_put_inline(tx);
        return status;
    }

    return UCS_INPROGRESS;
}

ucs_arbiter_cb_result_t uct_scopy_ep_progress_tx(ucs_arbiter_t *arbiter,
                                                 ucs_arbiter_elem_t *elem,
                                                 void *arg)
{
    uct_scopy_iface_t *iface = ucs_container_of(arbiter, uct_scopy_iface_t,
                                                arbiter);
    uct_scopy_ep_t *ep       = ucs_container_of(ucs_arbiter_elem_group(elem),
                                                uct_scopy_ep_t, arb_group);
    uct_scopy_tx_t *tx       = ucs_container_of(elem, uct_scopy_tx_t, arb_elem);
    size_t seg_size          = ucs_min(iface->config.seg_size,
                                       tx->total_length - tx->consumed_length);
    unsigned *count          = (unsigned*)arg;
    ucs_status_t status;

    status = iface->tx(&ep->super.super, tx->iov, tx->iov_cnt,
                       &tx->iov_iter, &seg_size,
                       tx->remote_addr + tx->consumed_length,
                       tx->rkey, tx->op);
    status = uct_scopy_ep_comp_tx(ep, tx, status, seg_size);
    if (ucs_likely(!UCS_STATUS_IS_ERR(status))) {
        count++;
        if (status == UCS_INPROGRESS) {
            return UCS_ARBITER_CB_RESULT_RESCHED_GROUP;
        }
    }

    return UCS_ARBITER_CB_RESULT_REMOVE_ELEM;
}

ucs_status_t uct_scopy_ep_flush(uct_ep_h tl_ep, unsigned flags,
                                uct_completion_t *comp)
{
    uct_scopy_ep_t *ep       = ucs_derived_of(tl_ep, uct_scopy_ep_t);
    uct_scopy_iface_t *iface = ucs_derived_of(tl_ep->iface,
                                              uct_scopy_iface_t);
    uct_scopy_comp_t *flush_comp;

    if (!ucs_arbiter_group_is_empty(&ep->arb_group)) {

        if (comp != NULL) {
            flush_comp = ucs_mpool_get_inline(&iface->tx_mpool);
            if (ucs_unlikely(flush_comp == NULL)) {
                return UCS_ERR_NO_MEMORY;
            }

            flush_comp->comp = comp;
            ucs_list_add_tail(&ep->last_tx->flush_comp_list,
                              &flush_comp->list_elem);
        }

        UCT_TL_EP_STAT_FLUSH_WAIT(&ep->super);
        return UCS_INPROGRESS;
    }

    UCT_TL_EP_STAT_FLUSH(&ep->super);
    return UCS_OK;
}
