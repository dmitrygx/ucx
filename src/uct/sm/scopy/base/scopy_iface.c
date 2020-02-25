/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <ucs/arch/cpu.h>
#include <uct/sm/base/sm_iface.h>
#include <uct/sm/scopy/base/scopy_iface.h>


ucs_config_field_t uct_scopy_iface_config_table[] = {
    {"SM_", "", NULL,
     ucs_offsetof(uct_scopy_iface_config_t, super),
     UCS_CONFIG_TYPE_TABLE(uct_sm_iface_config_table)},

    {"MAX_IOV", "16",
     "Maximum IOV count that can contain user-defined payload in a single\n"
     "call to GET/PUT Zcopy operation",
     ucs_offsetof(uct_scopy_iface_config_t, max_iov), UCS_CONFIG_TYPE_ULONG},

    UCT_IFACE_MPOOL_CONFIG_FIELDS("TX_", -1, 8, "send",
                                  ucs_offsetof(uct_scopy_iface_config_t, tx_mpool), ""),

    {NULL}
};

void uct_scopy_iface_query(uct_iface_h tl_iface, uct_iface_attr_t *iface_attr)
{
    uct_scopy_iface_t *iface = ucs_derived_of(tl_iface, uct_scopy_iface_t);

    uct_base_iface_query(&iface->super.super, iface_attr);

    /* default values for all shared memory transports */
    iface_attr->cap.put.min_zcopy       = 0;
    iface_attr->cap.put.max_zcopy       = SIZE_MAX;
    iface_attr->cap.put.opt_zcopy_align = 1;
    iface_attr->cap.put.align_mtu       = iface_attr->cap.put.opt_zcopy_align;
    iface_attr->cap.put.max_iov         = iface->config.max_iov;

    iface_attr->cap.get.min_zcopy       = 0;
    iface_attr->cap.get.max_zcopy       = SIZE_MAX;
    iface_attr->cap.get.opt_zcopy_align = 1;
    iface_attr->cap.get.align_mtu       = iface_attr->cap.get.opt_zcopy_align;
    iface_attr->cap.get.max_iov         = iface->config.max_iov;

    iface_attr->device_addr_len        = uct_sm_iface_get_device_addr_len();
    iface_attr->ep_addr_len            = 0;
    iface_attr->max_conn_priv          = 0;
    iface_attr->cap.flags              = UCT_IFACE_FLAG_GET_ZCOPY |
                                         UCT_IFACE_FLAG_PUT_ZCOPY |
                                         UCT_IFACE_FLAG_PENDING   |
                                         UCT_IFACE_FLAG_CONNECT_TO_IFACE;
    iface_attr->latency.overhead       = 80e-9; /* 80 ns */
    iface_attr->latency.growth         = 0;
}

static ucs_mpool_ops_t uct_scopy_mpool_ops = {
    ucs_mpool_chunk_malloc,
    ucs_mpool_chunk_free,
    NULL,
    NULL
};

UCS_CLASS_INIT_FUNC(uct_scopy_iface_t, uct_scopy_iface_ops_t *ops, uct_md_h md,
                    uct_worker_h worker, const uct_iface_params_t *params,
                    const uct_iface_config_t *tl_config)
{
    uct_scopy_iface_config_t *config = ucs_derived_of(tl_config,
                                                      uct_scopy_iface_config_t);
    ucs_status_t status;

    UCS_CLASS_CALL_SUPER_INIT(uct_sm_iface_t, &ops->super, md,
                              worker, params, tl_config);

    self->tx_fn[UCT_SCOPY_TX_PUT_ZCOPY] = ops->put_zcopy;
    self->tx_fn[UCT_SCOPY_TX_GET_ZCOPY] = ops->get_zcopy;
    self->config.max_iov                = ucs_min(config->max_iov,
                                                  ucs_iov_get_max());

    ucs_queue_head_init(&self->tx_queue);

    status = ucs_mpool_init(&self->tx_mpool, 0,
                            sizeof(uct_scopy_tx_t) +
                            self->config.max_iov * sizeof(uct_iov_t),
                            0, UCS_SYS_CACHE_LINE_SIZE,
                            (config->tx_mpool.bufs_grow == 0) ?
                            8 : config->tx_mpool.bufs_grow,
                            config->tx_mpool.max_bufs,
                            &uct_scopy_mpool_ops, "uct_scopy_iface_tx_mp");
    return status;
}

static UCS_CLASS_CLEANUP_FUNC(uct_scopy_iface_t)
{
    self->super.super.super.ops.iface_progress_disable(&self->super.super.super,
                                                       UCT_PROGRESS_SEND |
                                                       UCT_PROGRESS_RECV);
    ucs_mpool_cleanup(&self->tx_mpool, 1);
}

UCS_CLASS_DEFINE(uct_scopy_iface_t, uct_sm_iface_t)

unsigned uct_scopy_iface_progress(uct_iface_h tl_iface)
{
    uct_scopy_iface_t *iface = ucs_derived_of(tl_iface, uct_scopy_iface_t);
    uct_scopy_tx_t *tx;
    ucs_status_t status;

    if (ucs_queue_is_empty(&iface->tx_queue)) {
        return 0;
    }

    tx = ucs_queue_pull_elem_non_empty(&iface->tx_queue, uct_scopy_tx_t,
                                       queue_elem);
    ucs_assert(tx != NULL);
    uct_scopy_trace_data(tx->remote_addr, tx->rkey, "%s [length %zu]",
                         uct_scopy_tx_op_str[tx->op], tx->total_length);
    status = iface->tx_fn[tx->op](tx);
    uct_invoke_completion(tx->comp, status);
    ucs_mpool_put_inline(tx);

    return 1;
}

ucs_status_t uct_scopy_iface_flush(uct_iface_h tl_iface, unsigned flags,
                                   uct_completion_t *comp)
{
    uct_scopy_iface_t *iface = ucs_derived_of(tl_iface, uct_scopy_iface_t);

    if (!ucs_queue_is_empty(&iface->tx_queue)) {
        UCT_TL_IFACE_STAT_FLUSH_WAIT(ucs_derived_of(tl_iface, uct_base_iface_t));
        return UCS_INPROGRESS;
    }

    UCT_TL_IFACE_STAT_FLUSH(ucs_derived_of(tl_iface, uct_base_iface_t));
    return UCS_OK;
}
