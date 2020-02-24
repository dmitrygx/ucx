/**
 * Copyright (c) UT-Battelle, LLC. 2014-2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <uct/base/uct_md.h>
#include <ucs/sys/string.h>
#include <ucs/arch/cpu.h>

#include <uct/sm/cma/cma_md.h>
#include <uct/sm/cma/cma_iface.h>

typedef struct {
    pid_t                            id;
} ucs_cma_iface_base_device_addr_t;

typedef struct {
    ucs_cma_iface_base_device_addr_t super;
    ucs_sys_ns_t                     pid_ns;
} ucs_cma_iface_ext_device_addr_t;


static ucs_config_field_t uct_cma_iface_config_table[] = {
    {"SM_", "ALLOC=huge,thp,mmap,heap;BW=11145MBs", NULL,
     ucs_offsetof(uct_cma_iface_config_t, super),
     UCS_CONFIG_TYPE_TABLE(uct_sm_iface_config_table)},

    UCT_IFACE_MPOOL_CONFIG_FIELDS("TX_", -1, 8, "send",
                                  ucs_offsetof(uct_cma_iface_config_t, tx_mpool), ""),

    {NULL}
};

static ucs_status_t uct_cma_iface_get_address(uct_iface_t *tl_iface,
                                              uct_iface_addr_t *addr)
{
    ucs_cma_iface_ext_device_addr_t *iface_addr = (void*)addr;

    ucs_assert(!(getpid() & UCT_CMA_IFACE_ADDR_FLAG_PID_NS));

    iface_addr->super.id = getpid();
    if (!ucs_sys_ns_is_default(UCS_SYS_NS_TYPE_PID)) {
        iface_addr->super.id |= UCT_CMA_IFACE_ADDR_FLAG_PID_NS;
        iface_addr->pid_ns    = ucs_sys_get_ns(UCS_SYS_NS_TYPE_PID);
    }
    return UCS_OK;
}

static ucs_status_t uct_cma_iface_query(uct_iface_h tl_iface,
                                       uct_iface_attr_t *iface_attr)
{
    uct_cma_iface_t *iface = ucs_derived_of(tl_iface, uct_cma_iface_t);

    uct_base_iface_query(&iface->super.super, iface_attr);

    /* default values for all shared memory transports */
    iface_attr->cap.put.min_zcopy       = 0;
    iface_attr->cap.put.max_zcopy       = SIZE_MAX;
    iface_attr->cap.put.opt_zcopy_align = 1;
    iface_attr->cap.put.align_mtu       = iface_attr->cap.put.opt_zcopy_align;
    iface_attr->cap.put.max_iov         = iface->super.config.max_iov;

    iface_attr->cap.get.min_zcopy       = 0;
    iface_attr->cap.get.max_zcopy       = SIZE_MAX;
    iface_attr->cap.get.opt_zcopy_align = 1;
    iface_attr->cap.get.align_mtu       = iface_attr->cap.get.opt_zcopy_align;
    iface_attr->cap.get.max_iov         = iface->super.config.max_iov;

    iface_attr->iface_addr_len          = ucs_sys_ns_is_default(UCS_SYS_NS_TYPE_PID) ?
                                          sizeof(ucs_cma_iface_base_device_addr_t) :
                                          sizeof(ucs_cma_iface_ext_device_addr_t);
    iface_attr->device_addr_len         = uct_sm_iface_get_device_addr_len();
    iface_attr->ep_addr_len             = 0;
    iface_attr->max_conn_priv           = 0;
    iface_attr->cap.flags               = UCT_IFACE_FLAG_GET_ZCOPY |
                                          UCT_IFACE_FLAG_PUT_ZCOPY |
                                          UCT_IFACE_FLAG_PENDING   |
                                          UCT_IFACE_FLAG_CONNECT_TO_IFACE;
    iface_attr->latency.overhead        = 80e-9; /* 80 ns */
    iface_attr->latency.growth          = 0;
    iface_attr->bandwidth.dedicated     = iface->super.config.bandwidth;
    iface_attr->bandwidth.shared        = 0;
    iface_attr->overhead                = 0.4e-6; /* 0.4 us */

    return UCS_OK;
}

static int
uct_cma_iface_is_reachable(const uct_iface_h tl_iface,
                           const uct_device_addr_t *dev_addr,
                           const uct_iface_addr_t *tl_iface_addr)
{
    ucs_cma_iface_ext_device_addr_t *iface_addr = (void*)tl_iface_addr;

    if (!uct_sm_iface_is_reachable(tl_iface, dev_addr, tl_iface_addr)) {
        return 0;
    }

    if (iface_addr->super.id & UCT_CMA_IFACE_ADDR_FLAG_PID_NS) {
        return ucs_sys_get_ns(UCS_SYS_NS_TYPE_PID) == iface_addr->pid_ns;
    }

    return ucs_sys_ns_is_default(UCS_SYS_NS_TYPE_PID);
}

static UCS_F_ALWAYS_INLINE ucs_status_t uct_cma_do_zcopy(uct_cma_tx_t *tx)
{
    size_t local_iov_idx               = 0;
    size_t UCS_V_UNUSED remote_iov_idx = 0;
    ssize_t ret;

    do {
        ret = tx->fn(tx->ep->remote_pid, &tx->local_iov[local_iov_idx],
                     tx->local_iov_cnt - local_iov_idx,
                     &tx->remote_iov, 1, 0);
        if (ucs_unlikely(ret < 0)) {
            ucs_error("CMA function for pid=%d length=%zu returned %zd: %m",
                      tx->ep->remote_pid, tx->remote_iov.iov_len, ret);
            return UCS_ERR_IO_ERROR;
        }

        ucs_assert(ret <= tx->remote_iov.iov_len);
        ucs_iov_advance(tx->local_iov, tx->local_iov_cnt, &local_iov_idx, ret);
        ucs_iov_advance(&tx->remote_iov, 1, &remote_iov_idx, ret);
    } while (tx->remote_iov.iov_len != 0);

    return UCS_OK;
}

static unsigned uct_cma_iface_progress(uct_iface_h tl_iface)
{
    uct_cma_iface_t *iface = ucs_derived_of(tl_iface, uct_cma_iface_t);
    uct_cma_tx_t *tx;
    ucs_status_t status;

    if (ucs_queue_is_empty(&iface->tx_queue)) {
        return 0;
    }

    tx = ucs_queue_pull_elem_non_empty(&iface->tx_queue, uct_cma_tx_t,
                                       queue_elem);
    ucs_assert(tx->ep->tx_cnt != 0);
    status = uct_cma_do_zcopy(tx);
    tx->ep->tx_cnt--;
    uct_invoke_completion(tx->comp, status);
    ucs_mpool_put_inline(tx);

    return 1;
}

static ucs_status_t uct_cma_iface_flush(uct_iface_h tl_iface, unsigned flags,
                                        uct_completion_t *comp)
{
    uct_cma_iface_t *iface = ucs_derived_of(tl_iface, uct_cma_iface_t);

    if (ucs_unlikely(comp != NULL)) {
        return UCS_ERR_UNSUPPORTED;
    }

    if (!ucs_queue_is_empty(&iface->tx_queue)) {
        UCT_TL_IFACE_STAT_FLUSH_WAIT(&iface->super.super);
        return UCS_INPROGRESS;
    }

    UCT_TL_IFACE_STAT_FLUSH(&iface->super.super);
    return UCS_OK;
}

static UCS_CLASS_DECLARE_DELETE_FUNC(uct_cma_iface_t, uct_iface_t);

static uct_iface_ops_t uct_cma_iface_ops = {
    .ep_put_zcopy             = uct_cma_ep_put_zcopy,
    .ep_get_zcopy             = uct_cma_ep_get_zcopy,
    .ep_pending_add           = ucs_empty_function_return_busy,
    .ep_pending_purge         = ucs_empty_function,
    .ep_flush                 = uct_cma_ep_flush,
    .ep_fence                 = uct_sm_ep_fence,
    .ep_create                = UCS_CLASS_NEW_FUNC_NAME(uct_cma_ep_t),
    .ep_destroy               = UCS_CLASS_DELETE_FUNC_NAME(uct_cma_ep_t),
    .iface_flush              = uct_cma_iface_flush,
    .iface_fence              = uct_sm_iface_fence,
    .iface_progress_enable    = uct_base_iface_progress_enable,
    .iface_progress_disable   = uct_base_iface_progress_disable,
    .iface_progress           = uct_cma_iface_progress,
    .iface_close              = UCS_CLASS_DELETE_FUNC_NAME(uct_cma_iface_t),
    .iface_query              = uct_cma_iface_query,
    .iface_get_address        = uct_cma_iface_get_address,
    .iface_get_device_address = uct_sm_iface_get_device_address,
    .iface_is_reachable       = uct_cma_iface_is_reachable
};

static ucs_mpool_ops_t uct_cma_iface_mpool_ops = {
    ucs_mpool_chunk_malloc,
    ucs_mpool_chunk_free,
    NULL,
    NULL
};

static UCS_CLASS_INIT_FUNC(uct_cma_iface_t, uct_md_h md, uct_worker_h worker,
                           const uct_iface_params_t *params,
                           const uct_iface_config_t *tl_config)
{
    uct_cma_iface_config_t *config = ucs_derived_of(tl_config,
                                                    uct_cma_iface_config_t);
    ucs_status_t status;

    UCS_CLASS_CALL_SUPER_INIT(uct_sm_iface_t, &uct_cma_iface_ops, md,
                              worker, params, tl_config);

    ucs_queue_head_init(&self->tx_queue);

    status = ucs_mpool_init(&self->tx_mpool, 0,
                            sizeof(uct_cma_tx_t) +
                            sizeof(struct iovec) *
                            self->super.config.max_iov,
                            0, UCS_SYS_CACHE_LINE_SIZE,
                            (config->tx_mpool.bufs_grow == 0) ?
                            8 : config->tx_mpool.bufs_grow,
                            config->tx_mpool.max_bufs,
                            &uct_cma_iface_mpool_ops,
                            "uct_cma_iface_tx_buf_mp");
    if (status != UCS_OK) {
        return status;
    }

    return UCS_OK;
}

static UCS_CLASS_CLEANUP_FUNC(uct_cma_iface_t)
{
    uct_base_iface_progress_disable(&self->super.super.super,
                                    UCT_PROGRESS_SEND |
                                    UCT_PROGRESS_RECV);

    ucs_mpool_cleanup(&self->tx_mpool, 1);
}

UCS_CLASS_DEFINE(uct_cma_iface_t, uct_base_iface_t);

static UCS_CLASS_DEFINE_NEW_FUNC(uct_cma_iface_t, uct_iface_t, uct_md_h,
                                 uct_worker_h, const uct_iface_params_t*,
                                 const uct_iface_config_t *);
static UCS_CLASS_DEFINE_DELETE_FUNC(uct_cma_iface_t, uct_iface_t);

UCT_TL_DEFINE(&uct_cma_component, cma, uct_sm_base_query_tl_devices,
              uct_cma_iface_t, "CMA_", uct_cma_iface_config_table,
              uct_cma_iface_config_t);
