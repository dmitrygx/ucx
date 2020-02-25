/**
 * Copyright (c) UT-Battelle, LLC. 2014-2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <uct/base/uct_md.h>
#include <ucs/sys/string.h>
#include <uct/sm/scopy/knem/knem_iface.h>
#include <uct/sm/scopy/knem/knem_ep.h>


static ucs_config_field_t uct_knem_iface_config_table[] = {
    {"SCOPY_", "SM_BW=13862MBs", NULL,
     ucs_offsetof(uct_knem_iface_config_t, super),
     UCS_CONFIG_TYPE_TABLE(uct_scopy_iface_config_table)},

    {NULL}
};


/**
 * Fill KNEM IOVEC data structure by data provided in the array of UCT IOVs.
 * The function avoids copying IOVs with zero length.
 *
 * @param [out]    io_vec          Pointer to the resulted array of KNEM IOVECs
 * @param [in]     iov             Pointer to the array of UCT IOVs
 * @paran [in]     iovcnt          Number of the elemnts in the array of UCT IOVs
 * @param [in/out] total_length_p  The length of data that should be copied and,
 *                                 as an output value, indicates to a user how much
 *                                 data was copied
 *
 * @return Number of elements in io_vec[].
 */
size_t uct_knem_iovec_fill_iov(struct knem_cmd_param_iovec *io_vec,
                               const uct_iov_t *iov, size_t iovcnt,
                               size_t *total_length_p)
{
    size_t total_length = 0;
    size_t io_vec_it    = 0;
    size_t io_vec_len   = 0;
    size_t iov_it;

    for (iov_it = 0; (iov_it < iovcnt) && (*total_length_p != 0); ++iov_it) {
        io_vec_len = uct_iov_get_length(&iov[iov_it]);

        /* Avoid zero length elements in resulted iov_vec */
        if (io_vec_len != 0) {
            io_vec[io_vec_it].len  = ucs_min(io_vec_len,
                                             *total_length_p);
            io_vec[io_vec_it].base = (uintptr_t)iov[iov_it].buffer;

            ucs_assert(*total_length_p >= io_vec[io_vec_it].len);
            total_length               += io_vec[io_vec_it].len;
            *total_length_p            -= io_vec[io_vec_it].len;
            ++io_vec_it;
        }
    }

    *total_length_p = total_length;

    return io_vec_it;
}

static ucs_status_t uct_knem_iface_query(uct_iface_h tl_iface,
                                         uct_iface_attr_t *iface_attr)
{
    uct_knem_iface_t *iface = ucs_derived_of(tl_iface, uct_knem_iface_t);

    uct_scopy_iface_query(tl_iface, iface_attr);

    iface_attr->bandwidth.shared       = iface->super.super.config.bandwidth;
    iface_attr->bandwidth.dedicated    = 0;
    iface_attr->overhead               = 0.25e-6; /* 0.25 us */

    return UCS_OK;
}

static UCS_CLASS_DECLARE_DELETE_FUNC(uct_knem_iface_t, uct_iface_t);

static uct_scopy_iface_ops_t uct_knem_iface_ops = {
    .super = {
        .ep_put_zcopy             = uct_scopy_ep_put_zcopy,
        .ep_get_zcopy             = uct_scopy_ep_get_zcopy,
        .ep_pending_add           = (uct_ep_pending_add_func_t)ucs_empty_function_return_busy,
        .ep_pending_purge         = (uct_ep_pending_purge_func_t)ucs_empty_function,
        .ep_flush                 = uct_base_ep_flush,
        .ep_fence                 = uct_sm_ep_fence,
        .ep_create                = UCS_CLASS_NEW_FUNC_NAME(uct_knem_ep_t),
        .ep_destroy               = UCS_CLASS_DELETE_FUNC_NAME(uct_knem_ep_t),
        .iface_fence              = uct_sm_iface_fence,
        .iface_progress_enable    = uct_base_iface_progress_enable,
        .iface_progress_disable   = uct_base_iface_progress_disable,
        .iface_progress           = uct_scopy_iface_progress,
        .iface_flush              = uct_base_iface_flush,
        .iface_close              = UCS_CLASS_DELETE_FUNC_NAME(uct_knem_iface_t),
        .iface_query              = uct_knem_iface_query,
        .iface_get_device_address = uct_sm_iface_get_device_address,
        .iface_get_address        = (uct_iface_get_address_func_t)ucs_empty_function_return_success,
        .iface_is_reachable       = uct_sm_iface_is_reachable
    },
    .fill_iov                     = (uct_scopy_iface_fill_iov_func_t)uct_knem_iovec_fill_iov,
    .tx                           = uct_knem_ep_tx
};

static UCS_CLASS_INIT_FUNC(uct_knem_iface_t, uct_md_h md, uct_worker_h worker,
                           const uct_iface_params_t *params,
                           const uct_iface_config_t *tl_config)
{
    uct_scopy_iface_attr_t attr = {
        .iov_elem_size = sizeof(struct knem_cmd_param_iovec),
    };

    UCS_CLASS_CALL_SUPER_INIT(uct_scopy_iface_t, &uct_knem_iface_ops, &attr,
                              md, worker, params, tl_config);
    self->knem_md = (uct_knem_md_t*)md;

    return UCS_OK;
}

static UCS_CLASS_CLEANUP_FUNC(uct_knem_iface_t)
{
    /* No OP */
}

UCS_CLASS_DEFINE(uct_knem_iface_t, uct_scopy_iface_t);

static UCS_CLASS_DEFINE_NEW_FUNC(uct_knem_iface_t, uct_iface_t, uct_md_h,
                                 uct_worker_h, const uct_iface_params_t*,
                                 const uct_iface_config_t *);
static UCS_CLASS_DEFINE_DELETE_FUNC(uct_knem_iface_t, uct_iface_t);

UCT_TL_DEFINE(&uct_knem_component, knem, uct_sm_base_query_tl_devices,
              uct_knem_iface_t, "KNEM_", uct_knem_iface_config_table,
              uct_knem_iface_config_t);
