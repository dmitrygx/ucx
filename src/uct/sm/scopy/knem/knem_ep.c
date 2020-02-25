/**
 * Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <ucs/debug/log.h>
#include <uct/sm/base/sm_iface.h>
#include <uct/sm/scopy/knem/knem_md.h>
#include <uct/sm/scopy/knem/knem_ep.h>


static UCS_CLASS_INIT_FUNC(uct_knem_ep_t, const uct_ep_params_t *params)
{
    UCS_CLASS_CALL_SUPER_INIT(uct_scopy_ep_t, params);

    return UCS_OK;
}

static UCS_CLASS_CLEANUP_FUNC(uct_knem_ep_t)
{
    /* No op */
}

UCS_CLASS_DEFINE(uct_knem_ep_t, uct_scopy_ep_t)
UCS_CLASS_DEFINE_NEW_FUNC(uct_knem_ep_t, uct_ep_t, const uct_ep_params_t *);
UCS_CLASS_DEFINE_DELETE_FUNC(uct_knem_ep_t, uct_ep_t);

ucs_status_t uct_knem_ep_tx(uct_ep_h tl_ep, void *local_iov_ptr,
                            size_t local_iov_cnt, uint64_t remote_addr,
                            size_t length, uct_rkey_t rkey,
                            uct_scopy_tx_op_t tx_op)
{
    uct_knem_iface_t *knem_iface           = ucs_derived_of(tl_ep->iface,
                                                            uct_knem_iface_t);
    int knem_fd                            = knem_iface->knem_md->knem_fd;
    uct_knem_key_t *key                    = (uct_knem_key_t*)rkey;
    struct knem_cmd_param_iovec *local_iov =
        (struct knem_cmd_param_iovec*)local_iov_ptr;
    struct knem_cmd_inline_copy icopy;
    int rc;

    icopy.local_iovec_array = (uintptr_t)local_iov;
    icopy.local_iovec_nr    = local_iov_cnt;
    icopy.remote_cookie     = key->cookie;
    icopy.current_status    = 0;
    ucs_assert(remote_addr >= key->address);
    icopy.remote_offset     = remote_addr - key->address;
    /* if `false` then, READ from the remote region into my local segments
     * if `true` then, WRITE to the remote region from my local segment */
    icopy.write             = (tx_op == UCT_SCOPY_TX_PUT_ZCOPY);
    /* TBD: add check and support for KNEM_FLAG_DMA */
    icopy.flags             = 0;

    ucs_assert(knem_fd > -1);
    rc = ioctl(knem_fd, KNEM_CMD_INLINE_COPY, &icopy);
    if (ucs_unlikely((rc < 0) || (icopy.current_status != KNEM_STATUS_SUCCESS))) {
        ucs_error("KNEM inline copy failed, ioctl() return value - %d, "
                  "copy status - %d: %m", rc, icopy.current_status);
        return UCS_ERR_IO_ERROR;
    }

    return UCS_OK;
}
