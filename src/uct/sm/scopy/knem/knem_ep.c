/**
 * Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <knem_io.h>

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

#define UCT_KNEM_ZERO_LENGTH_POST(len) \
    if (0 == len) { \
        ucs_trace_data("Zero length request: skip it"); \
        return UCS_OK; \
    }

static inline ucs_status_t uct_knem_rma(uct_ep_h tl_ep, const uct_iov_t *iov,
                                        size_t iovcnt, uint64_t remote_addr,
                                        uct_knem_key_t *key, int write)
{
    uct_knem_iface_t *knem_iface = ucs_derived_of(tl_ep->iface, uct_knem_iface_t);
    int knem_fd                  = knem_iface->knem_md->knem_fd;
    size_t knem_iov_it           = 0;
    struct knem_cmd_inline_copy icopy;
    struct knem_cmd_param_iovec knem_iov[UCT_SM_MAX_IOV];
    int rc;
    size_t iov_it;

    for (iov_it = 0; iov_it < iovcnt; ++iov_it) {
        knem_iov[knem_iov_it].base = (uintptr_t)iov[iov_it].buffer;
        knem_iov[knem_iov_it].len  = uct_iov_get_length(iov + iov_it);
        /* Skip zero length buffers */
        if (knem_iov[knem_iov_it].len != 0) {
            ++knem_iov_it;
        }
    }

    UCT_KNEM_ZERO_LENGTH_POST(knem_iov_it);

    icopy.local_iovec_array = (uintptr_t)knem_iov;
    icopy.local_iovec_nr    = knem_iov_it;
    icopy.remote_cookie     = key->cookie;
    ucs_assert(remote_addr >= key->address);
    icopy.current_status    = 0;
    icopy.remote_offset     = remote_addr - key->address;
    /* if 0 then, READ from the remote region into my local segments
     * if 1 then, WRITE to the remote region from my local segment */
    icopy.write             = write;
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

ucs_status_t uct_knem_ep_put_zcopy(uct_scopy_tx_t *tx)
{
    return uct_knem_rma(tx->tl_ep, tx->iov, tx->iovcnt, tx->remote_addr,
                        (uct_knem_key_t*)tx->rkey, 1);
}

ucs_status_t uct_knem_ep_get_zcopy(uct_scopy_tx_t *tx)
{
    return uct_knem_rma(tx->tl_ep, tx->iov, tx->iovcnt, tx->remote_addr,
                        (uct_knem_key_t*)tx->rkey, 0);
}
