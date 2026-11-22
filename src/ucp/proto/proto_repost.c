/**
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2026. ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "proto_repost.h"
#include "proto_debug.h"
#include "proto_perf.h"
#include "proto_common.inl"

#include <ucp/core/ucp_ep.inl>
#include <ucp/core/ucp_rkey.inl>
#include <ucp/wireup/wireup_ep.h>

#include <string.h>


/* Operations outstanding purge reports.
 * The lane must support all of them since we have a single protocol
 * for reposting. */
#define UCP_PROTO_REPOST_TL_CAP_FLAGS \
    (UCT_IFACE_FLAG_AM_SHORT  | UCT_IFACE_FLAG_AM_BCOPY  | \
     UCT_IFACE_FLAG_PUT_SHORT | UCT_IFACE_FLAG_PUT_BCOPY | \
     UCT_IFACE_FLAG_PUT_ZCOPY | UCT_IFACE_FLAG_GET_BCOPY | \
     UCT_IFACE_FLAG_GET_ZCOPY)


typedef struct {
    ucp_lane_index_t lane;
} ucp_proto_repost_priv_t;


typedef struct {
    const void *buffer;
    size_t     length;
} ucp_proto_repost_pack_t;


static size_t ucp_proto_repost_pack(void *dest, void *arg)
{
    ucp_proto_repost_pack_t *pack = arg;

    if (pack->length > 0) {
        memcpy(dest, pack->buffer, pack->length);
    }

    return pack->length;
}

/* The receiver gets the same data as from AM short: header, then payload */
static size_t ucp_proto_repost_pack_am_short(void *dest, void *arg)
{
    const uct_ep_op_info_t *info = arg;
    size_t length                = info->am.payload.data.length;

    memcpy(dest, &info->am.header.value, sizeof(info->am.header.value));
    if (length > 0) {
        memcpy(UCS_PTR_BYTE_OFFSET(dest, sizeof(info->am.header.value)),
               info->am.payload.data.buffer, length);
    }

    return sizeof(info->am.header.value) + length;
}

static void ucp_proto_repost_probe(const ucp_proto_init_params_t *params)
{
    ucp_proto_perf_factors_t perf_factors = UCP_PROTO_PERF_FACTORS_INITIALIZER;
    ucp_lane_index_t lane                 = UCP_NULL_LANE;
    ucp_lane_index_t num_lanes;
    ucp_proto_repost_priv_t rpriv;
    ucp_proto_perf_t *perf;
    ucs_status_t status;

    UCS_STATIC_ASSERT(UCP_OP_ID_REPOST < UCP_PROTO_SELECT_OP_FLAGS_BASE);

    if (params->select_param->op_id_flags != UCP_OP_ID_REPOST) {
        return;
    }

    num_lanes = ucp_proto_common_find_lanes(params, 0, UCP_LANE_TYPE_LAST,
                                            UCP_PROTO_REPOST_TL_CAP_FLAGS, 0,
                                            1, 0, NULL, &lane);
    rpriv.lane = (num_lanes == 0) ? UCP_NULL_LANE : lane;

    status = ucp_proto_perf_create("repost", &perf);
    if (status != UCS_OK) {
        return;
    }

    /* Finite cost, so this instance wins over the reconfig fallback. */
    perf_factors[UCP_PROTO_PERF_FACTOR_LOCAL_TL] = UCS_LINEAR_FUNC_ZERO;
    status = ucp_proto_perf_add_funcs(
            perf, 0, SIZE_MAX, perf_factors,
            ucp_proto_perf_node_new_data("repost", ""), NULL);
    if (status != UCS_OK) {
        ucp_proto_perf_destroy(perf);
        return;
    }

    ucp_proto_select_add_proto(params, 0, 0, perf, &rpriv, sizeof(rpriv));
}

static ucs_status_t
ucp_proto_repost_copy_data(ucp_request_t *req, void **buffer_p, size_t length)
{
    void *copy;

    if (length == 0) {
        return UCS_OK;
    }

    if (*buffer_p == NULL) {
        return UCS_ERR_INVALID_PARAM;
    }

    copy = ucs_malloc(length, "ucp_proto_repost_data");
    if (copy == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    memcpy(copy, *buffer_p, length);
    *buffer_p                = copy;
    req->send.repost.buffer  = copy;
    return UCS_OK;
}

static ucs_status_t ucp_proto_repost_copy_iov(ucp_request_t *req)
{
    uct_ep_op_info_t *info = req->send.repost.info;
    size_t iovcnt          = info->rma.payload.zcopy.iovcnt;
    const uct_iov_t *iov   = info->rma.payload.zcopy.iov;
    uct_iov_t *copy;

    if (iovcnt == 0) {
        return UCS_OK;
    }

    if (iov == NULL) {
        return UCS_ERR_INVALID_PARAM;
    }

    copy = ucs_malloc(iovcnt * sizeof(*copy), "ucp_proto_repost_iov");
    if (copy == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    memcpy(copy, iov, iovcnt * sizeof(*copy));
    info->rma.payload.zcopy.iov = copy;
    req->send.repost.buffer     = copy;
    return UCS_OK;
}


static void ucp_proto_repost_cleanup(ucp_request_t *req)
{
    ucs_free(req->send.repost.buffer);
    ucs_free(req->send.repost.info);
    req->send.repost.buffer = NULL;
    req->send.repost.info   = NULL;
}

static void
ucp_proto_repost_assert_fields(const uct_ep_op_info_t *info UCS_V_UNUSED)
{
    switch (info->operation) {
    case UCT_EP_OP_AM_SHORT:
        ucs_assert(ucs_test_all_flags(info->field_mask,
                                      UCT_EP_OP_INFO_FIELD_OPERATION |
                                      UCT_EP_OP_INFO_FIELD_AM));
        ucs_assert(ucs_test_all_flags(info->am.field_mask,
                                      UCT_EP_OP_INFO_AM_FIELD_AM_ID |
                                      UCT_EP_OP_INFO_AM_FIELD_HEADER_VALUE |
                                      UCT_EP_OP_INFO_AM_FIELD_PAYLOAD_DATA));
        break;
    case UCT_EP_OP_AM_BCOPY:
        ucs_assert(ucs_test_all_flags(info->field_mask,
                                      UCT_EP_OP_INFO_FIELD_OPERATION |
                                      UCT_EP_OP_INFO_FIELD_AM));
        ucs_assert(ucs_test_all_flags(info->am.field_mask,
                                      UCT_EP_OP_INFO_AM_FIELD_AM_ID |
                                      UCT_EP_OP_INFO_AM_FIELD_FLAGS |
                                      UCT_EP_OP_INFO_AM_FIELD_PAYLOAD_DATA));
        break;
    case UCT_EP_OP_PUT_SHORT:
    case UCT_EP_OP_PUT_BCOPY:
        ucs_assert(ucs_test_all_flags(info->field_mask,
                                      UCT_EP_OP_INFO_FIELD_OPERATION |
                                      UCT_EP_OP_INFO_FIELD_RMA));
        ucs_assert(ucs_test_all_flags(info->rma.field_mask,
                                      UCT_EP_OP_INFO_RMA_FIELD_REMOTE_ADDR |
                                      UCT_EP_OP_INFO_RMA_FIELD_RKEY |
                                      UCT_EP_OP_INFO_RMA_FIELD_PAYLOAD_DATA));
        break;
    case UCT_EP_OP_PUT_ZCOPY:
    case UCT_EP_OP_GET_ZCOPY:
        ucs_assert(ucs_test_all_flags(info->field_mask,
                                      UCT_EP_OP_INFO_FIELD_OPERATION |
                                      UCT_EP_OP_INFO_FIELD_RMA));
        ucs_assert(ucs_test_all_flags(info->rma.field_mask,
                                      UCT_EP_OP_INFO_RMA_FIELD_REMOTE_ADDR |
                                      UCT_EP_OP_INFO_RMA_FIELD_RKEY |
                                      UCT_EP_OP_INFO_RMA_FIELD_PAYLOAD_ZCOPY));
        break;
    case UCT_EP_OP_GET_BCOPY:
        ucs_assert(ucs_test_all_flags(info->field_mask,
                                      UCT_EP_OP_INFO_FIELD_OPERATION |
                                      UCT_EP_OP_INFO_FIELD_RMA));
        ucs_assert(ucs_test_all_flags(info->rma.field_mask,
                                      UCT_EP_OP_INFO_RMA_FIELD_REMOTE_ADDR |
                                      UCT_EP_OP_INFO_RMA_FIELD_RKEY |
                                      UCT_EP_OP_INFO_RMA_FIELD_PAYLOAD_UNPACK));
        break;
    default:
        break;
    }
}

/* Descriptor and short/bcopy bytes are valid only for the purge callback. */
static ucs_status_t
ucp_proto_repost_save(ucp_request_t *req, const uct_ep_op_info_t *op_info)
{
    uct_ep_op_info_t *info;
    ucs_status_t status;

    info = ucs_malloc(sizeof(*info), "ucp_proto_repost_info");
    if (info == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    *info                   = *op_info;
    req->send.repost.info   = info;
    req->send.repost.buffer = NULL;
    ucp_proto_repost_assert_fields(info);

    switch (info->operation) {
    case UCT_EP_OP_AM_SHORT:
    case UCT_EP_OP_AM_BCOPY:
        status = ucp_proto_repost_copy_data(req, &info->am.payload.data.buffer,
                                            info->am.payload.data.length);
        break;
    case UCT_EP_OP_PUT_SHORT:
    case UCT_EP_OP_PUT_BCOPY:
        status = ucp_proto_repost_copy_data(req, &info->rma.payload.data.buffer,
                                            info->rma.payload.data.length);
        break;
    case UCT_EP_OP_PUT_ZCOPY:
    case UCT_EP_OP_GET_ZCOPY:
        status = ucp_proto_repost_copy_iov(req);
        break;
    case UCT_EP_OP_GET_BCOPY:
        status = UCS_OK;
        break;
    default:
        status = UCS_ERR_UNSUPPORTED;
        break;
    }

    return status;
}


static void ucp_proto_repost_release(ucp_request_t *req, ucs_status_t status)
{
    ucp_proto_repost_cleanup(req);
    ucp_request_complete_send(req, status);
}

static uct_completion_t *ucp_proto_repost_origin_comp(const ucp_request_t *req)
{
    const uct_ep_op_info_t *info = req->send.repost.info;

    if ((info != NULL) && (info->field_mask & UCT_EP_OP_INFO_FIELD_COMP)) {
        return info->comp;
    }

    return NULL;
}

/* Protocol v2 stores this completion in the origin request. */
static ucp_request_t *ucp_proto_repost_origin_req(const ucp_request_t *req)
{
    uct_completion_t *origin = ucp_proto_repost_origin_comp(req);
    ucp_request_t *origin_req;

    if (origin == NULL) {
        return NULL;
    }

    ucs_assert(origin->func != ucp_ep_flush_mem_completion);
    origin_req = ucs_container_of(origin, ucp_request_t, send.state.uct_comp);
    ucs_assert(origin_req->flags & UCP_REQUEST_FLAG_PROTO_SEND);
    return origin_req;
}

static ucp_rkey_h ucp_proto_repost_origin_rkey(const ucp_request_t *origin)
{
    ucp_operation_id_t op_id;

    ucs_assert(origin->send.proto_config != NULL);

    op_id = ucp_proto_select_op_id(&origin->send.proto_config->select_param);
    /* A repost request stores the copied payload where rma.rkey sits. */
    ucs_assert(op_id != UCP_OP_ID_REPOST);
    if ((op_id >= UCP_OP_ID_RNDV_FIRST) && (op_id < UCP_OP_ID_RNDV_LAST)) {
        return origin->send.rndv.rkey;
    }

    return origin->send.rma.rkey;
}

static ucs_status_t
ucp_proto_repost_tl_rkey(ucp_request_t *req, ucp_lane_index_t lane,
                         uct_rkey_t *tl_rkey_p)
{
    const uct_ep_op_info_t *info = req->send.repost.info;
    ucp_request_t *origin;
    ucp_md_index_t dst_md_index;
    ucp_rkey_h rkey;
    unsigned rkey_index;

    if (ucp_proto_repost_origin_comp(req) == NULL) {
        *tl_rkey_p = info->rma.rkey;
        return UCS_OK;
    }

    origin = ucp_proto_repost_origin_req(req);
    if (origin == NULL) {
        return UCS_ERR_UNREACHABLE;
    }

    rkey = ucp_proto_repost_origin_rkey(origin);
    if (rkey == NULL) {
        return UCS_ERR_UNREACHABLE;
    }

    dst_md_index = ucp_ep_config(req->send.ep)->key.lanes[lane].dst_md_index;
    if ((dst_md_index == UCP_NULL_RESOURCE) ||
        !(rkey->md_map & UCS_BIT(dst_md_index))) {
        return UCS_ERR_UNREACHABLE;
    }

    rkey_index = ucs_bitmap2idx(rkey->md_map, dst_md_index);
    *tl_rkey_p = ucp_rkey_get_tl_rkey(rkey, rkey_index);
    return UCS_OK;
}

/* Origin buffer memory handle on the memory domain of @a lane */
static ucs_status_t
ucp_proto_repost_uct_memh(ucp_request_t *req, ucp_lane_index_t lane,
                          uct_mem_h *uct_memh_p)
{
    ucp_request_t *origin = ucp_proto_repost_origin_req(req);
    ucp_md_index_t md_index;
    ucp_mem_h memh;

    if ((origin == NULL) ||
        (origin->send.state.dt_iter.dt_class != UCP_DATATYPE_CONTIG)) {
        return UCS_ERR_UNREACHABLE;
    }

    memh     = origin->send.state.dt_iter.type.contig.memh;
    md_index = ucp_ep_md_index(req->send.ep, lane);
    if ((memh == NULL) || !(memh->md_map & UCS_BIT(md_index))) {
        return UCS_ERR_UNREACHABLE;
    }

    *uct_memh_p = ucp_datatype_iter_uct_memh(memh, md_index);
    return UCS_OK;
}

/* Copied zcopy IOV still holds the failed device memory handle.
 * ucp_proto_repost_lane_fits() found the origin memory handle on @a lane. */
static void
ucp_proto_repost_zcopy_memh(ucp_request_t *req, ucp_lane_index_t lane)
{
    uct_ep_op_info_t *op_info = req->send.repost.info;
    uct_iov_t *iov            = (uct_iov_t*)op_info->rma.payload.zcopy.iov;
    uct_mem_h uct_memh        = UCT_MEM_HANDLE_NULL;
    ucs_status_t UCS_V_UNUSED status;
    size_t i;

    status = ucp_proto_repost_uct_memh(req, lane, &uct_memh);
    ucs_assertv(status == UCS_OK, "req %p lane %d: %s", req, lane,
                ucs_status_string(status));

    for (i = 0; i < op_info->rma.payload.zcopy.iovcnt; ++i) {
        iov[i].memh = uct_memh;
    }
}

static size_t ucp_proto_repost_am_short_length(const uct_ep_op_info_t *info)
{
    return sizeof(info->am.header.value) + info->am.payload.data.length;
}

static int ucp_proto_repost_am_short_fits(ucp_ep_h ep, ucp_lane_index_t lane,
                                          const uct_ep_op_info_t *info)
{
    const uct_iface_attr_t *attr = ucp_ep_get_iface_attr(ep, lane);

    return (attr->cap.flags & UCT_IFACE_FLAG_AM_SHORT) &&
           (ucp_proto_repost_am_short_length(info) <= attr->cap.am.max_short);
}

static int ucp_proto_repost_zcopy_fits(ucp_request_t *req,
                                       ucp_lane_index_t lane, size_t min_zcopy,
                                       size_t max_zcopy, size_t max_iov)
{
    const uct_ep_op_info_t *info = req->send.repost.info;
    const uct_iov_t *iov         = info->rma.payload.zcopy.iov;
    size_t length                = 0;
    uct_mem_h uct_memh;
    size_t i;

    for (i = 0; i < info->rma.payload.zcopy.iovcnt; ++i) {
        length += iov[i].length * iov[i].count;
    }

    return (info->rma.payload.zcopy.iovcnt <= max_iov) &&
           (length >= min_zcopy) && (length <= max_zcopy) &&
           (ucp_proto_repost_uct_memh(req, lane, &uct_memh) == UCS_OK);
}

/* The failed lane may have taken more than @a lane: longer AM short with
 * device memory, longer bcopy with a larger segment, or memory and remote
 * keys of its own device. The transport checks some of these limits only with
 * parameter checks, and none for the length that an AM bcopy pack returns. */
static int ucp_proto_repost_lane_fits(ucp_request_t *req, ucp_lane_index_t lane)
{
    const uct_ep_op_info_t *info = req->send.repost.info;
    ucp_ep_h ep                  = req->send.ep;
    const uct_iface_attr_t *attr;
    uct_rkey_t tl_rkey;
    int fits;

    if ((lane >= ucp_ep_num_lanes(ep)) ||
        (ucp_ep_get_rsc_index(ep, lane) == UCP_NULL_RESOURCE) ||
        (ucp_ep_get_failed_lanes(ep) & UCS_BIT(lane)) ||
        (ucp_ep_get_lane(ep, lane) == NULL)) {
        return 0;
    }

    attr = ucp_ep_get_iface_attr(ep, lane);
    switch (info->operation) {
    case UCT_EP_OP_AM_SHORT:
        return ucp_proto_repost_am_short_fits(ep, lane, info) ||
               ((attr->cap.flags & UCT_IFACE_FLAG_AM_BCOPY) &&
                (ucp_proto_repost_am_short_length(info) <=
                 ucp_ep_get_max_bcopy(ep, lane)));
    case UCT_EP_OP_AM_BCOPY:
        return (attr->cap.flags & UCT_IFACE_FLAG_AM_BCOPY) &&
               (info->am.payload.data.length <= ucp_ep_get_max_bcopy(ep, lane));
    case UCT_EP_OP_PUT_SHORT:
        fits = (attr->cap.flags & UCT_IFACE_FLAG_PUT_SHORT) &&
               (info->rma.payload.data.length <= attr->cap.put.max_short);
        break;
    case UCT_EP_OP_PUT_BCOPY:
        fits = (attr->cap.flags & UCT_IFACE_FLAG_PUT_BCOPY) &&
               (info->rma.payload.data.length <= attr->cap.put.max_bcopy);
        break;
    case UCT_EP_OP_PUT_ZCOPY:
        fits = (attr->cap.flags & UCT_IFACE_FLAG_PUT_ZCOPY) &&
               ucp_proto_repost_zcopy_fits(req, lane, attr->cap.put.min_zcopy,
                                           attr->cap.put.max_zcopy,
                                           attr->cap.put.max_iov);
        break;
    case UCT_EP_OP_GET_BCOPY:
        fits = (attr->cap.flags & UCT_IFACE_FLAG_GET_BCOPY) &&
               (info->rma.payload.unpack.length <= attr->cap.get.max_bcopy);
        break;
    case UCT_EP_OP_GET_ZCOPY:
        fits = (attr->cap.flags & UCT_IFACE_FLAG_GET_ZCOPY) &&
               ucp_proto_repost_zcopy_fits(req, lane, attr->cap.get.min_zcopy,
                                           attr->cap.get.max_zcopy,
                                           attr->cap.get.max_iov);
        break;
    default:
        return 0;
    }

    return fits && (ucp_proto_repost_tl_rkey(req, lane, &tl_rkey) == UCS_OK);
}

/* The protocol lane, or another connected lane when it does not take the
 * operation */
static ucp_lane_index_t ucp_proto_repost_select_lane(ucp_request_t *req)
{
    const ucp_proto_repost_priv_t *rpriv = req->send.proto_config->priv;
    ucp_ep_h ep                          = req->send.ep;
    ucp_lane_index_t lane;

    if (ucp_proto_repost_lane_fits(req, rpriv->lane)) {
        return rpriv->lane;
    }

    /* Without an origin request, an RMA operation has only the remote key of
     * the failed lane, which another lane may not accept */
    if ((req->send.repost.info->field_mask & UCT_EP_OP_INFO_FIELD_RMA) &&
        (ucp_proto_repost_origin_comp(req) == NULL)) {
        return UCP_NULL_LANE;
    }

    for (lane = 0; lane < ucp_ep_num_lanes(ep); ++lane) {
        if (ucp_proto_repost_lane_fits(req, lane) &&
            !ucp_wireup_ep_test(ucp_ep_get_lane(ep, lane))) {
            ucp_trace_req(req, "repost on lane %u instead of %u",
                          (unsigned)lane, (unsigned)rpriv->lane);
            return lane;
        }
    }

    return UCP_NULL_LANE;
}


static void ucp_proto_repost_completion(uct_completion_t *self)
{
    ucp_request_t *req       = ucs_container_of(self, ucp_request_t,
                                                send.state.uct_comp);
    uct_completion_t *origin = ucp_proto_repost_origin_comp(req);

    if (req->send.repost.info != NULL) {
        req->send.repost.info->comp = NULL;
    }

    if (origin != NULL) {
        ucp_invoke_uct_completion(origin, self->status);
    }

    ucp_proto_repost_release(req, self->status);
}


/* An operation without a completion finished when it was posted, so no request
 * can send it again. Fail the endpoint, so that the error callback, a fetch
 * atomic which waits for its reply, and a flush see the loss. Schedule it,
 * since the repost runs inside a purge of the endpoint lanes, which failing
 * the endpoint would purge again. */
static void ucp_proto_repost_fail_ep(ucp_ep_h ep, const uct_ep_op_info_t *info,
                                     ucs_status_t status)
{
    /* A failed or a closed endpoint drops its operations anyway */
    if (ep->flags & (UCP_EP_FLAG_FAILED | UCP_EP_FLAG_CLOSED)) {
        return;
    }

    ucs_error("ep %p: failed to repost %s: %s, failing the endpoint", ep,
              uct_ep_operation_names[info->operation],
              ucs_status_string(status));
    ucp_ep_set_lanes_failed_schedule(ep, 0, UCS_ERR_ENDPOINT_TIMEOUT);
}

static void ucp_proto_repost_abort(ucp_request_t *req, ucs_status_t status)
{
    uct_completion_t *origin = ucp_proto_repost_origin_comp(req);

    /* The transport invokes the repost completion only after UCS_INPROGRESS,
     * so complete the purged operation here. */
    if (origin != NULL) {
        req->send.repost.info->comp = NULL;
        ucp_invoke_uct_completion(origin, status);
    } else if (UCS_STATUS_IS_ERR(status)) {
        ucp_proto_repost_fail_ep(req->send.ep, req->send.repost.info, status);
    }

    ucp_proto_repost_release(req, status);
}

static ucs_status_t ucp_proto_repost_progress(uct_pending_req_t *self)
{
    ucp_request_t *req           = ucs_container_of(self, ucp_request_t,
                                                    send.uct);
    const uct_ep_op_info_t *info = req->send.repost.info;
    uct_rkey_t tl_rkey           = UCT_INVALID_RKEY;
    ucp_proto_repost_pack_t pack;
    uct_completion_t *comp;
    ucp_lane_index_t lane;
    ucs_status_t status;
    uct_ep_h uct_ep;
    ssize_t packed;

    ucp_proto_repost_assert_fields(info);

    /* No lane takes it: an operation with a completion restarts its request,
     * and one without fails the endpoint */
    lane = ucp_proto_repost_select_lane(req);
    if (lane == UCP_NULL_LANE) {
        ucp_proto_repost_abort(req, UCS_ERR_UNREACHABLE);
        return UCS_OK;
    }

    uct_ep = ucp_ep_get_lane(req->send.ep, lane);
    comp   = (ucp_proto_repost_origin_comp(req) != NULL) ?
             &req->send.state.uct_comp : NULL;

    if (info->field_mask & UCT_EP_OP_INFO_FIELD_RMA) {
        /* ucp_proto_repost_lane_fits() found the remote key on this lane */
        status = ucp_proto_repost_tl_rkey(req, lane, &tl_rkey);
        ucs_assertv(status == UCS_OK, "req %p lane %d: %s", req, lane,
                    ucs_status_string(status));
    }

    switch (info->operation) {
    case UCT_EP_OP_AM_SHORT:
        /* As AM short or, with the same bytes for the receiver, as AM bcopy */
        if (ucp_proto_repost_am_short_fits(req->send.ep, lane, info)) {
            status = uct_ep_am_short(uct_ep, info->am.am_id,
                                     info->am.header.value,
                                     info->am.payload.data.buffer,
                                     info->am.payload.data.length);
        } else {
            packed = uct_ep_am_bcopy(uct_ep, info->am.am_id,
                                     ucp_proto_repost_pack_am_short,
                                     req->send.repost.info, 0);
            status = ucp_proto_bcopy_send_func_status(packed);
        }
        break;
    case UCT_EP_OP_AM_BCOPY:
        pack.buffer = info->am.payload.data.buffer;
        pack.length = info->am.payload.data.length;
        packed      = uct_ep_am_bcopy(uct_ep, info->am.am_id,
                                      ucp_proto_repost_pack, &pack,
                                      info->am.flags);
        status      = ucp_proto_bcopy_send_func_status(packed);
        break;
    case UCT_EP_OP_PUT_SHORT:
        status = uct_ep_put_short(uct_ep, info->rma.payload.data.buffer,
                                  info->rma.payload.data.length,
                                  info->rma.remote_addr, tl_rkey);
        break;
    case UCT_EP_OP_PUT_BCOPY:
        pack.buffer = info->rma.payload.data.buffer;
        pack.length = info->rma.payload.data.length;
        packed      = uct_ep_put_bcopy(uct_ep, ucp_proto_repost_pack, &pack,
                                       info->rma.remote_addr, tl_rkey);
        status      = ucp_proto_bcopy_send_func_status(packed);
        break;
    case UCT_EP_OP_PUT_ZCOPY:
        ucp_proto_repost_zcopy_memh(req, lane);
        status = uct_ep_put_zcopy(uct_ep, info->rma.payload.zcopy.iov,
                                  info->rma.payload.zcopy.iovcnt,
                                  info->rma.remote_addr, tl_rkey, comp);
        break;
    case UCT_EP_OP_GET_BCOPY:
        status = uct_ep_get_bcopy(uct_ep, info->rma.payload.unpack.unpack_cb,
                                  info->rma.payload.unpack.arg,
                                  info->rma.payload.unpack.length,
                                  info->rma.remote_addr, tl_rkey, comp);
        break;
    case UCT_EP_OP_GET_ZCOPY:
        ucp_proto_repost_zcopy_memh(req, lane);
        status = uct_ep_get_zcopy(uct_ep, info->rma.payload.zcopy.iov,
                                  info->rma.payload.zcopy.iovcnt,
                                  info->rma.remote_addr, tl_rkey, comp);
        break;
    default:
        ucp_proto_repost_abort(req, UCS_ERR_UNSUPPORTED);
        return UCS_OK;
    }

    if (status == UCS_ERR_NO_RESOURCE) {
        req->send.lane = lane;
        return UCS_ERR_NO_RESOURCE;
    }

    /* Transport invokes the request completion only after UCS_INPROGRESS. */
    if ((status == UCS_INPROGRESS) &&
        (ucp_proto_repost_origin_comp(req) != NULL)) {
        return UCS_OK;
    }

    ucp_proto_repost_abort(req, (status == UCS_INPROGRESS) ? UCS_OK : status);
    return UCS_OK;
}

/* Keep the saved operation. Restart posts this request on the new lane. */
static ucs_status_t ucp_proto_repost_reset(ucp_request_t *req)
{
    if (ucp_proto_repost_origin_comp(req) != NULL) {
        ucp_proto_completion_init(&req->send.state.uct_comp,
                                  ucp_proto_repost_completion);
    }

    return UCS_OK;
}

static const ucp_proto_config_t *ucp_proto_repost_config(ucp_ep_h ep);

/* A software discard of a lane aborts the reposts which wait there for
 * resources. They were not sent, so restart them when the endpoint is live and
 * its configuration changed. The origin request of an operation with a
 * completion restarts it instead. */
static void
ucp_proto_repost_pending_abort(ucp_request_t *req, ucs_status_t status)
{
    ucp_ep_h ep = req->send.ep;

    if ((ucp_proto_repost_origin_comp(req) == NULL) &&
        !(ep->flags & (UCP_EP_FLAG_FAILED | UCP_EP_FLAG_CLOSED)) &&
        (req->send.proto_config->ep_cfg_index != ep->cfg_index) &&
        (ucp_proto_repost_config(ep) != NULL)) {
        ucp_proto_request_restart(req);
        return;
    }

    ucp_proto_repost_abort(req, status);
}


ucp_proto_t ucp_repost_proto = {
    .name     = "repost",
    .desc     = "repost an undelivered operation",
    .flags    = UCP_PROTO_FLAG_INVALID,
    .dt_mask  = UCP_DT_MASK_ALL,
    .probe    = ucp_proto_repost_probe,
    .query    = ucp_proto_default_query,
    .progress = {ucp_proto_repost_progress},
    .abort    = ucp_proto_repost_pending_abort,
    .reset    = ucp_proto_repost_reset
};


static const ucp_proto_config_t *
ucp_proto_repost_config(ucp_ep_h ep)
{
    ucp_proto_select_param_t select_param;
    ucp_proto_select_elem_t *select_elem;
    const ucp_proto_threshold_elem_t *thresh;

    memset(&select_param, 0, sizeof(select_param));
    select_param.op_id_flags = UCP_OP_ID_REPOST;
    select_param.dt_class    = UCP_DATATYPE_CONTIG;
    select_param.mem_type    = UCS_MEMORY_TYPE_HOST;
    select_param.sg_count    = 1;

    select_elem = ucp_proto_select_lookup_slow(
            ep->worker, &ucp_ep_config(ep)->proto_select, 0, ep->cfg_index,
            UCP_WORKER_CFG_INDEX_NULL, &select_param);
    if (select_elem == NULL) {
        return NULL;
    }

    thresh = ucp_proto_thresholds_search_slow(select_elem->thresholds, 0);
    if ((thresh == NULL) ||
        (thresh->proto_config.proto != &ucp_repost_proto)) {
        return NULL;
    }

    return &thresh->proto_config;
}

ucs_status_t
ucp_proto_repost_submit(ucp_ep_h ep, const uct_ep_op_info_t *op_info)
{
    const ucp_proto_config_t *config;
    ucp_request_t *req;
    ucs_status_t status;

    /* A memory flush completion is not in the origin request. The caller
     * completes it; a later flush uses the reconfigured lanes. */
    if ((op_info->field_mask & UCT_EP_OP_INFO_FIELD_COMP) &&
        (op_info->comp != NULL) &&
        (op_info->comp->func == ucp_ep_flush_mem_completion)) {
        return UCS_ERR_CANCELED;
    }

    /* This completion is the live repost request. Saving op_info again would
     * replace the user completion with it. */
    if ((op_info->field_mask & UCT_EP_OP_INFO_FIELD_COMP) &&
        (op_info->comp != NULL) &&
        (op_info->comp->func == ucp_proto_repost_completion)) {
        req = ucs_container_of(op_info->comp, ucp_request_t,
                                              send.state.uct_comp);
        ucp_trace_req(req, "repost restart op %s",
                      uct_ep_operation_names[op_info->operation]);
        ucp_proto_request_restart(req);
        return UCS_OK;
    }

    config = ucp_proto_repost_config(ep);
    if (config == NULL) {
        status = UCS_ERR_UNREACHABLE;
        goto err;
    }

    req = ucp_request_get(ep->worker);
    if (req == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto err;
    }

    req->flags                     = UCP_REQUEST_FLAG_PROTO_SEND |
                                     UCP_REQUEST_FLAG_RELEASED;
    req->send.ep                   = ep;
    req->send.state.dt_iter.offset = 0;
    req->send.state.dt_iter.length = 0;
    req->send.repost.info          = NULL;
    req->send.repost.buffer        = NULL;
    ucp_proto_request_set_proto(req, config, 0);

    status = ucp_proto_repost_save(req, op_info);
    if (status != UCS_OK) {
        ucp_proto_repost_release(req, status);
        goto err;
    }

    if (ucp_proto_repost_origin_comp(req) != NULL) {
        ucp_proto_completion_init(&req->send.state.uct_comp,
                                  ucp_proto_repost_completion);
    }

    ucp_trace_req(req, "repost submit op %s",
                  uct_ep_operation_names[op_info->operation]);
    ucp_request_send(req);
    return UCS_OK;

err:
    /* The purge callback completes an operation which has a completion */
    if (!(op_info->field_mask & UCT_EP_OP_INFO_FIELD_COMP)) {
        ucp_proto_repost_fail_ep(ep, op_info, status);
    }

    return status;
}
