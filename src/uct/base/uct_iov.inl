/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef UCT_IOV_INL_
#define UCT_IOV_INL_

#include <uct/api/uct.h>
#include <ucs/sys/math.h>
#include <ucs/debug/assert.h>

#include <ucs/sys/iovec.h>
#include <ucs/sys/iovec.inl>


/**
 * Calculates total length of the particular UCT IOV data buffer.
 *
 * @param [in]     iov             Pointer to the UCT IOV element.
 *
 * @return The length of the UCT IOV data buffer.
 * @note Currently has no support for stride. If stride supported it
 *       should be like: length + ((count - 1) * stride)
 */
static UCS_F_ALWAYS_INLINE
size_t uct_iov_get_length(const uct_iov_t *iov)
{
    return iov->count * iov->length;
}

/**
 * Returns the particular UCT IOV data buffer.
 *
 * @param [in]     iov             Pointer to the UCT IOV element.
 *
 * @return The UCT IOV data buffer.
 */
static UCS_F_ALWAYS_INLINE
void *uct_iov_get_buffer(const uct_iov_t *iov)
{
    return iov->buffer;
}

/**
 * Calculates total length of the UCT IOV array buffers.
 *
 * @param [in]     iov             Pointer to the array of UCT IOVs.
 * @param [in]     iovcnt          Number of the elements in the array of UCT IOVs.
 *
 * @return The total length of the array of UCT IOVs.
 */
static UCS_F_ALWAYS_INLINE
size_t uct_iov_total_length(const uct_iov_t *iov, size_t iovcnt)
{
    size_t iov_it, total_length = 0;

    for (iov_it = 0; iov_it < iovcnt; ++iov_it) {
        total_length += uct_iov_get_length(&iov[iov_it]);
    }

    return total_length;
}

/**
 * Fill IOVEC data structure by data provided in the array of UCT IOVs.
 * The function avoids copying IOVs with zero length.
 *
 * @param [out]    io_vec          Pointer to the resulted array of IOVECs.
 * @param [in/out] io_vec_cnt_p    Pointer to the varibale that holds the number
 *                                 of the elements in the array of IOVECs (input:
 *                                 initial, out: result).
 * @param [in]     uct_iov         Pointer to the array of UCT IOVs.
 * @param [in]     uct_iov_cnt     Number of the elements in the array of UCT IOVs.
 * @param [in]     max_length      The maximal total length of bytes that can be
 *                                 in the resulted array of IOVECs.
 * @param [in]     uct_iov_iter    Pointer to the UCT IOV iterator.
 *
 * @return The total length of the resulted array of IOVECs.
 */
static UCS_F_ALWAYS_INLINE
size_t uct_iov_to_io_vec(struct iovec *io_vec, size_t *io_vec_cnt_p,
                         const uct_iov_t *uct_iov, size_t uct_iov_cnt,
                         size_t max_length, ucs_iov_iter_t *uct_iov_iter_p)
{
    return ucs_iov_converter(io_vec, io_vec_cnt_p,
                             ucs_iov_set_buffer, ucs_iov_set_length,
                             uct_iov, uct_iov_cnt,
                             uct_iov_get_buffer, uct_iov_get_length,
                             max_length, uct_iov_iter_p);
}

#endif
