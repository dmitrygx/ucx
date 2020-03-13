/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef UCS_IOVEC_INL_
#define UCS_IOVEC_INL_

#include <ucs/sys/math.h>
#include <ucs/sys/iovec.h>
#include <ucs/debug/assert.h>


/**
 * Fill the destination array of IOVs by data provided in the source
 * array of IOVs.
 * The function avoids copying IOVs with zero length.
 *
 * @param [out]    _dst_iov              Pointer to the resulted array of IOVs.
 * @param [in/out] _dst_iov_cnt_p        Pointer to the varibale that holds the number
 *                                       of the elements in the array of IOVs (input:
 *                                       initial, out: result).
 * @param [in]     _dst_iov_set_buffer_f Function that sets the buffer to the IOV element
 *                                       from the destination array.
 * @param [in]     _dst_iov_set_length_f Function that sets the length to the IOV element
 *                                       from the destination array.
 * @param [in]     _src_iov              Pointer to the source array of IOVs.
 * @param [in]     _src_iov_cnt          Number of the elements in the source array of IOVs.
 * @param [in]     _src_iov_get_buffer_f Function that gets the buffer of the IOV element
 *                                       from the destination array.
 * @param [in]     _src_iov_get_length_f Function that gets the length of the IOV element
 *                                       from the destination array.
 * @param [in]     _max_length           The maximal total length of bytes that can be
 *                                       in the resulted array of IOVs.
 * @param [in]     _iov_iter_p           Pointer to the IOV iterator.
 *
 * @return The total length of the resulted array of IOVs.
 */
#define ucs_iov_converter(_dst_iov, _dst_iov_cnt_p, \
                          _dst_iov_set_buffer_f, _dst_iov_set_length_f, \
                          _src_iov, _src_iov_cnt, \
                          _src_iov_get_buffer_f, _src_iov_get_length_f, \
                          _max_length, _iov_iter_p) \
   ({ \
        size_t __remain_length = _max_length; \
        size_t __total_length  = 0; \
        size_t __dst_iov_it    = 0; \
        size_t __src_iov_it    = (_iov_iter_p)->iov_offset; \
        size_t __dst_iov_length, __calc_iov_length; \
        \
        while ((__src_iov_it < _src_iov_cnt) && (__remain_length != 0) && \
               (__dst_iov_it < *(_dst_iov_cnt_p))) { \
            ucs_assert(_src_iov_get_length_f(&(_src_iov)[__src_iov_it]) >= \
                       (_iov_iter_p)->buffer_offset); \
            __calc_iov_length = _src_iov_get_length_f(&(_src_iov)[__src_iov_it]) - \
                                (_iov_iter_p)->buffer_offset; \
            if (__calc_iov_length == 0) { \
                /* Avoid zero length elements in resulted IOV */ \
                ++__src_iov_it; \
                continue; \
            } \
            \
            __dst_iov_length = ucs_min(__calc_iov_length, __remain_length); \
            \
            _dst_iov_set_length_f(&(_dst_iov)[__dst_iov_it], __dst_iov_length); \
            _dst_iov_set_buffer_f(&(_dst_iov)[__dst_iov_it], \
                                  UCS_PTR_BYTE_OFFSET(_src_iov_get_buffer_f( \
                                                          &(_src_iov)[__src_iov_it]), \
                                                      (_iov_iter_p)->buffer_offset)); \
            \
            if (__calc_iov_length > __remain_length) { \
                (_iov_iter_p)->buffer_offset += __remain_length; \
            } else { \
                ucs_assert(((_iov_iter_p)->buffer_offset == 0) || \
                           (__src_iov_it == (_iov_iter_p)->iov_offset)); \
                (_iov_iter_p)->buffer_offset = 0; \
                ++__src_iov_it; \
            } \
            \
            ucs_assert(__remain_length >= __dst_iov_length); \
            __total_length             += __dst_iov_length; \
            __remain_length            -= __dst_iov_length; \
            ++__dst_iov_it; \
            \
        } \
        \
        ucs_assert((__total_length <= _max_length) && \
                   (__dst_iov_it <= *(_dst_iov_cnt_p))); \
        (_iov_iter_p)->iov_offset = __src_iov_it; \
        *(_dst_iov_cnt_p)         = __dst_iov_it; \
        __total_length; \
    })


/**
 * Initializes IOV iterator by the initial values.
 *
 * @param [in] iov_iter    Pointer to the IOV iterator.
 */
static UCS_F_ALWAYS_INLINE
void ucs_iov_iter_init(ucs_iov_iter_t *iov_iter)
{
    iov_iter->iov_offset    = 0;
    iov_iter->buffer_offset = 0;
}

/**
 * Sets the particular IOV data buffer.
 *
 * @param [in]     iov             Pointer to the IOV element.
 * @param [in]     length          The length that needs to be set.
 */
static UCS_F_ALWAYS_INLINE
void ucs_iov_set_length(struct iovec *iov, size_t length)
{
    iov->iov_len = length;
}

/**
 * Sets the length of the particular IOV data buffer.
 *
 * @param [in]     iov             Pointer to the IOV element.
 * @param [in]     buffer          The buffer that needs to be set.
 */
static UCS_F_ALWAYS_INLINE
void ucs_iov_set_buffer(struct iovec *iov, void *buffer)
{
    iov->iov_base = buffer;
}

#endif
