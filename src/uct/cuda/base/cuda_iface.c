/**
 * Copyright (C) Mellanox Technologies Ltd. 2019.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "cuda_iface.h"


ucs_status_t
uct_cuda_base_query_devices(uct_md_h md, uct_tl_device_resource_t **tl_devices_p,
                           unsigned *num_tl_devices_p)
{
    ucs_sys_device_t sys_device = UCS_SYS_DEVICE_ID_UNKNOWN;
    CUdevice cuda_device;
    int device_count;

    if ((cuCtxGetDevice(&cuda_device) == CUDA_SUCCESS) ||
        /* if there is no selected device by user, get system info for the first
         * as a workaround */
        ((cudaGetDeviceCount(&device_count) == cudaSuccess) &&
         (device_count > 0) &&
         (cuDeviceGet(&cuda_device, 0) == CUDA_SUCCESS))) {
        uct_cuda_base_get_sys_dev(cuda_device, &sys_device);
    }

    return uct_single_device_resource(md, UCT_CUDA_DEV_NAME,
                                      UCT_DEVICE_TYPE_ACC, sys_device,
                                      tl_devices_p, num_tl_devices_p);
}
