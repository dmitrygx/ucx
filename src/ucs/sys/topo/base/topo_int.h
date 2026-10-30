/**
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2026. ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef UCS_TOPO_INT_H
#define UCS_TOPO_INT_H

#include "topo.h"

#include <ucs/config/types.h>

BEGIN_C_DECLS

/* Possible role of a current device wrt its sibling */
typedef enum {
    /* No sibling capability */
    UCS_TOPO_SIBLING_ROLE_NONE,

    /* Memory device, a sibling device could access its memory */
    UCS_TOPO_SIBLING_ROLE_MEM,

    /* Device that could access memory from its sibling */
    UCS_TOPO_SIBLING_ROLE_DEV
} ucs_topo_sibling_role_t;


typedef struct {
    ucs_sys_bus_id_t        bus_id;
    char                    *name;
    unsigned                name_priority;
    ucs_numa_node_t         numa_node;
    ucs_sys_pci_id_t        pci_id;
    uintptr_t               user_value;
    ucs_topo_device_class_t device_class;
    unsigned                flags; /* ucs_topo_device_flags_t */

    /* Cached rank of the device's BDF within its class, or
     * UCS_SYS_DEVICE_ORDINAL_INVALID if not yet computed.
     * Invalidated when any device's class changes. */
    unsigned                class_ordinal;

    /* Secondary device for the current device */
    ucs_sys_device_t        sys_dev_aux;

    ucs_topo_sibling_role_t sibling_role; /* Role of the current device */
    /* MEM role: matched DEV. DEV role: one representative matched MEM. */
    ucs_sys_device_t        sibling_sys_dev;

    /* Whether peer-to-peer traffic with an accelerator under the same root
     * port goes through the root complex, UCS_AUTO until detected */
    ucs_ternary_auto_value_t p2p_redirect;
} ucs_topo_sys_device_info_t;


/**
 * Compare two system device information entries by their topology identity.
 */
int ucs_topo_sys_device_info_cmp(const ucs_topo_sys_device_info_t *device1,
                                 const ucs_topo_sys_device_info_t *device2);


/**
 * Convert a PCI bus id to its canonical sysfs path.
 *
 * @param [in]  bus_id  Device PCI bus id.
 * @param [out] path    Filled with the resolved sysfs path. Must have room
 *                      for at least PATH_MAX bytes.
 * @param [in]  max     Size of @a path in bytes.
 *
 * @return UCS_OK on success, or an error status otherwise.
 */
ucs_status_t ucs_topo_bus_id_to_sysfs_path(const ucs_sys_bus_id_t *bus_id,
                                           char *path, size_t max);


/**
 * Check in sysfs whether peer-to-peer traffic between a PCI device and an
 * accelerator under the same PCIe root port goes through the root complex.
 *
 * @param [in] pci_devices_dir  Directory with links to all PCI devices, such
 *                              as /sys/bus/pci/devices.
 * @param [in] dev_path         Canonical sysfs path of the device.
 *
 * @return Nonzero if such an accelerator exists, 0 otherwise.
 */
int ucs_topo_sysfs_is_p2p_redirected(const char *pci_devices_dir,
                                     const char *dev_path);


END_C_DECLS

#endif
