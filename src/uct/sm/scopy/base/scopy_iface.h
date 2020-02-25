/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCT_SCOPY_IFACE_H
#define UCT_SCOPY_IFACE_H

#include <uct/base/uct_iface.h>
#include <uct/sm/base/sm_iface.h>


extern ucs_config_field_t uct_scopy_iface_config_table[];


typedef struct uct_scopy_iface_config {
    uct_sm_iface_config_t         super;
} uct_scopy_iface_config_t;


typedef struct uct_scopy_iface {
    uct_sm_iface_t                super;
} uct_scopy_iface_t;


void uct_scopy_iface_query(uct_iface_h tl_iface, uct_iface_attr_t *iface_attr);

UCS_CLASS_DECLARE(uct_scopy_iface_t, uct_iface_ops_t*, uct_md_h, uct_worker_h,
                  const uct_iface_params_t*, const uct_iface_config_t*);

#endif
