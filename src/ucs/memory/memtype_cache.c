/**
 * Copyright (C) Mellanox Technologies Ltd. 2018.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "memtype_cache.h"

#include <ucs/arch/atomic.h>
#include <ucs/type/class.h>
#include <ucs/datastruct/rbtree.h>
#include <ucs/debug/log.h>
#include <ucs/profile/profile.h>
#include <ucs/debug/memtrack.h>
#include <ucs/stats/stats.h>
#include <ucs/sys/sys.h>
#include <ucs/sys/math.h>
#include <ucm/api/ucm.h>


#define UCS_MEMTYPE_CACHE_REGION_FMT "{[%p..%p], mem_type: %d} region"
#define UCS_MEMTYPE_CACHE_REGION_ARG(_region) \
    (_region)->iov.iov_base, \
    UCS_PTR_BYTE_OFFSET((_region)->iov.iov_base, \
                        (_region)->iov.iov_len), \
    (_region)->mem_type
                                              


typedef enum {
    UCS_MEMTYPE_CACHE_ACTION_SET_MEMTYPE,
    UCS_MEMTYPE_CACHE_ACTION_REMOVE
} ucs_memtype_cache_action_t;


static int ucs_memtype_cache_find_overlapped(void *key1, void *key2)
{
    struct iovec *iov1 = key1, *iov2 = key2;

    if (ucs_iov_left(iov1, iov2)) {
        return -1;
    } else if (ucs_iov_right(iov1, iov2)) {
        return 1;
    } else {
        return 0;
    }
}

int ucs_memtype_cache_is_empty(ucs_memtype_cache_t *memtype_cache)
{
    return (rbtBegin(memtype_cache->tree) == NULL);
}

/* Lock must be held in write mode */
static void ucs_memtype_cache_insert(ucs_memtype_cache_t *memtype_cache,
                                     void *address, size_t size,
                                     ucs_memory_type_t mem_type)
{
    ucs_memtype_cache_region_t *region;
    RbtStatus status;

    /* Allocate structure for new region */
    region = ucs_malloc(sizeof(ucs_memtype_cache_region_t),
                        "memtype_cache_region");
    if (region == NULL) {
        ucs_warn("failed to allocate memtype_cache region");
        return;
    }

    region->iov.iov_base = address;
    region->iov.iov_len  = size;
    region->mem_type     = mem_type;

    status = rbtInsert(memtype_cache->tree, &region->iov, region);
    if (status != RBT_STATUS_OK) {
        ucs_error("failed to insert "UCS_MEMTYPE_CACHE_REGION_FMT
                  "from RB tree : %d",
                  UCS_MEMTYPE_CACHE_REGION_ARG(region), status);
        ucs_free(region);
        return;
    }

    ucs_trace("inserted "UCS_MEMTYPE_CACHE_REGION_FMT" to RB tree",
              UCS_MEMTYPE_CACHE_REGION_ARG(region));
}

static ucs_memtype_cache_region_t*
ucs_memtype_cache_find_region(ucs_memtype_cache_t *memtype_cache,
                              void *address, size_t size, RbtIterator *iter)
{
    struct iovec iov = {
        .iov_base = address,
        .iov_len  = size
    };
    struct iovec *key;
    ucs_memtype_cache_region_t *region;

    *iter = rbtFind(memtype_cache->tree, &iov);
    if (*iter == NULL) {
        return NULL;
    }

    rbtKeyValue(memtype_cache->tree, *iter, (void**)&key, (void**)&region);

    return region;
}

/* find and remove all regions which intersect with specified one */
static void
ucs_memtype_cache_remove_matched_regions(ucs_memtype_cache_t *memtype_cache,
                                         void *address, size_t size,
                                         ucs_list_link_t *found_regions)
{
    ucs_memtype_cache_region_t *region;
    RbtStatus rbt_status;
    RbtIterator iter;

    while (1) {
        region = ucs_memtype_cache_find_region(memtype_cache, address,
                                               size, &iter);
        if (region == NULL) {
            break;
        }

        rbt_status = rbtErase(memtype_cache->tree, iter);
        if (rbt_status != RBT_STATUS_OK) {
            ucs_error("failed to remove "UCS_MEMTYPE_CACHE_REGION_FMT" from RB tree",
                      UCS_MEMTYPE_CACHE_REGION_ARG(region));
            continue;
        }

        ucs_trace("removed "UCS_MEMTYPE_CACHE_REGION_FMT" from RB tree",
                  UCS_MEMTYPE_CACHE_REGION_ARG(region));

        ucs_list_add_tail(found_regions, &region->list);
    }
}

UCS_PROFILE_FUNC_VOID(ucs_memtype_cache_update_internal,
                      (memtype_cache, address, size, mem_type, action),
                      ucs_memtype_cache_t *memtype_cache, void *address,
                      size_t size, ucs_memory_type_t mem_type,
                      ucs_memtype_cache_action_t action)
{
    UCS_LIST_HEAD(found_regions);
    ucs_memtype_cache_region_t *region;
    void *region_end_p, *end_p;

    pthread_rwlock_wrlock(&memtype_cache->lock);

    ucs_memtype_cache_remove_matched_regions(memtype_cache, address,
                                             size, &found_regions);

    ucs_list_for_each(region, &found_regions, list) {
        end_p        = UCS_PTR_BYTE_OFFSET(address, size);
        region_end_p = UCS_PTR_BYTE_OFFSET(region->iov.iov_base,
                                           region->iov.iov_len);

        if (address > region->iov.iov_base) {
            /* insert the left side of the previous buffer */
            ucs_memtype_cache_insert(memtype_cache, region->iov.iov_base,
                                     UCS_PTR_BYTE_DIFF(region->iov.iov_base,
                                                       address),
                                     region->mem_type);
        }

        if (end_p < region_end_p) {
            /* insert the right side of the previous buffer */
            ucs_memtype_cache_insert(memtype_cache, end_p,
                                     UCS_PTR_BYTE_DIFF(end_p, region_end_p),
                                     region->mem_type);
        }

        ucs_free(region);
    }

    if (action == UCS_MEMTYPE_CACHE_ACTION_SET_MEMTYPE) {
        ucs_memtype_cache_insert(memtype_cache, address, size, mem_type);
    }

    pthread_rwlock_unlock(&memtype_cache->lock);
}

void ucs_memtype_cache_update(ucs_memtype_cache_t *memtype_cache, void *address,
                              size_t size, ucs_memory_type_t mem_type)
{
    ucs_memtype_cache_update_internal(memtype_cache, address, size, mem_type,
                                      UCS_MEMTYPE_CACHE_ACTION_SET_MEMTYPE);
}

static void ucs_memtype_cache_event_callback(ucm_event_type_t event_type,
                                              ucm_event_t *event, void *arg)
{
    ucs_memtype_cache_t *memtype_cache = arg;
    ucs_memtype_cache_action_t action;

    if (event_type & UCM_EVENT_MEM_TYPE_ALLOC) {
        action = UCS_MEMTYPE_CACHE_ACTION_SET_MEMTYPE;
    } else if (event_type & UCM_EVENT_MEM_TYPE_FREE) {
        action = UCS_MEMTYPE_CACHE_ACTION_REMOVE;
    } else {
        return;
    }

    ucs_memtype_cache_update_internal(memtype_cache, event->mem_type.address,
                                      event->mem_type.size,
                                      event->mem_type.mem_type, action);
}

UCS_PROFILE_FUNC(ucs_status_t, ucs_memtype_cache_lookup,
                 (memtype_cache, address, size, mem_type_p),
                 ucs_memtype_cache_t *memtype_cache, void *address,
                 size_t size, ucs_memory_type_t *mem_type_p)
{
    ucs_memtype_cache_region_t *region;
    RbtIterator iter;
    ucs_status_t status;

    pthread_rwlock_rdlock(&memtype_cache->lock);

    region = UCS_PROFILE_CALL(ucs_memtype_cache_find_region,
                              memtype_cache, address, size, &iter);
    if ((region == NULL) ||
        (UCS_PTR_BYTE_OFFSET(address, size) >
         UCS_PTR_BYTE_OFFSET(region->iov.iov_base,
                             region->iov.iov_len))) {
        status = UCS_ERR_NO_ELEM;
        goto out_unlock;
    }

    ucs_trace("found "UCS_MEMTYPE_CACHE_REGION_FMT" in RB tree",
              UCS_MEMTYPE_CACHE_REGION_ARG(region));

    *mem_type_p = region->mem_type;
    status      = UCS_OK;

out_unlock:
    pthread_rwlock_unlock(&memtype_cache->lock);
    return status;
}

static UCS_CLASS_INIT_FUNC(ucs_memtype_cache_t)
{
    ucs_status_t status;
    int ret;

    ret = pthread_rwlock_init(&self->lock, NULL);
    if (ret) {
        ucs_error("pthread_rwlock_init() failed: %m");
        status = UCS_ERR_INVALID_PARAM;
        goto err;
    }

    self->tree = rbtNew(ucs_memtype_cache_find_overlapped);
    if (self->tree == NULL) {
        ucs_error("failed to create RB tree");
        goto err_destroy_rwlock;
    }

    status = ucm_set_event_handler(UCM_EVENT_MEM_TYPE_ALLOC |
                                   UCM_EVENT_MEM_TYPE_FREE |
                                   UCM_EVENT_FLAG_EXISTING_ALLOC,
                                   1000, ucs_memtype_cache_event_callback,
                                   self);
    if (status != UCS_OK) {
        ucs_error("failed to set UCM memtype event handler: %s",
                  ucs_status_string(status));
        goto err_delete_tree;
    }

    return UCS_OK;

err_delete_tree:
    rbtDelete(self->tree);
err_destroy_rwlock:
    pthread_rwlock_destroy(&self->lock);
err:
    return status;
}

static UCS_CLASS_CLEANUP_FUNC(ucs_memtype_cache_t)
{
    ucm_unset_event_handler((UCM_EVENT_MEM_TYPE_ALLOC | UCM_EVENT_MEM_TYPE_FREE),
                            ucs_memtype_cache_event_callback, self);
    rbtDelete(self->tree);
    pthread_rwlock_destroy(&self->lock);
}

UCS_CLASS_DEFINE(ucs_memtype_cache_t, void);
UCS_CLASS_DEFINE_NAMED_NEW_FUNC(ucs_memtype_cache_create, ucs_memtype_cache_t,
                                ucs_memtype_cache_t)
UCS_CLASS_DEFINE_NAMED_DELETE_FUNC(ucs_memtype_cache_destroy, ucs_memtype_cache_t,
                                   ucs_memtype_cache_t)
