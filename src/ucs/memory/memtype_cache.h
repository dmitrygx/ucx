/**
 * Copyright (C) Mellanox Technologies Ltd. 2018.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef UCS_MEMTYPE_CACHE_H_
#define UCS_MEMTYPE_CACHE_H_

#include "memory_type.h"

#include <ucs/datastruct/pgtable.h>
#include <ucs/datastruct/list.h>
#include <ucs/sys/iovec.h>
#include <ucs/stats/stats_fwd.h>
#include <ucs/sys/compiler_def.h>


BEGIN_C_DECLS

typedef struct ucs_memtype_cache         ucs_memtype_cache_t;
typedef struct ucs_memtype_cache_region  ucs_memtype_cache_region_t;

struct ucs_memtype_cache_region {
    struct iovec        iov;
    ucs_memory_type_t   mem_type; /**< Memory type the address belongs to */
    ucs_list_link_t     list;
};


struct ucs_memtype_cache {
    pthread_rwlock_t    lock;       /**< protests the page table */
    void                *tree;
};


/**
 * Create a memtype cache.
 *
 * @param [out] memtype_cache_p Filled with a pointer to the memtype cache.
 */
ucs_status_t ucs_memtype_cache_create(ucs_memtype_cache_t **memtype_cache_p);


/**
 * Destroy a memtype cache.
 *
 * @param [in]  memtype_cache       Memtype cache to destroy.
 */
void ucs_memtype_cache_destroy(ucs_memtype_cache_t *memtype_cache);


int ucs_memtype_cache_is_empty(ucs_memtype_cache_t *memtype_cache);


/**
 * Find if address range is in memtype cache.
 *
 * @param [in]  memtype_cache   Memtype cache to search
 * @param [in]  address         Address to lookup
 * @param [in]  size            Length of the memory
 * @param [out] mem_type_p      Set to the memory type of the address range.
 *                              UCS_MEMORY_TYPE_LAST is a special value which
 *                              means the memory type is an unknown non-host
 *                              memory, and should be detected in another way.
 *
 * @return Error code.
 */
ucs_status_t
ucs_memtype_cache_lookup(ucs_memtype_cache_t *memtype_cache, void *address,
                         size_t size, ucs_memory_type_t *mem_type_p);


/**
 * Update the memory type of an address range.
 * Can be used after @ucs_memtype_cache_lookup returns UCM_MEM_TYPE_LAST, to
 * set the memory type after it was detected.
 *
 * @param [in]  address         Start address to update.
 * @param [in]  size            Size of the memory to update.
 * @param [out] mem_type        Set the memory type of the address range to this
 *                              value.
 */
void ucs_memtype_cache_update(ucs_memtype_cache_t *memtype_cache, void *address,
                              size_t size, ucs_memory_type_t mem_type);

END_C_DECLS

#endif
