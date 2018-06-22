/* -*-pgsql-c-*- */
/*
 * pgpool: a language independent connection pool server for PostgreSQL
 * written by Tatsuo Ishii
 *
 * Copyright (c) 2003-2018	PgPool Global Development Group
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose and without fee is hereby
 * granted, provided that the above copyright notice appear in all
 * copies and that both that copyright notice and this permission
 * notice appear in supporting documentation, and that the name of the
 * author not be used in advertising or publicity pertaining to
 * distribution of the software without specific, written prior
 * permission. The author makes no representations about the
 * suitability of this software for any purpose.  It is provided "as
 * is" without express or implied warranty.
 *
 * pool_shmem.c: query cache module for shared memory
 *
 */
#include "pool.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <arpa/inet.h>
#include <dirent.h>

#ifdef USE_MEMCACHED
#include <libmemcached/memcached.h>
#endif

#include "auth/md5.h"
#include "pool_config.h"
#include "protocol/pool_proto_modules.h"
#include "parser/parsenodes.h"
#include "context/pool_session_context.h"
#include "query_cache/pool_memqcache.h"
#include "utils/pool_relcache.h"
#include "utils/pool_select_walker.h"
#include "utils/pool_stream.h"
#include "utils/pool_stream.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/memutils.h"


#ifdef USE_MEMCACHED
memcached_st *memc;
#endif

static POOL_CACHEID *pool_find_item_on_shmem_cache(POOL_QUERY_HASH *query_hash);
static void pool_set_memqcache_blocks(int num_blocks);
static int pool_get_memqcache_blocks(void);
static void *pool_memory_cache_address(void);
static void pool_reset_fsmm(size_t size);
static void *pool_fsmm_address(void);
static void pool_update_fsmm(POOL_CACHE_BLOCKID blockid, size_t free_space);
static POOL_CACHE_BLOCKID pool_get_block(size_t free_space);
static POOL_CACHE_ITEM_HEADER *pool_cache_item_header(POOL_CACHEID *cacheid);
static int pool_init_cache_block(POOL_CACHE_BLOCKID blockid);
#if NOT_USED
static void pool_wipe_out_cache_block(POOL_CACHE_BLOCKID blockid);
#endif

static char *block_address(int blockid);
static POOL_CACHE_ITEM_POINTER *item_pointer(char *block, int i);
static POOL_CACHE_ITEM_HEADER *item_header(char *block, int i);
static POOL_CACHE_BLOCKID pool_reuse_block(void);
#ifdef SHMEMCACHE_DEBUG
static void dump_shmem_cache(POOL_CACHE_BLOCKID blockid);
#endif

static int pool_hash_reset(int nelements);
static int pool_hash_insert(POOL_QUERY_HASH *key, POOL_CACHEID *cacheid, bool update);
static uint32 create_hash_key(POOL_QUERY_HASH *key);
static volatile POOL_HASH_ELEMENT *get_new_hash_element(void);
static void put_back_hash_element(volatile POOL_HASH_ELEMENT *element);
static bool is_free_hash_element(void);

/*
 * Remember memory cache number of blocks.
 */
static int memqcache_num_blocks;
static void pool_set_memqcache_blocks(int num_blocks)
{
	memqcache_num_blocks = num_blocks;
}

/*
 * Return memory cache number of blocks.
 */
static int pool_get_memqcache_blocks(void)
{
	return memqcache_num_blocks;
}

/*
 * Query cache on shared memory management modules.
 */

/*
 * Calculate necessary shared memory size.
 */
size_t pool_shared_memory_cache_size(void)
{
	int64 num_blocks;
	size_t size;

	if (pool_config->memqcache_maxcache > pool_config->memqcache_cache_block_size)
		ereport(FATAL,
			(errmsg("invalid memory cache configuration"),
					errdetail("memqcache_cache_block_size %d should be greater or equal to memqcache_maxcache %d",
								pool_config->memqcache_cache_block_size,
								pool_config->memqcache_maxcache)));


	num_blocks = pool_config->memqcache_total_size/
		pool_config->memqcache_cache_block_size;
	if (num_blocks == 0)
		ereport(FATAL,
			(errmsg("invalid memory cache configuration"),
					errdetail("memqcache_total_size %ld should be greater or equal to memqcache_cache_block_size %d",
								pool_config->memqcache_total_size,
								pool_config->memqcache_cache_block_size)));

		ereport(LOG,
			(errmsg("memory cache initialized"),
					errdetail("memcache blocks :%ld",num_blocks)));
	/* Remember # of blocks */
	pool_set_memqcache_blocks(num_blocks);
	size = pool_config->memqcache_cache_block_size * num_blocks;
	return size;
}

/*
 * Acquire and initialize shared memory cache. This should be called
 * only once from pgpool main process at the process staring up time.
 */
static void *shmem;
int pool_init_memory_cache(size_t size)
{
	ereport(DEBUG1,
		(errmsg("memory cache request size : %zd",size)));

	shmem = pool_shared_memory_create(size);
	return 0;
}

/*
 * Clear all the shared memory cache and reset FSMM and hash table.
 */
void
pool_clear_memory_cache(void)
{
	size_t size;
	pool_sigset_t oldmask;

	POOL_SETMASK2(&BlockSig, &oldmask);
	pool_shmem_lock();

    PG_TRY();
    {
        size = pool_shared_memory_cache_size();
        memset(shmem, 0, size);

        size = pool_shared_memory_fsmm_size();
        pool_reset_fsmm(size);

        pool_discard_oid_maps();

        pool_hash_reset(pool_config->memqcache_max_num_cache);
    }
    PG_CATCH();
    {
        pool_shmem_unlock();
        POOL_SETMASK(&oldmask);
        PG_RE_THROW();
    }
    PG_END_TRY();

	pool_shmem_unlock();
	POOL_SETMASK(&oldmask);
}

/*
 * Return shared memory cache address
 */
static void *pool_memory_cache_address(void)
{
	return shmem;
}

/*
 * Initialize new block
 */

/*
 * Free space management map
 * 
 * Free space management map (FSMM) consists of bytes. Each byte
 * corresponds to block id. For example, if you have 1GB cache and
 * block size is 8Kb, number of blocks = 131,072, thus total size of
 * FSMM is 128Kb.  Each FSMM entry has value from 0 to 255. Those
 * values describes total free space in each block.
 * For example, if the value is 2, the free space can be between 64
 * bytes and 95 bytes.
 *
 * value free space(in bytes)
 * 0     0-31
 * 1     32-63
 * 2     64-95
 * 3     96-127
 * :
 * 255   8160-8192
 */

/*
 * Calculate necessary shared memory size for FSMM. Should be called after 
 * pool_shared_memory_cache_size.
 */
size_t pool_shared_memory_fsmm_size(void)
{
	size_t size;

	size = pool_get_memqcache_blocks() * sizeof(char);
	return size;
}

/*
 * Acquire and initialize shared memory cache for FSMM. This should be
 * called after pool_shared_memory_cache_size only once from pgpool
 * main process at the process staring up time.
 */
static void *fsmm;
int pool_init_fsmm(size_t size)
{
	int maxblock = 	pool_get_memqcache_blocks();
	int encode_value;

	fsmm = pool_shared_memory_create(size);
	encode_value = POOL_MAX_FREE_SPACE/POOL_FSMM_RATIO;
	memset(fsmm, encode_value, maxblock);
	return 0;
}

/*
 * Return shared memory fsmm address
 */
static void *pool_fsmm_address(void)
{
	return fsmm;
}

/*
 * Clock algorithm shared query cache management modules.
 */

/*
 * Clock hand pointing to next victim block
 */
static int *pool_fsmm_clock_hand;

/*
 * Allocate and initialize clock hand on shmem
 */
void pool_allocate_fsmm_clock_hand(void)
{
	pool_fsmm_clock_hand = pool_shared_memory_create(sizeof(*pool_fsmm_clock_hand));
	*pool_fsmm_clock_hand = 0;
}

/*
 * Reset FSMM.
 */
static void
pool_reset_fsmm(size_t size)
{
	int encode_value;

	encode_value = POOL_MAX_FREE_SPACE/POOL_FSMM_RATIO;
	memset(fsmm, encode_value, size);

	*pool_fsmm_clock_hand = 0;
}

/*
 * Find victim block using clock algorithm and make it free.
 * Returns new free block id.
 */
static POOL_CACHE_BLOCKID pool_reuse_block(void)
{
	int maxblock = pool_get_memqcache_blocks();
	char *block = block_address(*pool_fsmm_clock_hand);
	POOL_CACHE_BLOCK_HEADER *bh = (POOL_CACHE_BLOCK_HEADER *)block;
	POOL_CACHE_BLOCKID reused_block;
	POOL_CACHE_ITEM_POINTER *cip;
	char *p;
	int i;

	bh->flags = 0;
	reused_block = *pool_fsmm_clock_hand;
	p = block_address(reused_block);

	for (i=0;i<bh->num_items;i++)
	{
		cip = item_pointer(p, i);

		if (!(POOL_ITEM_DELETED & cip->flags))
		{
			pool_hash_delete(&cip->query_hash);
			ereport(DEBUG1,
					(errmsg("pool_reuse_block: blockid: %d item: %d", reused_block, i)));
		}
	}

	pool_init_cache_block(reused_block);
	pool_update_fsmm(reused_block, POOL_MAX_FREE_SPACE);

	(*pool_fsmm_clock_hand)++;
	if (*pool_fsmm_clock_hand >= maxblock)
		*pool_fsmm_clock_hand = 0;

	ereport(LOG,
			(errmsg("pool_reuse_block: blockid: %d", reused_block)));

	return reused_block;
}

/*
 * Get block id which has enough space
 */
static POOL_CACHE_BLOCKID pool_get_block(size_t free_space)
{
	int encode_value;
	unsigned char *p = pool_fsmm_address();
	int i;
	int maxblock = pool_get_memqcache_blocks();
	POOL_CACHE_BLOCK_HEADER *bh;

	if (p == NULL)
	{
		ereport(WARNING,
				(errmsg("memcache: getting block: FSMM is not initialized")));
		return -1;
	}

	if (free_space > POOL_MAX_FREE_SPACE)
	{
		ereport(WARNING,
			(errmsg("memcache: getting block: invalid free space:%zd", free_space),
				 errdetail("requested free space: %zd is more than maximum allowed space:%lu",free_space,POOL_MAX_FREE_SPACE)));
		return -1;
	}

	encode_value = free_space/POOL_FSMM_RATIO;

	for (i=0;i<maxblock;i++)
	{
		if (p[i] >= encode_value)
		{
			/*
			 * This block *may" have enough space.
			 * We need to make sure it actually has enough space.
			 */
			bh = (POOL_CACHE_BLOCK_HEADER *)block_address(i);
			if (bh->free_bytes >= free_space)
			{
				return (POOL_CACHE_BLOCKID)i;
			}
		}
	}

	/*
	 * No enough space found. Reuse victim block
	 */
	return pool_reuse_block();
}

/*
 * Update free space info for specified block
 */
static void pool_update_fsmm(POOL_CACHE_BLOCKID blockid, size_t free_space)
{
	int encode_value;
	char *p = pool_fsmm_address();

	if (p == NULL)
	{
		ereport(WARNING,
				(errmsg("memcache: updating free space in block: FSMM is not initialized")));
		return;
	}

	if (blockid >= pool_get_memqcache_blocks())
	{
		ereport(WARNING,
				(errmsg("memcache: updating free space in block: block id:%d in invalid",blockid)));
		return;
	}

	if (free_space > POOL_MAX_FREE_SPACE)
	{
		ereport(WARNING,
			(errmsg("memcache: updating free space in block: invalid free space:%zd", free_space),
				 errdetail("requested free space: %zd is more than maximum allowed space:%lu",free_space,POOL_MAX_FREE_SPACE)));

		return;
	}

	encode_value = free_space/POOL_FSMM_RATIO;

	p[blockid] = encode_value;

	return;
}

/*
 * Add item data to shared memory cache.
 * On successful registration, returns cache id.
 * The cache id is overwritten by the subsequent call to this function.
 * On error returns NULL.
 */
POOL_CACHEID *pool_add_item_shmem_cache(POOL_QUERY_HASH *query_hash, char *data, int size)
{
	static POOL_CACHEID cacheid;
	POOL_CACHE_BLOCKID blockid;
	POOL_CACHE_BLOCK_HEADER *bh;
	POOL_CACHE_ITEM_POINTER *cip;

	POOL_CACHE_ITEM ci;
	POOL_CACHE_ITEM_POINTER cip_body;
	char *item;

	int request_size;
	char *p;
	int i;
	char *src;
	char *dst;
	int num_deleted;
	char *dcip;
	char *dci;
	bool need_pack;
	char *work_buffer;
	int index;

	if (query_hash == NULL)
	{
		ereport(LOG,
				(errmsg("error while adding item to shmem cache, query hash is NULL")));
		return NULL;
	}

	if (data == NULL)
	{
		ereport(LOG,
				(errmsg("error while adding item to shmem cache, data is NULL")));
		return NULL;
	}

	if (size <= 0)
	{
		ereport(LOG,
				(errmsg("error while adding item to shmem cache, invalid request size: %d", size)));
		return NULL;
	}

	/* Add overhead */
	request_size = size + sizeof(POOL_CACHE_ITEM_POINTER) + sizeof(POOL_CACHE_ITEM_HEADER);

	/* Get cache block which has enough space */
	blockid = pool_get_block(request_size);

	if (blockid == -1)
	{
		return NULL;
	}

	/*
	 * Initialize the block if necessary. If no live items are
	 * remained, we also initialize the block. If there's contiguous
	 * deleted items, we turn them into free space as well.
	 */
	pool_init_cache_block(blockid);

	/*
	 * Make sure that we have at least one free hash element.
	 */
	while (!is_free_hash_element())
	{
		/* If not, reuse next victim block */
		blockid = pool_reuse_block();
		pool_init_cache_block(blockid);
	}

	/* Get block address on shmem */
	p = block_address(blockid);
	bh = (POOL_CACHE_BLOCK_HEADER *)p;

	/*
	 * Create contiguous free space. We assume that item bodies are
	 * ordered from bottom to top of the block, and corresponding item
	 * pointers are ordered from the youngest to the oldest in the
	 * beginning of the block.
	 */

	/*
	 * Optimization. If there's no deleted items, we don't need to
	 * pack it to create contiguous free space.
	 */
	need_pack = false;
	for (i=0;i<bh->num_items;i++)
	{
		cip = item_pointer(p, i);

		if (POOL_ITEM_DELETED & cip->flags)		/* Deleted item? */
		{
			need_pack = true;
			ereport(DEBUG1,
				(errmsg("memcache adding item"),
					 errdetail("start creating contiguous space")));
			break;
		}
	}

	/*
	 * We disable packing for now.
	 * Revisit and remove following code fragment later.
	 */
	need_pack = false;

	if (need_pack)
	{
		/*
		 * Pack and create contiguous free space.
		 */
		dcip = calloc(1, pool_config->memqcache_cache_block_size);
		if (!dcip)
		{
			ereport(WARNING,
					(errmsg("memcache: adding item to cache: calloc failed")));
			return NULL;
		}

		work_buffer = dcip;
		dci = dcip + pool_config->memqcache_cache_block_size;
		num_deleted = 0;
		index = 0;

		for (i=0;i<bh->num_items;i++)
		{
			int total_length;
			POOL_CACHEID cid;

			cip = item_pointer(p, i);

			if (POOL_ITEM_DELETED & cip->flags)		/* Deleted item? */
			{
				num_deleted++;
				continue;
			}

			/* Copy item body */
			src = p + cip->offset;
			total_length = item_header(p, i)->total_length;
			dst = dci - total_length;
			cip->offset = dst - work_buffer;
			memcpy(dst, src, total_length);

			dci -= total_length;

			/* Copy item pointer */
			src = (char *)cip;
			dst = (char *)item_pointer(dcip, index);
			memcpy(dst, src, sizeof(POOL_CACHE_ITEM_POINTER));

			/* Update hash index */
			cid.blockid = blockid;
			cid.itemid = index;
			pool_hash_insert(&cip->query_hash, &cid, true);
			ereport(DEBUG1,
				(errmsg("memcache adding item"),
					errdetail("item cid updated. old:%d %d new:%d %d",
						   blockid, i, blockid, index)));
			index++;
		}
	
		/* All items deleted? */
		if (num_deleted > 0 && num_deleted == bh->num_items)
		{
			ereport(DEBUG1,
				(errmsg("memcache adding item"),
					 errdetail("all items deleted, total deleted:%d", num_deleted)));
			bh->flags = 0;
			pool_init_cache_block(blockid);
			pool_update_fsmm(blockid, POOL_MAX_FREE_SPACE);
		}
		else
		{
			/* Update number of items */
			bh->num_items -= num_deleted;

			/* Copy back the packed block except block header */
			memcpy(p+sizeof(POOL_CACHE_BLOCK_HEADER),
				   work_buffer+sizeof(POOL_CACHE_BLOCK_HEADER),
				   pool_config->memqcache_cache_block_size-sizeof(POOL_CACHE_BLOCK_HEADER));
		}
		free(work_buffer);
	}

	/*
	 * Make sure that we have enough free space
	 */
	if (bh->free_bytes < request_size)
	{
		/* This should not happen */
		ereport(WARNING,
			(errmsg("memcache: adding item to cache: not enough space"),
				 errdetail("free space: %d required: %d block id:%d",
						   bh->free_bytes, request_size, blockid)));
		return NULL;
	}

	/*
	 * At this point, new item can be located at block_header->num_items
	 */

	/* Fill in cache item header */
	ci.header.timestamp = time(NULL);
	ci.header.total_length = sizeof(POOL_CACHE_ITEM_HEADER) + size;

	/* Calculate item body address */
	if (bh->num_items == 0)
	{
		/* This is the #0 item. So address is block_bottom -
		 * data_length */
		item = p + pool_config->memqcache_cache_block_size - ci.header.total_length;

		/* Mark this block used */
		bh->flags = POOL_BLOCK_USED;
	}
	else
	{
		cip = item_pointer(p, bh->num_items-1);
		item = p + cip->offset - ci.header.total_length;
	}

	/* Copy item header */
	memcpy(item, &ci, sizeof(POOL_CACHE_ITEM_HEADER));
	bh->free_bytes -= sizeof(POOL_CACHE_ITEM_HEADER);

	/* Copy item body */
	memcpy(item + sizeof(POOL_CACHE_ITEM_HEADER), data, size);
	bh->free_bytes -= size;

	/* Copy cache item pointer */
	memcpy(&cip_body.query_hash, query_hash, sizeof(POOL_QUERY_HASH));
	memset(&cip_body.next, 0, sizeof(POOL_CACHEID));
	cip_body.offset = item - p;
	cip_body.flags = POOL_ITEM_USED;
	memcpy(item_pointer(p, bh->num_items), &cip_body, sizeof(POOL_CACHE_ITEM_POINTER));
	bh->free_bytes -= sizeof(POOL_CACHE_ITEM_POINTER);

	/* Update FSMM */
	pool_update_fsmm(blockid, bh->free_bytes);

	cacheid.blockid = blockid;
	cacheid.itemid = bh->num_items;
	ereport(DEBUG1,
		(errmsg("memcache adding item"),
			errdetail("new item inserted. blockid: %d itemid:%d",
				   cacheid.blockid, cacheid.itemid)));

	/* Add up number of items */
	bh->num_items++;

	/* Update hash table */
	if (pool_hash_insert(query_hash, &cacheid, false) < 0)
	{
		ereport(LOG,
				(errmsg("error while adding item to shmem cache, hash insert failed")));

		/* Since we have failed to insert hash index entry, we need to
		 * undo the addition of cache entry.
		 */
		pool_delete_item_shmem_cache(&cacheid);
		return NULL;
	}
	ereport(DEBUG1,
		(errmsg("memcache adding item"),
			 errdetail("block: %d item: %d", cacheid.blockid, cacheid.itemid)));

#ifdef SHMEMCACHE_DEBUG
	dump_shmem_cache(blockid);
#endif
	return &cacheid;
}

/*
 * Returns item data address on shared memory cache specified by query hash.
 * Also data length is set to *size.
 * On error or data not found case returns NULL.
 * Detail is set to *sts. (0: success, 1: not found, -1: error)
 */
char *pool_get_item_shmem_cache(POOL_QUERY_HASH *query_hash, int *size, int *sts)
{
	POOL_CACHEID *cacheid;
	POOL_CACHE_ITEM_HEADER *cih;

	if (sts == NULL)
	{
		ereport(LOG,
				(errmsg("error while getting item from shmem cache, sts is NULL")));
		return NULL;
	}

	*sts = -1;

	if (query_hash == NULL)
	{
		ereport(LOG,
				(errmsg("error while getting item from shmem cache, query hash is NULL")));
		return NULL;
	}

	if (size == NULL)
	{
		ereport(LOG,
				(errmsg("error while getting item from shmem cache, size is NULL")));
		return NULL;
	}

	/*
	 * Find cache header by using hash table
	 */
	cacheid = pool_find_item_on_shmem_cache(query_hash);
	if (cacheid == NULL)
	{
		/* Not found */
		*sts = 1;
		return NULL;
	}

	cih = pool_cache_item_header(cacheid);

	*size = cih->total_length - sizeof(POOL_CACHE_ITEM_HEADER);
	return (char *)cih + sizeof(POOL_CACHE_ITEM_HEADER);
}

/*
 * Find data on shared memory cache specified query hash.
 * On success returns cache id.
 * The cache id is overwritten by the subsequent call to this function.
 */
static POOL_CACHEID *pool_find_item_on_shmem_cache(POOL_QUERY_HASH *query_hash)
{
	static POOL_CACHEID cacheid;
	POOL_CACHEID *c;
	POOL_CACHE_ITEM_HEADER *cih;
	time_t now;

	c = pool_hash_search(query_hash);
	if (!c)
	{
		return NULL;
	}

	cih = item_header(block_address(c->blockid), c->itemid);

	/* Check cache expiration */
	if (pool_config->memqcache_expire > 0)
	{
		now = time(NULL);
		if (now > (cih->timestamp + pool_config->memqcache_expire))
		{
			ereport(DEBUG1,
				(errmsg("memcache finding item"),
					errdetail("cache expired: now: %ld timestamp: %ld",
						   now, cih->timestamp + pool_config->memqcache_expire)));
			pool_delete_item_shmem_cache(c);
			return NULL;
		}
	}

	cacheid.blockid = c->blockid;
	cacheid.itemid = c->itemid;
	return &cacheid;
}

/*
 * Delete item data specified cache id from shmem.
 * On successful deletion, returns 0.
 * Other wise return -1.
 * FSMM is also updated.
 */
int pool_delete_item_shmem_cache(POOL_CACHEID *cacheid)
{
	POOL_CACHE_BLOCK_HEADER *bh;
	POOL_CACHE_ITEM_POINTER *cip;
	POOL_CACHE_ITEM_HEADER *cih;
	POOL_QUERY_HASH key;
	int size;
	ereport(DEBUG1,
		(errmsg("memcache deleting item data"),
			errdetail("cacheid:%d itemid:%d",cacheid->blockid, cacheid->itemid)));

	if (cacheid->blockid >= pool_get_memqcache_blocks())
	{
		ereport(LOG,
				(errmsg("error while deleting item from shmem cache, invalid block: %d",
						cacheid->blockid)));
		return -1;
	}

	bh = (POOL_CACHE_BLOCK_HEADER *)block_address(cacheid->blockid);
	if (!(bh->flags & POOL_BLOCK_USED))
	{
		ereport(LOG,
				(errmsg("error while deleting item from shmem cache, block: %d is not used",
						cacheid->blockid)));
		return -1;
	}

	if (cacheid->itemid >= bh->num_items)
	{
		/*
		 * This could happen if the block is reused.  Since contents
		 * of oid map file is not updated when the block is reused.
		 */
		ereport(DEBUG1,
			(errmsg("memcache error deleting item data"),
				errdetail("invalid item id %d in block:%d",
					   cacheid->itemid, cacheid->blockid)));

		return -1;
	}

	cip = item_pointer(block_address(cacheid->blockid), cacheid->itemid);
	if (!(cip->flags & POOL_ITEM_USED))
	{
		ereport(LOG,
				(errmsg("error while deleting item from shmem cache, item: %d was not used",
						cacheid->itemid)));
		return -1;
	}

	if (cip->flags & POOL_ITEM_DELETED)
	{
		ereport(LOG,
				(errmsg("error while deleting item from shmem cache, item: %d was already deleted",
						cacheid->itemid)));
		return -1;
	}

	/* Save cache key */
	memcpy(&key, &cip->query_hash, sizeof(POOL_QUERY_HASH));

	cih = pool_cache_item_header(cacheid);
	size = cih->total_length + sizeof(POOL_CACHE_ITEM_POINTER);

	/* Delete item pointer */
	cip->flags |= POOL_ITEM_DELETED;

	/*
	 * We do NOT count down bh->num_items here. The deleted space will be recycled
	 * by pool_add_item_shmem_cache(). However, if this is the last item, we can
	 * recycle whole block.
	 *
	 * 2012/4/1: Now we do not pack data in
	 * pool_add_item_shmem_cache() for performance reason. Also we
	 * count down num_items if it is the last one.
	 */
	if ((bh->num_items -1) == 0)
	{
		ereport(DEBUG1,
			(errmsg("memcache deleting item data"),
				 errdetail("no item remains. initialize block")));
		bh->flags = 0;
		pool_init_cache_block(cacheid->blockid);
	}

	/* Remove hash index */
	pool_hash_delete(&key);

	/*
	 * If the deleted item is last one in the block, we add it to the free space.
	 */
	if (cacheid->itemid == (bh->num_items -1))
	{
		bh->free_bytes += size;
		ereport(DEBUG1,
			(errmsg("memcache deleting item data"),
				errdetail("deleted %d bytes, freebytes is = %d",
					   size, bh->free_bytes)));

		bh->num_items--;
	}

	/* Update FSMM */
	pool_update_fsmm(cacheid->blockid, bh->free_bytes);

	return 0;
}

/*
 * Returns item header specified by cache id.
 */
static POOL_CACHE_ITEM_HEADER *pool_cache_item_header(POOL_CACHEID *cacheid)
{
	POOL_CACHE_BLOCK_HEADER *bh;

	if (cacheid->blockid >= pool_get_memqcache_blocks())
	{
		ereport(WARNING,
				(errmsg("error while getting cache item header, invalid block id: %d", cacheid->blockid)));
		return NULL;
	}

	bh = (POOL_CACHE_BLOCK_HEADER *)block_address(cacheid->blockid);
	if (cacheid->itemid >= bh->num_items)
	{
		ereport(WARNING,
				(errmsg("error while getting cache item header, invalid item id: %d", cacheid->itemid)));
		return NULL;
	}

	return item_header((char *)bh, cacheid->itemid);
}

/*
 * Initialize specified block.
 */
static int pool_init_cache_block(POOL_CACHE_BLOCKID blockid)
{
	char *p;
	POOL_CACHE_BLOCK_HEADER *bh;

	if (blockid >= pool_get_memqcache_blocks())
	{
		ereport(WARNING,
				(errmsg("error while initializing cache block, invalid block id: %d", blockid)));
		return -1;
	}

	p = block_address(blockid);
	bh = (POOL_CACHE_BLOCK_HEADER *)p;

	/* Is this block used? */
	if (!(bh->flags & POOL_BLOCK_USED))
	{
		/* Initialize empty block */
		memset(p, 0, pool_config->memqcache_cache_block_size);
		bh->free_bytes = pool_config->memqcache_cache_block_size -
			sizeof(POOL_CACHE_BLOCK_HEADER);
	}
	return 0;
}

#if NOT_USED
/*
 * Delete all items in the block.
 */
static void pool_wipe_out_cache_block(POOL_CACHE_BLOCKID blockid)
{
	char *p;
	POOL_CACHE_BLOCK_HEADER *bh;
	POOL_CACHE_ITEM_POINTER *cip;
	POOL_CACHEID cacheid;
	int i;

	/* Get block address on shmem */
	p = block_address(blockid);
	bh = (POOL_CACHE_BLOCK_HEADER *)p;
	cacheid.blockid = blockid;

	for (i=0;i<bh->num_items;i++)
	{
		cip = item_pointer(p, i);

		if ((POOL_ITEM_DELETED & cip->flags) == 0)		/* Not deleted item? */
		{
			cacheid.itemid = i;
			pool_delete_item_shmem_cache(&cacheid);
		}
	}

	bh->flags = 0;
	pool_init_cache_block(blockid);
	pool_update_fsmm(blockid, POOL_MAX_FREE_SPACE);
}
#endif

/*
 * Acquire lock: XXX giant lock
 */
void pool_shmem_lock(void)
{
	if (pool_is_shmem_cache())
	{
		pool_semaphore_lock(SHM_CACHE_SEM);

	}
}

/*
 * Release lock
 */
void pool_shmem_unlock(void)
{
	if (pool_is_shmem_cache())
	{
		pool_semaphore_unlock(SHM_CACHE_SEM);
	}
}

/*
 * Returns cache block address specified by block id 
 */
static char *block_address(int blockid)
{
	char *p;

	p = pool_memory_cache_address() +
		blockid *	pool_config->memqcache_cache_block_size;
	return p;
}

/*
 * Returns i th item pointer in block address block
 */
static POOL_CACHE_ITEM_POINTER *item_pointer(char *block, int i)
{
	return (POOL_CACHE_ITEM_POINTER *)(block + sizeof(POOL_CACHE_BLOCK_HEADER) +
									   sizeof(POOL_CACHE_ITEM_POINTER) * i);
}

/*
 * Returns i th item header in block address block
 */
static POOL_CACHE_ITEM_HEADER *item_header(char *block, int i)
{
	POOL_CACHE_ITEM_POINTER *cip;

	cip = item_pointer(block, i);
	return (POOL_CACHE_ITEM_HEADER *)(block + cip->offset);
}

#ifdef SHMEMCACHE_DEBUG
/*
 * Dump shmem cache block
 */
static void dump_shmem_cache(POOL_CACHE_BLOCKID blockid)
{
	POOL_CACHE_BLOCK_HEADER *bh;
	POOL_CACHE_ITEM_POINTER *cip;
	POOL_CACHE_ITEM_HEADER *cih;
	int i;

	bh = (POOL_CACHE_BLOCK_HEADER *)block_address(blockid);
	fprintf(stderr, "shmem: block header(%lu bytes): flags:%x num_items:%d free_bytes:%d\n",
			sizeof(*bh), bh->flags, bh->num_items, bh->free_bytes);
	for (i=0;i<bh->num_items;i++)
	{
		cip = item_pointer((char *)bh, i);
		fprintf(stderr, "shmem: block: %d %d th item pointer(%lu bytes): offset:%d flags:%x\n",
				blockid, i, sizeof(*cip), cip->offset, cip->flags);
		cih = item_header((char *)bh, i);
		fprintf(stderr, "shmem: block: %d %d th item header(%lu bytes): timestamp:%ld length:%d\n",
				blockid, i, sizeof(*cih), cih->timestamp, cih->total_length);
	}
}
#endif

/*
 * On shared memory hash table implementation.  We use sub part of md5
 * hash key as hash function.  The experiment has shown that has_any()
 * of PostgreSQL is a little bit better than the method using part of
 * md5 hash value, but it seems adding some cpu cycles to call
 * hash_any() is not worth the trouble.
 */

static volatile POOL_HASH_HEADER *hash_header;
static volatile POOL_HASH_ELEMENT *hash_elements;
static volatile POOL_HASH_ELEMENT *hash_free;

/*
 * Initialize hash table on shared memory "nelements" is max number of
 * hash keys. The actual number of hash key is rounded up to power of
 * 2.
 */
#undef POOL_HASH_DEBUG

int pool_hash_init(int nelements)
{
	size_t size;
	int nelements2;		/* number of rounded up hash keys */
	int shift;
	uint32 mask;
	POOL_HASH_HEADER hh;
	int i;

	if (nelements <= 0)
		ereport(ERROR,
			(errmsg("initializing hash table on shared memory, invalid number of elements: %d",nelements)));

	/* Round up to power of 2 */
	shift = 32;
	nelements2 = 1;
	do
	{
		nelements2 <<= 1;
		shift--;
	} while (nelements2 < nelements);

	mask = ~0;
	mask >>= shift;
	size = (char *)&hh.elements - (char *)&hh + sizeof(POOL_HEADER_ELEMENT)*nelements2;
	hash_header = pool_shared_memory_create(size);
	hash_header->nhash = nelements2;
    hash_header->mask = mask;

#ifdef POOL_HASH_DEBUG
	ereport(LOG,
		(errmsg("initializing hash table on shared memory"),
			 errdetail("size:%zd nelements2:%d", size, nelements2)));

#endif

	size = sizeof(POOL_HASH_ELEMENT)*nelements2;
	hash_elements = pool_shared_memory_create(size);

#ifdef POOL_HASH_DEBUG
	ereport(LOG,
		(errmsg("initializing hash table on shared memory"),
			 errdetail("size:%zd nelements2:%d", size, nelements2)));
#endif

	for (i=0;i<nelements2-1;i++)
	{
		hash_elements[i].next = (POOL_HASH_ELEMENT *)&hash_elements[i+1];
	}
	hash_elements[nelements2-1].next = NULL;
	hash_free = hash_elements;

	return 0;
}

/*
 * Reset hash table on shared memory "nelements" is max number of
 * hash keys. The actual number of hash key is rounded up to power of
 * 2.
 */
static int
pool_hash_reset(int nelements)
{
	size_t size;
	int nelements2;		/* number of rounded up hash keys */
	int shift;
	uint32 mask;
	POOL_HASH_HEADER hh;
	int i;

	if (nelements <= 0)
		ereport(ERROR,
				(errmsg("clearing hash table on shared memory, invalid number of elements: %d",nelements)));

	/* Round up to power of 2 */
	shift = 32;
	nelements2 = 1;
	do
	{
		nelements2 <<= 1;
		shift--;
	} while (nelements2 < nelements);

	mask = ~0;
	mask >>= shift;

	size = (char *)&hh.elements - (char *)&hh + sizeof(POOL_HEADER_ELEMENT)*nelements2;
	memset((void *)hash_header, 0, size);

	hash_header->nhash = nelements2;
    	hash_header->mask = mask;

	size = sizeof(POOL_HASH_ELEMENT)*nelements2;
	memset((void *)hash_elements, 0, size);

	for (i=0;i<nelements2-1;i++)
	{
		hash_elements[i].next = (POOL_HASH_ELEMENT *)&hash_elements[i+1];
	}
	hash_elements[nelements2-1].next = NULL;
	hash_free = hash_elements;

	return 0;
}

/*
 * Search cacheid by MD5 hash key string
 * If found, returns cache id, otherwise NULL.
 */
POOL_CACHEID *pool_hash_search(POOL_QUERY_HASH *key)
{
	volatile POOL_HASH_ELEMENT *element;

	uint32 hash_key = create_hash_key(key);

	if (hash_key >= hash_header->nhash)
	{
		ereport(WARNING,
			(errmsg("memcache: searching cacheid from hash. invalid hash key"),
				errdetail("invalid hash key: %uld nhash: %ld",
					   hash_key, hash_header->nhash)));
		return NULL;
	}

	{
		char md5[POOL_MD5_HASHKEYLEN+1];
		memcpy(md5, key->query_hash, POOL_MD5_HASHKEYLEN);
		md5[POOL_MD5_HASHKEYLEN] = '\0';
#ifdef POOL_HASH_DEBUG
		ereport(LOG,
			(errmsg("searching hash table"),
				 errdetail("hash_key:%d md5:%s", hash_key, md5)));
#endif
	}

	element = hash_header->elements[hash_key].element;
	while (element)
	{
		{
			char md5[POOL_MD5_HASHKEYLEN+1];
			memcpy(md5, key->query_hash, POOL_MD5_HASHKEYLEN);
			md5[POOL_MD5_HASHKEYLEN] = '\0';
#ifdef POOL_HASH_DEBUG
			ereport(LOG,
				(errmsg("searching hash table"),
					 errdetail("element md5:%s", md5)));
#endif
		}

		if (memcmp((const void *)element->hashkey.query_hash,
				   (const void *)key->query_hash, sizeof(key->query_hash)) == 0)
		{
			return (POOL_CACHEID *)&element->cacheid;
		}
		element = element->next;
	}
	return NULL;
}

/*
 * Insert MD5 key and associated cache id into shmem hash table.  If
 * "update" is true, replace cacheid associated with the MD5 key,
 * rather than throw an error.
 */
static int pool_hash_insert(POOL_QUERY_HASH *key, POOL_CACHEID *cacheid, bool update)
{
	POOL_HASH_ELEMENT *element;
	POOL_HASH_ELEMENT *new_element;

	uint32 hash_key = create_hash_key(key);

	if (hash_key >= hash_header->nhash)
	{
		ereport(WARNING,
			(errmsg("memcache: adding cacheid to hash. invalid hash key"),
				 errdetail("invalid hash key: %uld nhash: %ld",
						   hash_key, hash_header->nhash)));
		return -1;
	}

	{
		char md5[POOL_MD5_HASHKEYLEN+1];
		memcpy(md5, key->query_hash, POOL_MD5_HASHKEYLEN);
		md5[POOL_MD5_HASHKEYLEN] = '\0';
#ifdef POOL_HASH_DEBUG
		ereport(LOG,
			(errmsg("searching hash table"),
				 errdetail("hash_key:%d md5:%s block:%d item:%d", hash_key, md5, cacheid->blockid, cacheid->itemid)));
#endif
	}

	/*
	 * Look for hash key.
	 */
	element = hash_header->elements[hash_key].element;

	while (element)
	{
		if (memcmp((const void *)element->hashkey.query_hash,
				   (const void *)key->query_hash, sizeof(key->query_hash)) == 0)
		{
			/* Hash key found. If "update" is false, just throw an error. */
			char md5[POOL_MD5_HASHKEYLEN+1];

			if (!update)
			{
				memcpy(md5, key->query_hash, POOL_MD5_HASHKEYLEN);
				md5[POOL_MD5_HASHKEYLEN] = '\0';
				ereport(LOG,
					(errmsg("memcache: adding cacheid to hash. hash key:\"%s\" already exists",md5)));
				return -1;
			}
			else
			{
				/* Update cache id */
				memcpy((void *)&element->cacheid, cacheid, sizeof(POOL_CACHEID));
				return 0;
			}
		}
		element = element->next;
	}

	/*
	 * Ok, same key did not exist. Just insert new hash key.
	 */
	new_element = (POOL_HASH_ELEMENT *)get_new_hash_element();
	if (!new_element)
	{
		ereport(LOG,
				(errmsg("memcache: adding cacheid to hash. failed to get new element")));
		return -1;
	}

	element = hash_header->elements[hash_key].element;

	hash_header->elements[hash_key].element = new_element;
	new_element->next = element;

	memcpy((void *)new_element->hashkey.query_hash, key->query_hash, POOL_MD5_HASHKEYLEN);
	memcpy((void *)&new_element->cacheid, cacheid, sizeof(POOL_CACHEID));

	return 0;
}

/*
 * Delete MD5 key and associated cache id into shmem hash table.
 */
int pool_hash_delete(POOL_QUERY_HASH *key)
{
	POOL_HASH_ELEMENT *element;
	POOL_HASH_ELEMENT **delete_point;
	bool found;

	uint32 hash_key = create_hash_key(key);

	if (hash_key >= hash_header->nhash)
	{
		ereport(LOG,
			(errmsg("memcache: deleting key from hash. invalid key"),
				 errdetail("invalid hash key: %uld nhash: %ld",
						   hash_key, hash_header->nhash)));
		return -1;
	}

	/*
	 * Look for delete location
	 */
	found = false;
	delete_point = (POOL_HASH_ELEMENT **)&(hash_header->elements[hash_key].element);
	element = hash_header->elements[hash_key].element;

	while (element)
	{
		if (memcmp(element->hashkey.query_hash, key->query_hash, sizeof(key->query_hash)) == 0)
		{
			found = true;
			break;
		}
		delete_point = &element->next;
		element = element->next;
	}

	if (!found)
	{
		char md5[POOL_MD5_HASHKEYLEN+1];

		memcpy(md5, key->query_hash, POOL_MD5_HASHKEYLEN);
		md5[POOL_MD5_HASHKEYLEN] = '\0';
		ereport(LOG,
			(errmsg("memcache: deleting key from hash. key:\"%s\" not found",md5)));
		return -1;
	}

	/*
	 * Put back the element to free list
	 */
	*delete_point = element->next;
	put_back_hash_element(element);

	return 0;
}

/* 
 * Calculate 32bit binary hash key(i.e. location in hash header) from MD5
 * string. We use top most 8 characters of MD5 string for calculation.
*/
static uint32 create_hash_key(POOL_QUERY_HASH *key)
{
#define POOL_HASH_NCHARS 8

	char md5[POOL_HASH_NCHARS+1];
	uint32 mask;

	memcpy(md5, key->query_hash, POOL_HASH_NCHARS);
	md5[POOL_HASH_NCHARS] = '\0';
	mask = strtoul(md5, NULL, 16);
	mask &= hash_header->mask;
	return mask;
}

/*
 * Get new free hash element from free list.
 */
static volatile POOL_HASH_ELEMENT *get_new_hash_element(void)
{
	volatile POOL_HASH_ELEMENT *elm;

	if (!hash_free->next)
	{
		/* No free element */
		return NULL;
	}

#ifdef POOL_HASH_DEBUG
	ereport(LOG,
		(errmsg("getting new hash element"),
			errdetail("hash_free->next:%p hash_free->next->next:%p",
				   hash_free->next, hash_free->next->next)));
#endif

	elm = hash_free->next;
	hash_free->next = elm->next;

	return elm;
}

/*
 * Put back hash element to free list.
 */
static void put_back_hash_element(volatile POOL_HASH_ELEMENT *element)
{
	POOL_HASH_ELEMENT *elm;

#ifdef POOL_HASH_DEBUG
	ereport(LOG,
		(errmsg("getting new hash element"),
			errdetail("hash_free->next:%p hash_free->next->next:%p",
				   hash_free->next, hash_free->next->next)));
#endif

	elm = hash_free->next;
	hash_free->next = (POOL_HASH_ELEMENT *)element;
	element->next = elm;
}

/*
 * Return true if there's a free hash element.
 */
static bool is_free_hash_element(void)
{
	return hash_free->next != NULL;
}

/*
 * Returns shared memory cache stats.
 * Subsequent call to this function will break return value
 * because its in static memory.
 * Caller must hold shmem_lock before calling this function.
 * If in memory query cache is not enabled, all stats are 0.
 */
POOL_SHMEM_STATS *pool_get_shmem_storage_stats(void)
{
	static POOL_SHMEM_STATS mystats;
	POOL_HASH_ELEMENT *element;
	int nblocks;
	int i;

	memset(&mystats, 0, sizeof(POOL_SHMEM_STATS));

	if (!pool_config-> memory_cache_enabled)
		return &mystats;

	/*
	 * Copy cache hit data
	 */
	mystats.cache_stats.num_selects = stats->num_selects;
	mystats.cache_stats.num_cache_hits = stats->num_cache_hits;

	if (pool_config->memqcache_method != SHMEM_CACHE)
		return &mystats;

	/* number of total hash entries */
	mystats.num_hash_entries = hash_header->nhash;

	/* number of used hash entries */
	for (i=0;i<hash_header->nhash;i++)
	{
		element = hash_header->elements[i].element;
		while (element)
		{
			mystats.used_hash_entries++;
			element = element->next;
		}
	}

	nblocks = pool_get_memqcache_blocks();

	for (i=0;i<nblocks;i++)
	{
		POOL_CACHE_BLOCK_HEADER *bh;
		POOL_CACHE_ITEM_POINTER *cip;
		char *p = block_address(i);
		bh = (POOL_CACHE_BLOCK_HEADER *)p;
		int j;

		if (bh->flags & POOL_BLOCK_USED)
		{
			for (j=0;j<bh->num_items;j++)
			{
				cip = item_pointer(p, j);
				if (POOL_ITEM_DELETED & cip->flags)
				{
					mystats.fragment_cache_entries_size += item_header(p, j)->total_length;
				}
				else
				{
					/* number of used cache entries */
					mystats.num_cache_entries++;
					/* total size of used cache entries */
					mystats.used_cache_entries_size += (item_header(p, j)->total_length + sizeof(POOL_CACHE_ITEM_POINTER));
				}
			}
			mystats.used_cache_entries_size += sizeof(POOL_CACHE_BLOCK_HEADER);
			/* total size of free(usable) cache entries */
			mystats.free_cache_entries_size += bh->free_bytes;
		}
		else
		{
			mystats.free_cache_entries_size += pool_config->memqcache_cache_block_size;
		}
	}

	/*
	 * Copy POOL_QUERY_CACHE_STATS
	 */
	memcpy(&mystats.cache_stats, stats, sizeof(mystats.cache_stats));

	return &mystats;
}
