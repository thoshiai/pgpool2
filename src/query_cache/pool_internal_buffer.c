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
 * pool_internal_buffer.c: query cache module for internal buffer management
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


static POOL_INTERNAL_BUFFER *pool_create_buffer(void);
static void pool_discard_buffer(POOL_INTERNAL_BUFFER *buffer);
static void pool_add_buffer(POOL_INTERNAL_BUFFER *buffer, void *data, size_t len);

#ifdef NOT_USED
static char *pool_get_buffer_pointer(POOL_INTERNAL_BUFFER *buffer);
#endif
static size_t pool_get_buffer_length(POOL_INTERNAL_BUFFER *buffer);

/*
 * Reset SELECT data buffers
 */
void pool_reset_memqcache_buffer(void)
{
	POOL_SESSION_CONTEXT * session_context;

	session_context = pool_get_session_context(true);

	if (session_context && session_context->query_cache_array)
	{
		POOL_TEMP_QUERY_CACHE *cache;

		ereport(DEBUG1,
			(errmsg("memcache reset buffer"),
				 errdetail("discard: %p", session_context->query_cache_array)));

		pool_discard_query_cache_array(session_context->query_cache_array);
		session_context->query_cache_array = pool_create_query_cache_array();

		ereport(DEBUG1,
			(errmsg("memcache reset buffer"),
				 errdetail("create: %p", session_context->query_cache_array)));
		/*
		 * if the query context is still under use, we cannot discard
		 * temporary cache.
		 */
		if (can_query_context_destroy(session_context->query_context))
		{
			ereport(DEBUG1,
				(errmsg("memcache reset buffer"),
					errdetail("discard temp buffer of %p (%s)",
						   session_context->query_context, session_context->query_context->original_query)));

			cache = pool_get_current_cache();
			pool_discard_temp_query_cache(cache);
			/*
			 * Reset temp_cache pointer in the current query context
			 * so that we don't double free memory.
			 */
			session_context->query_context->temp_cache = NULL;
		}
	}
	pool_discard_dml_table_oid();
	pool_tmp_stats_reset_num_selects();
}

/*
 * SELECT query result array modules
 */
POOL_QUERY_CACHE_ARRAY *pool_create_query_cache_array(void)
{
#define POOL_QUERY_CACHE_ARRAY_ALLOCATE_NUM 128
#define POOL_QUERY_CACHE_ARRAY_HEADER_SIZE (sizeof(int)+sizeof(int))

	size_t size;
	POOL_QUERY_CACHE_ARRAY *p;
	POOL_SESSION_CONTEXT *session_context =pool_get_session_context(false);
	MemoryContext old_context = MemoryContextSwitchTo(session_context->memory_context);

	size = POOL_QUERY_CACHE_ARRAY_HEADER_SIZE + POOL_QUERY_CACHE_ARRAY_ALLOCATE_NUM *
		sizeof(POOL_TEMP_QUERY_CACHE *);
	p = palloc(size);
	p->num_caches = 0;
	p->array_size = POOL_QUERY_CACHE_ARRAY_ALLOCATE_NUM;
    MemoryContextSwitchTo(old_context);
	return p;
}

/*
 * Discard query cache array
 */
void pool_discard_query_cache_array(POOL_QUERY_CACHE_ARRAY *cache_array)
{
	int i;

	if (!cache_array)
		return;

	ereport(DEBUG1,
		(errmsg("memcache discarding query cache array"),
			 errdetail("num_caches: %d", cache_array->num_caches)));

	for (i=0;i<cache_array->num_caches;i++)
	{
		ereport(DEBUG2,
			(errmsg("memcache discarding query cache array"),
				 errdetail("cache no: %d cache: %p", i, cache_array->caches[i])));
		pool_discard_temp_query_cache(cache_array->caches[i]);
	}
	pfree(cache_array);
}

/*
 * Add query cache array
 */
POOL_QUERY_CACHE_ARRAY * pool_add_query_cache_array(POOL_QUERY_CACHE_ARRAY *cache_array, POOL_TEMP_QUERY_CACHE *cache)
{
	size_t size;
	POOL_QUERY_CACHE_ARRAY *cp = cache_array;

	if (!cache_array)
		return cp;

	ereport(DEBUG2,
		(errmsg("memcache adding query cache array"),
			 errdetail("num_caches: %d cache: %p", cache_array->num_caches, cache)));
	if (cache_array->num_caches >= 	cache_array->array_size)
	{
		cache_array->array_size += POOL_QUERY_CACHE_ARRAY_ALLOCATE_NUM;
		size = POOL_QUERY_CACHE_ARRAY_HEADER_SIZE + cache_array->array_size *
			sizeof(POOL_TEMP_QUERY_CACHE *);
		cache_array = repalloc(cache_array, size);
	}
	cache_array->caches[cache_array->num_caches++] = cache;
	return cache_array;
}

/*
 * SELECT query result temporary cache modules
 */

/*
 * Create SELECT result temporary cache
 */
POOL_TEMP_QUERY_CACHE *pool_create_temp_query_cache(char *query)
{
	POOL_SESSION_CONTEXT *session_context =pool_get_session_context(false);
    MemoryContext old_context = MemoryContextSwitchTo(session_context->memory_context);
	POOL_TEMP_QUERY_CACHE *p;
	p = palloc(sizeof(*p));
    p->query = pstrdup(query);

    p->buffer = pool_create_buffer();
    p->oids = pool_create_buffer();
    p->num_oids = 0;
    p->is_exceeded = false;
    p->is_discarded = false;

    MemoryContextSwitchTo(old_context);

	ereport(DEBUG1,
			(errmsg("pool_create_temp_query_cache: cache created: %p", p)));

	return p;
}

/*
 * Discard temp query cache
 */
void pool_discard_temp_query_cache(POOL_TEMP_QUERY_CACHE *temp_cache)
{
	if (!temp_cache)
		return;

	if (temp_cache->query)
		pfree(temp_cache->query);
	if (temp_cache->buffer)
		pool_discard_buffer(temp_cache->buffer);
	if (temp_cache->oids)
		pool_discard_buffer(temp_cache->oids);
	pfree(temp_cache);

	ereport(DEBUG1,
			(errmsg("pool_discard_temp_query_cache: cache discarded: %p", temp_cache)));
}

/*
 * Add data to temp query cache.
 * Data must be FE/BE protocol packet.
 */
void pool_add_temp_query_cache(POOL_TEMP_QUERY_CACHE *temp_cache, char kind, char *data, int data_len)
{
	POOL_INTERNAL_BUFFER *buffer;
	size_t buflen;
	int send_len;

	if (temp_cache == NULL)
	{
		/* This could happen if cache exceeded in previous query
		 * execution in the same unnamed portal.
		 */
		ereport(DEBUG1,
			(errmsg("memcache adding temporary query cache"),
				 errdetail("POOL_TEMP_QUERY_CACHE is NULL")));
		return;
	}

	if (temp_cache->is_exceeded)
	{
		ereport(DEBUG1,
			(errmsg("memcache adding temporary query cache"),
				 errdetail("memqcache_maxcache exceeds")));
		return;
	}

	/*
	 * We only store T(Table Description), D(Data row), C(Command Complete),
	 * 1(ParseComplete), 2(BindComplete)
	 */
    if (kind != 'T' && kind != 'D' && kind != 'C' && kind != '1' && kind != '2')
    {
		return;
	}

	/* Check data limit */
	buffer = temp_cache->buffer;
	buflen = pool_get_buffer_length(buffer);

	if ((buflen+data_len+sizeof(int)+1) > pool_config->memqcache_maxcache)
	{
		ereport(DEBUG1,
			(errmsg("memcache adding temporary query cache"),
				errdetail("data size exceeds memqcache_maxcache. current:%zd requested:%zd memq_maxcache:%d",
					   buflen, data_len+sizeof(int)+1, pool_config->memqcache_maxcache)));
		temp_cache->is_exceeded = true;
		return;
	}

	pool_add_buffer(buffer, &kind, 1);
	send_len = htonl(data_len + sizeof(int));
	pool_add_buffer(buffer, (char *)&send_len, sizeof(int));
	pool_add_buffer(buffer, data, data_len);

	return;
}

/*
 * Add table oids used by SELECT to temp query cache.
 */
void pool_add_oids_temp_query_cache(POOL_TEMP_QUERY_CACHE *temp_cache, int num_oids, int *oids)
{
	POOL_INTERNAL_BUFFER *buffer;

	if (!temp_cache || num_oids <= 0)
		return;

	buffer = temp_cache->oids;
	pool_add_buffer(buffer, oids, num_oids*sizeof(int));
	temp_cache->num_oids = num_oids;
}

/*
 * Internal buffer management modules.
 * Usage:
 * 1) Create buffer using pool_create_buffer().
 * 2) Add data to buffer using pool_add_buffer().
 * 3) Extract (copied) data from buffer using pool_get_buffer().
 * 4) Optionally you can:
 *		Obtain buffer length by using pool_get_buffer_length().
 *		Obtain buffer pointer by using pool_get_buffer_pointer().
 * 5) Discard buffer using pool_discard_buffer().
 */

/*
 * Create and return internal buffer
 */
static POOL_INTERNAL_BUFFER *pool_create_buffer(void)
{
	POOL_INTERNAL_BUFFER *p;
	p = palloc0(sizeof(*p));
	return p;
}

/*
 * Discard internal buffer
 */
static void pool_discard_buffer(POOL_INTERNAL_BUFFER *buffer)
{
	if (buffer)
	{
		if (buffer->buf)
			pfree(buffer->buf);
		pfree(buffer);
	}
}

/*
 * Add data to internal buffer
 */
static void pool_add_buffer(POOL_INTERNAL_BUFFER *buffer, void *data, size_t len)
{
#define POOL_ALLOCATE_UNIT 8192

	/* Sanity check */
	if (!buffer || !data || len == 0)
		return;
	POOL_SESSION_CONTEXT *session_context =pool_get_session_context(false);
	MemoryContext old_context = MemoryContextSwitchTo(session_context->memory_context);

	/* Check if we need to increase the buffer size */
	if ((buffer->buflen + len) > buffer->bufsize)
	{
		size_t allocate_size = ((buffer->buflen + len)/POOL_ALLOCATE_UNIT +1)*POOL_ALLOCATE_UNIT;
		ereport(DEBUG2,
			(errmsg("memcache adding data to internal buffer"),
				errdetail("realloc old size:%zd new size:%zd",
					   buffer->bufsize, allocate_size)));
		buffer->bufsize = allocate_size;
		buffer->buf = (char *)repalloc(buffer->buf, buffer->bufsize);
	}
	/* Add data to buffer */
	memcpy(buffer->buf+buffer->buflen, data, len);
	buffer->buflen += len;
	ereport(DEBUG2,
		(errmsg("memcache adding data to internal buffer"),
			errdetail("len:%zd, total:%zd bufsize:%zd",
				   len, buffer->buflen, buffer->bufsize)));
	MemoryContextSwitchTo(old_context);
	return;
}

/*
 * Get data from internal buffer.
 * Data is stored in newly malloc memory.
 * Data length is returned to len.
 */
void *pool_get_buffer(POOL_INTERNAL_BUFFER *buffer, size_t *len)
{
	void *p;

	if (buffer->bufsize == 0 || buffer->buflen == 0 ||
		buffer->buf == NULL)
	{
		*len = 0;
		return NULL;
	}

	p = palloc(buffer->buflen);
	memcpy(p, buffer->buf, buffer->buflen);
	*len = buffer->buflen;
	return p;
}

/*
 * Get internal buffer length.
 */
static size_t pool_get_buffer_length(POOL_INTERNAL_BUFFER *buffer)
{
	if (buffer == NULL)
		return 0;

	return buffer->buflen;
}

#ifdef NOT_USED
/*
 * Get internal buffer pointer.
 */
static char *pool_get_buffer_pointer(POOL_INTERNAL_BUFFER *buffer)
{
	if (buffer == NULL)
		return NULL;
	return buffer->buf;
}
#endif
/*
 * Get query cache buffer struct of current query context
 */
POOL_TEMP_QUERY_CACHE *pool_get_current_cache(void)
{
	POOL_SESSION_CONTEXT *session_context;
	POOL_QUERY_CONTEXT *query_context;
	POOL_TEMP_QUERY_CACHE *p = NULL;

	session_context = pool_get_session_context(true);
	if (session_context)
	{
		query_context = session_context->query_context;
		if (query_context)
		{
			p = query_context->temp_cache;
		}
	}
	return p;
}

/*
 * Get query cache buffer of current query context
 */
char *pool_get_current_cache_buffer(size_t *len)
{
	char *p = NULL;
	*len = 0;
	POOL_TEMP_QUERY_CACHE *cache;

	cache = pool_get_current_cache();
	if (cache)
	{
		p = pool_get_buffer(cache->buffer, len);
	}
	return p;
}

/*
 * Mark this temporary query cache buffer discarded if the SELECT
 * uses the table oid specified by oids.
 */
void pool_check_and_discard_cache_buffer(int num_oids, int *oids)
{
	POOL_SESSION_CONTEXT *session_context;
	POOL_TEMP_QUERY_CACHE *cache;
	int num_caches;
	size_t len;
	int *soids;
	int i, j, k;

	session_context = pool_get_session_context(true);

	if (!session_context || !session_context->query_cache_array)
		return;

	num_caches = session_context->query_cache_array->num_caches;

	for (i=0;i<num_caches;i++)
	{
		cache = session_context->query_cache_array->caches[i];
		if (!cache || cache->is_discarded)
			continue;

		soids = (int *)pool_get_buffer(cache->oids, &len);
		if (!soids || !len)
			continue;

		for(j=0;j<cache->num_oids;j++)
		{
			if (cache->is_discarded)
				break;

			for (k=0;k<num_oids;k++)
			{
				if (soids[j] == oids[k])
				{
					ereport(DEBUG1,
							(errmsg("discard cache for \"%s\"",cache->query)));
					cache->is_discarded = true;
					break;
				}
			}
		}
		pfree(soids);
	}
}
