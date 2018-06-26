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
 * pool_memqcache.c: query cache on shmem or memcached
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

static char* encode_key(const char *s, char *buf, POOL_CONNECTION_POOL *backend);
#ifdef DEBUG
static void dump_cache_data(const char *data, size_t len);
#endif
static int pool_commit_cache(POOL_CONNECTION_POOL *backend, char *query, char *data, size_t datalen, int num_oids, int *oids);
static int pool_fetch_cache(POOL_CONNECTION_POOL *backend, const char *query, char **buf, size_t *len);
static int send_cached_messages(POOL_CONNECTION *frontend, const char *qcache, int qcachelen);
static void send_message(POOL_CONNECTION *conn, char kind, int len, const char *data);

static void inject_cached_message(POOL_CONNECTION *backend, char *qcache, int qcachelen);

/*
 * Connect to Memcached
 */
int memcached_connect(void)
{
	char *memqcache_memcached_host;
	int memqcache_memcached_port;
#ifdef USE_MEMCACHED
	memcached_server_st *servers;
	memcached_return rc;

	/* Already connected? */
	if (memc)
	{
		return 0;
	}
#endif

	memqcache_memcached_host = pool_config->memqcache_memcached_host;
	memqcache_memcached_port = pool_config->memqcache_memcached_port;

	ereport(DEBUG1,
            (errmsg("connecting to memcached on Host:\"%s:%d\"", memqcache_memcached_host,memqcache_memcached_port)));

#ifdef USE_MEMCACHED
	memc = memcached_create(NULL);
	servers = memcached_server_list_append(NULL,
										   memqcache_memcached_host,
										   memqcache_memcached_port,
										   &rc);

	rc = memcached_server_push(memc, servers);
	if (rc != MEMCACHED_SUCCESS)
	{
		ereport(WARNING,
			(errmsg("failed to connect to memcached, server push error:\"%s\"\n", memcached_strerror(memc, rc))));
		memc = (memcached_st *)-1;
		return -1;
	}
	memcached_server_list_free(servers);
#else
	ereport(WARNING,
			(errmsg("failed to connect to memcached, memcached support is not enabled")));
	return -1;
#endif
	return 0;
}

/*
 * Disconnect to Memcached
 */
void memcached_disconnect (void)
{
#ifdef USE_MEMCACHED
	if (!memc)
	{
		return;
	}
	memcached_free(memc);
#else
	ereport(WARNING,
			(errmsg("failed to disconnect from memcached, memcached support is not enabled")));
#endif
}

/*
 * Register buffer data for query cache in memory cache
 */
void memqcache_register(char kind,
                        POOL_CONNECTION *frontend,
                        char *data,
                        int data_len)
{
	POOL_TEMP_QUERY_CACHE *cache;
	POOL_SESSION_CONTEXT *session_context;
	POOL_QUERY_CONTEXT *query_context;

	cache = pool_get_current_cache();

	if (cache == NULL)
	{
		session_context = pool_get_session_context(true);

		if (session_context && pool_is_query_in_progress())
		{
			char *query = pool_get_query_string();
			query_context = session_context->query_context;

			if (query)
				query_context->temp_cache = pool_create_temp_query_cache(query);
		}
	}

	pool_add_temp_query_cache(cache, kind, data, data_len);
}

/*
 * Commit SELECT results to cache storage.
 */
static int pool_commit_cache(POOL_CONNECTION_POOL *backend, char *query, char *data, size_t datalen, int num_oids, int *oids)
{
#ifdef USE_MEMCACHED
	memcached_return rc;
#endif
	POOL_CACHEKEY cachekey;
	char tmpkey[MAX_KEY];
	time_t memqcache_expire;

	/*
	 * get_buflen() will return -1 if query result exceeds memqcache_maxcache
	 */
	if (datalen == -1)
	{
		return -1;
	}

	/* query disabled */
	if (strlen(query) <= 0)
	{
		return -1;
	}
	ereport(DEBUG1,
		(errmsg("commiting SELECT results to cache storage"),
			 errdetail("Query=\"%s\"", query)));

#ifdef DEBUG
	dump_cache_data(data, datalen);
#endif

	/* encode md5key for memcached */
	encode_key(query, tmpkey, backend);
	ereport(DEBUG2,
		(errmsg("commiting SELECT results to cache storage"),
			 errdetail("search key : \"%s\"", tmpkey)));

	memcpy(cachekey.hashkey, tmpkey, 32);

	memqcache_expire = pool_config->memqcache_expire;
	ereport(DEBUG1,
		(errmsg("commiting SELECT results to cache storage"),
			 errdetail("memqcache_expire = %ld", memqcache_expire)));

	if (pool_is_shmem_cache())
	{
		POOL_CACHEID *cacheid;
		POOL_QUERY_HASH query_hash;

		memcpy(query_hash.query_hash, tmpkey, sizeof(query_hash.query_hash));

		cacheid = pool_hash_search(&query_hash);

		if (cacheid != NULL)
		{
			ereport(DEBUG1,
				(errmsg("commiting SELECT results to cache storage"),
					 errdetail("item already exists")));

			return 0;
		}
		else
		{
			cacheid = pool_add_item_shmem_cache(&query_hash, data, datalen);
			if (cacheid == NULL)
			{
				ereport(LOG,
						(errmsg("failed to add item to shmem cache")));
				return -1;
			}
			else
			{
				ereport(DEBUG2,
					(errmsg("commiting SELECT results to cache storage"),
						errdetail("blockid: %d itemid: %d",
							   cacheid->blockid, cacheid->itemid)));
			}
			cachekey.cacheid.blockid = cacheid->blockid;
			cachekey.cacheid.itemid = cacheid->itemid;
		}
	}

#ifdef USE_MEMCACHED
	else
	{
		rc = memcached_set(memc, tmpkey, 32,
						   data, datalen, (time_t)memqcache_expire, 0);
		if (rc != MEMCACHED_SUCCESS)
		{
			ereport(WARNING,
					(errmsg("cache commit failed with error:\"%s\"",memcached_strerror(memc, rc))));
			return -1;
		}
		ereport(DEBUG1,
			(errmsg("commiting SELECT results to cache storage"),
				 errdetail("set cache succeeded")));
	}
#endif

	/*
	 * Register cache id to oid map
	 */
	pool_add_table_oid_map(&cachekey, num_oids, oids);

	return 0;
}

/*
 * Fetch from memory cache.
 * Return:
 * 0: fetch success, 
 * 1: not found
 */
static int pool_fetch_cache(POOL_CONNECTION_POOL *backend, const char *query, char **buf, size_t *len)
{
	char *ptr;
	char tmpkey[MAX_KEY];
	int sts;
	char *p;

	if (strlen(query) <= 0)
		ereport(ERROR,
			(errmsg("fetching from cache storage, no query")));

	/* encode md5key for memcached */
	encode_key(query, tmpkey, backend);
	ereport(DEBUG1,
		(errmsg("fetching from cache storage"),
			 errdetail("search key \"%s\"", tmpkey)));


	if (pool_is_shmem_cache())
	{
		POOL_QUERY_HASH query_hash;
		int mylen;

		memcpy(query_hash.query_hash, tmpkey, sizeof(query_hash.query_hash));

		ptr = pool_get_item_shmem_cache(&query_hash, &mylen, &sts);
		if (ptr == NULL)
		{
			ereport(DEBUG1,
				(errmsg("fetching from cache storage"),
					 errdetail("cache not found on shared memory")));

			return 1;
		}
		*len = mylen;
	}
#ifdef USE_MEMCACHED
	else
	{
		memcached_return rc;
		unsigned int flags;

		ptr = memcached_get(memc, tmpkey, strlen(tmpkey), len, &flags, &rc);

		if (rc != MEMCACHED_SUCCESS)
		{
			if (rc != MEMCACHED_NOTFOUND)
			{
				ereport(LOG,
					(errmsg("fetching from cache storage, memcached_get failed with error: \"%s\"", memcached_strerror(memc, rc))));
				/*
				 * Turn off memory cache support to prevent future errors.
				 */
				pool_config->memory_cache_enabled = 0;
				/* Behave as if cache not found */
				return 1;
			}
			else
			{
				/* Not found */
				ereport(DEBUG1,
					(errmsg("fetching from cache storage"),
						 errdetail("cache item not found for key: \"%s\" and query:\"%s\"",tmpkey,query)));
				return 1;
			}
		}
	}
#else
	else
	{
		ereport(ERROR,
			(errmsg("memcached support is not enabled")));
	}
#endif

	p = palloc(*len);

	memcpy(p, ptr, *len);

	if (!pool_is_shmem_cache())
	{
		free(ptr);
	}

	ereport(DEBUG1,
		(errmsg("fetching from cache storage"),
			 errdetail("query=\"%s\" len:%zd", query, *len)));
#ifdef DEBUG
	dump_cache_data(p, *len);
#endif

	*buf = p;

	return 0;
}

/*
 * encode key.
 * create cache key as md5(username + query string + database name)
 */
static char* encode_key(const char *s, char *buf, POOL_CONNECTION_POOL *backend)
{
	char* strkey;
	int u_length;
	int d_length;
	int q_length;
	int length;

	u_length = strlen(backend->info->user);
	ereport(DEBUG1,
		(errmsg("memcache encode key"),
			 errdetail("username: \"%s\" database_name: \"%s\"", backend->info->user,backend->info->database)));

	d_length = strlen(backend->info->database);

	q_length = strlen(s);
	ereport(DEBUG1,
		(errmsg("memcache encode key"),
			 errdetail("query: \"%s\"", s)));

	length = u_length + d_length + q_length + 1;

	strkey = (char*)palloc(sizeof(char) * length);

	snprintf(strkey, length, "%s%s%s", backend->info->user, s, backend->info->database);

	pool_md5_hash(strkey, strlen(strkey), buf);
	ereport(DEBUG1,
		(errmsg("memcache encode key"),
			 errdetail("`%s' -> `%s'", strkey, buf)));
	pfree(strkey);
	return buf;
}

#ifdef DEBUG
/*
 * dump cache data
 */
static void dump_cache_data(const char *data, size_t len)
{
	int i;
	int plen;
	

	fprintf(stderr,"shmem: len = %zd\n", len);

	while (len > 0)
	{
		fprintf(stderr,"shmem: kind:%c\n", *data++);
		len--;
		memmove(&plen, data, 4);
		len -= 4;
		data += 4;
		plen = ntohl(plen);
		fprintf(stderr,"shmem: len:%d\n", plen);
		plen -= 4;

		fprintf(stderr, "shmem: ");
		for (i=0;i<plen;i++)
		{
			fprintf(stderr, "%02x ", (unsigned char)(*data++));
			len--;
		}
		fprintf(stderr, "\n");
	}
}
#endif

/*
 * send cached messages
 */
static int send_cached_messages(POOL_CONNECTION *frontend, const char *qcache, int qcachelen)
{
	int msg = 0;
	int i = 0;
	int is_prepared_stmt = 0;
	int len;
	const char *p;

	while (i < qcachelen)
	{
		char tmpkind;
		int tmplen;

		tmpkind = qcache[i];
		i++;

		memcpy(&tmplen, qcache+i, sizeof(tmplen));
		i += sizeof(tmplen);
		len = ntohl(tmplen);
		p = qcache + i;
		i += len - sizeof(tmplen);

		/* No need to cache PARSE and BIND responses */
		if (tmpkind == '1' || tmpkind == '2')
		{
			is_prepared_stmt = 1;
			continue;
		}

		/*
		 * In the prepared statement execution, there is no need to send
		 * 'T' response to the frontend.
		 */
		if (is_prepared_stmt && tmpkind == 'T')
		{
			continue;
		}

		/* send message to frontend */
		ereport(DEBUG1,
			(errmsg("memcache: sending cached messages: '%c' len: %d", tmpkind, len)));
		send_message(frontend, tmpkind, len, p);

		msg++;
	}

	return msg;
}

/*
 * send message to frontend
 */
static void send_message(POOL_CONNECTION *conn, char kind, int len, const char *data)
{
	ereport(DEBUG2,
			(errmsg("memcache: sending messages: kind '%c', len=%d, data=%p", kind, len, data)));

	pool_write(conn, &kind, 1);

	len = htonl(len);
	pool_write(conn, &len, sizeof(len));

	len = ntohl(len);
	pool_write(conn, (void *)data, len-sizeof(len));
}

/*
 * Fetch SELECT data from cache if possible.
 */
POOL_STATUS pool_fetch_from_memory_cache(POOL_CONNECTION *frontend,
										 POOL_CONNECTION_POOL *backend,
										 char *contents, bool *foundp)
{
	char *qcache;
	size_t qcachelen;
	int sts;
	pool_sigset_t oldmask;

	ereport(DEBUG1,
			(errmsg("pool_fetch_from_memory_cache called")));

	*foundp = false;
    
	POOL_SETMASK2(&BlockSig, &oldmask);
	pool_shmem_lock();

#ifdef NOT_USED
	if (!pool_is_shmem_cache())
	{
		lock_memcached();
	}
#endif

    PG_TRY();
    {
        sts = pool_fetch_cache(backend, contents, &qcache, &qcachelen);
    }
    PG_CATCH();
    {
#ifdef NOT_USED
		if (!pool_is_shmem_cache())
		{
			unlock_memcached();
		}
#endif
		POOL_SETMASK(&oldmask);
        PG_RE_THROW();
    }
    PG_END_TRY();

	pool_shmem_unlock();

#ifdef NOT_USED
	if (!pool_is_shmem_cache())
	{
		unlock_memcached();
	}
#endif
	POOL_SETMASK(&oldmask);

	if (sts != 0)
		/* Cache not found */
		return POOL_CONTINUE;

	/*
	 * Cache found. If we are doing extended query and in streaming
	 * replication mode, we need to retrieve any responses from backend and
	 * forward them to frontend.
	 */
	if (pool_is_doing_extended_query_message() && SL_MODE)
	{
		POOL_SESSION_CONTEXT *session_context;
		POOL_CONNECTION *target_backend;

		ereport(DEBUG1,
				(errmsg("memcache: injecting cache data")));

		session_context = pool_get_session_context(true);
		target_backend = CONNECTION(backend, session_context->load_balance_node_id);
		inject_cached_message(target_backend, qcache, qcachelen);
	}
	else
	{
		/*
		 * Send each messages to frontend
		 */
		send_cached_messages(frontend, qcache, qcachelen);
	}

	pfree(qcache);

	/*
	 * Send a "READY FOR QUERY" if not in extended query.
	 */
	if (!pool_is_doing_extended_query_message() && MAJOR(backend) == PROTO_MAJOR_V3)
	{
		signed char state;

		/*
		 * We keep previous transaction state.
		 */
		state = MASTER(backend)->tstate;
		send_message(frontend, 'Z', 5, (char *)&state);
	}

	if (!pool_is_doing_extended_query_message() || !SL_MODE)
	{
		if (pool_flush(frontend))
		{
			return POOL_END;
		}
	}

	*foundp = true;

	if (pool_config->log_per_node_statement)
		ereport(LOG,
				(errmsg("fetch from memory cache"),
				 errdetail("query result fetched from cache. statement: %s", contents)));

	ereport(DEBUG1,
			(errmsg("fetch from memory cache"),
			 errdetail("query result found in the query cache, %s", contents)));

	return POOL_CONTINUE;
}

/*
 * Simple and rough (thus unreliable) check if the query is likely
 * SELECT. Just check if the query starts with SELECT or WITH. This
 * can be used before parse tree is available.
 */
bool pool_is_likely_select(char *query)
{
	bool do_continue = false;

	if (query == NULL)
		return false;

	if (pool_config->ignore_leading_white_space)
	{
		/* Ignore leading white spaces */
		while (*query && isspace(*query))
			query++;
	}
	if (! *query)
	{
		return false;
	}

	/*
	 * Get rid of head comment.
	 * It is sure that the query is in correct format, because the parser
	 * has rejected bad queries such as the one with not-ended comment.
	 */
	while (*query)
	{
		/* Ignore spaces and return marks */
		do_continue = false;
		while (*query && isspace(*query))
		{
			query++;
			do_continue = true;
		}
		if (do_continue)
		{
			continue;
		}

		while (*query && !strncmp(query, "\n", 2))
		{
			query++;
			do_continue = true;
		}
		if (do_continue)
		{
			query += 2;
			continue;
		}

		/* Ignore comments like C */
		if (!strncmp(query, "/*", 2))
		{
			while (*query && strncmp(query, "*/", 2))
				query++;

			query += 2;
			continue;
		}

		/* Ignore SQL comments */
		if (!strncmp(query, "--", 2))
		{
			while (*query && strncmp(query, "\n", 2))
				query++;

			query += 2;
			continue;
		}

		if (!strncasecmp(query, "SELECT", 6) || !strncasecmp(query, "WITH", 4))
		{
			return true;
		}

		query++;
	}

	return false;
}

/*
 * Return true if SELECT can be cached.  "node" is the parse tree for
 * the query and "query" is the query string.
 * The query must be SELECT or WITH.
 */
bool pool_is_allow_to_cache(Node *node, char *query)
{
	int i = 0;
	int num_oids = -1;
	SelectContext ctx;

	/*
	 * If NO QUERY CACHE comment exists, do not cache.
	 */
	if (!strncasecmp(query, NO_QUERY_CACHE, NO_QUERY_CACHE_COMMENT_SZ))
		return false;
	/*
	 * Check black table list first.
	 */
	if (pool_config->num_black_memqcache_table_list > 0)
	{
		/* Extract oids in from clause of SELECT, and check if SELECT to them could be cached. */
		num_oids = pool_extract_table_oids_from_select_stmt(node, &ctx);
		if (num_oids > 0)
		{
			for (i = 0; i < num_oids; i++)
			{
				ereport(DEBUG1,
						(errmsg("memcache: checking if node is allowed to cache: check table_names[%d] = \"%s\"", i, ctx.table_names[i])));
				if (pool_is_table_in_black_list(ctx.table_names[i]) == true)
				{
					ereport(DEBUG1,
							(errmsg("memcache: node is not allowed to cache")));
					return false;
				}
			}
		}
	}

	/* SELECT INTO or SELECT FOR SHARE or UPDATE cannot be cached */
	if (pool_has_insertinto_or_locking_clause(node))
		return false;

	/*
	 * If SELECT uses non immutable functions, it's not allowed to
	 * cache.
	 */
	if (pool_has_non_immutable_function_call(node))
		return false;

	/*
	 * If SELECT uses temporary tables it's not allowed to cache.
	 */
	if (pool_config->check_temp_table && pool_has_temp_table(node))
		return false;

	/*
	 * If SELECT uses system catalogs, it's not allowed to cache.
	 */
	if (pool_has_system_catalog(node))
		return false;

	/*
	 * TABLESAMPLE is not allowed to cache.
	 */
	if (IsA(node, SelectStmt) && ((SelectStmt *)node)->fromClause)
	{
		List *tbl_list = ((SelectStmt *)node)->fromClause;
		ListCell   *tbl;
		foreach(tbl, tbl_list)
		{
			if (IsA(lfirst(tbl), RangeTableSample))
				return false;
		}
	}


	/*
	 * If the table is in the while list, allow to cache even if it is
	 * VIEW or unlogged table.
	 */
	if (pool_config->num_white_memqcache_table_list > 0)
	{
		if (num_oids < 0)
			num_oids = pool_extract_table_oids_from_select_stmt(node, &ctx);

		if (num_oids > 0)
		{
			for (i = 0; i < num_oids; i++)
			{
				char *table = ctx.table_names[i];
				ereport(DEBUG1,
						(errmsg("memcache: checking if node is allowed to cache: check table_names[%d] = \"%s\"", i, table)));
				if (is_view(table) || is_unlogged_table(table))
				{
					if (pool_is_table_in_white_list(table) == false)
					{
						ereport(DEBUG1,
								(errmsg("memcache: node is not allowed to cache")));
						return false;
					}
				}
			}
		}
	}
	else
	{
		/*
		 * If SELECT uses views, it's not allowed to cache.
		 */
		if (pool_has_view(node))
			return false;

		/*
		 * If SELECT uses unlogged tables, it's not allowed to cache.
		 */
		if (pool_has_unlogged_table(node))
			return false;
	}
	return true;
}


/*
 * Return true If the SELECTed table is in back list.
 */
bool pool_is_table_in_black_list(const char *table_name)
{

	if (pool_config->num_black_memqcache_table_list > 0 &&
	    pattern_compare((char *)table_name, BLACKLIST, "black_memqcache_table_list") == 1)
	{
		return true;
	}

	return false;
}

/*
 * Return true If the SELECTed table is in white list.
 */
bool pool_is_table_in_white_list(const char *table_name)
{
	if (pool_config->num_white_memqcache_table_list > 0 &&
		pattern_compare((char *)table_name, WHITELIST, "white_memqcache_table_list") == 1)
	{
		return true;
	}

	return false;
}

/*
 * Return true if memory cache method is "shmem".  The purpose of this
 * function is to cache the result of stcmp and to save a few cycle.
 */
bool pool_is_shmem_cache(void)
{
	return (pool_config->memqcache_method == SHMEM_CACHE)?true:false;
}

/*
 * At Ready for Query or Comand Complete handle query cache.  For streaming
 * replication mode and extended query at Comand Complete handle query cache.
 * For other case At Ready for Query handle query cache.
 */
void pool_handle_query_cache(POOL_CONNECTION_POOL *backend, char *query, Node *node, char state)
{
	POOL_SESSION_CONTEXT *session_context;
	pool_sigset_t oldmask;
	char *cache_buffer;
	size_t len;
	int num_oids;
	int *oids;
	int i;

	session_context = pool_get_session_context(true);

	/* Ok to cache SELECT result? */
	if (pool_is_cache_safe())
	{
		SelectContext ctx;
		MemoryContext old_context;
		old_context = MemoryContextSwitchTo(session_context->memory_context);
		num_oids = pool_extract_table_oids_from_select_stmt(node, &ctx);
		MemoryContextSwitchTo(old_context);
		oids = ctx.table_oids;
		ereport(DEBUG2,
			(errmsg("query cache handler for ReadyForQuery"),
				 errdetail("num_oids: %d oid: %d", num_oids, *oids)));

		if (state == 'I')		/* Not inside a transaction? */
		{
			/*
			 * Make sure that temporary cache is not exceeded.
			 */
			if (!pool_is_cache_exceeded())
			{
				POOL_TEMP_QUERY_CACHE *cache;
				/*
				 * If we are not inside a transaction, we can
				 * immediately register to cache storage.
				 */
				/* Register to memcached or shmem */
				POOL_SETMASK2(&BlockSig, &oldmask);
				pool_shmem_lock();				

				if (!pool_is_shmem_cache())
				{
					lock_memcached();
				}

				cache_buffer =  pool_get_current_cache_buffer(&len);
				if (cache_buffer)
				{
					if (session_context->query_context->skip_cache_commit == false)
					{
						if (pool_commit_cache(backend, query, cache_buffer, len, num_oids, oids) != 0)
						{
							ereport(WARNING,
									(errmsg("ReadyForQuery: pool_commit_cache failed")));
						}
					}
					/*
					 * Reset temporary query cache buffer. This is
					 * necessary if extended query protocol is used and a
					 * bind/execute message arrives which uses a statement
					 * created by prior parse message. In this case since
					 * the temp_cache is not initialized by a parse
					 * message, messages are added to pre existing temp
					 * cache buffer. The problem was found in bug#152.
					 * http://www.pgpool.net/mantisbt/view.php?id=152
					 */
					cache = pool_get_current_cache();
					ereport(DEBUG1,
							(errmsg("pool_handle_query_cache: temp_cache: %p", cache)));
					pool_discard_temp_query_cache(cache);

					if (SL_MODE && pool_is_doing_extended_query_message())
						session_context->query_context->temp_cache = NULL;
					else
						session_context->query_context->temp_cache = pool_create_temp_query_cache(query);
					pfree(cache_buffer);
				}
				pool_shmem_unlock();
				if (!pool_is_shmem_cache())
				{
					unlock_memcached();
				}
				POOL_SETMASK(&oldmask);
			}

			/* Count up SELECT stats */
			pool_stats_count_up_num_selects(1);

			/* Reset temp buffer */
			pool_reset_memqcache_buffer();
		}
		else
		{
			POOL_TEMP_QUERY_CACHE *cache = pool_get_current_cache();

			/* In transaction. Keep to temp query cache array */
			pool_add_oids_temp_query_cache(cache, num_oids, oids);

			/* 
			 * If temp cache has been overflowed, just trash the half
			 * baked temp cache.
			 */
			if (pool_is_cache_exceeded())
			{
				POOL_TEMP_QUERY_CACHE *cache;

				cache = pool_get_current_cache();
				pool_discard_temp_query_cache(cache);
				/*
				 * Reset temp_cache pointer in the current query context
				 * so that we don't double free memory.
				 */
				session_context->query_context->temp_cache = NULL;

			}
			/*
			 * Otherwise add to the temp cache array.
			 */
			else
			{
				session_context->query_cache_array = 
					pool_add_query_cache_array(session_context->query_cache_array, cache);
				/*
				 * Reset temp_cache pointer in the current query
				 * context so that we don't add the same temp cache to
				 * the cache array. This is necessary such that case
				 * when next query is just a "bind message", without
				 * "parse message". In the case the query context is
				 * reused and same cache pointer will be added to the
				 * query_cache_array which we do not want.
				 */
				session_context->query_context->temp_cache = NULL;
			}

			/* Count up temporary SELECT stats */
			pool_tmp_stats_count_up_num_selects();
		}
	}
	else if (is_rollback_query(node))	/* Rollback? */
	{
		/* Discard buffered data */
		pool_reset_memqcache_buffer();
	}
	else if (is_commit_query(node))		/* Commit? */
	{
		int num_caches;

		POOL_SETMASK2(&BlockSig, &oldmask);
		pool_shmem_lock();

		if (!pool_is_shmem_cache())
		{
			lock_memcached();
		}

		/* Invalidate query cache */
		if (pool_config->memqcache_auto_cache_invalidation)
		{
			num_oids = pool_get_dml_table_oid(&oids);
			pool_invalidate_query_cache(num_oids, oids, true, 0);
		}

		/*
		 * If we have something in the query cache buffer, that means
		 * either:
		 * - We only had SELECTs in the transaction
		 * - We had only SELECTs after last DML
		 * Thus we can register SELECT results to cache storage.
		 */
		num_caches = session_context->query_cache_array->num_caches;
		for (i=0;i<num_caches;i++)
		{
			POOL_TEMP_QUERY_CACHE *cache;

			cache = session_context->query_cache_array->caches[i];
			if (!cache || cache->is_discarded)
				continue;

			num_oids = cache->num_oids;
			oids = pool_get_buffer(cache->oids, &len);
			cache_buffer = pool_get_buffer(cache->buffer, &len);
						
			if (pool_commit_cache(backend, cache->query, cache_buffer, len, num_oids, oids) != 0)
			{
				ereport(WARNING,
						(errmsg("ReadyForQuery: pool_commit_cache failed")));
			}
			if (oids)
				pfree(oids);
			if (cache_buffer)
				pfree(cache_buffer);
		}
		pool_shmem_unlock();
		POOL_SETMASK(&oldmask);

		/* Count up number of SELECT stats */
		pool_stats_count_up_num_selects(pool_tmp_stats_get_num_selects());

		pool_reset_memqcache_buffer();
	}
	else		/* Non cache safe queries */
	{
		/* Non cachable SELECT */
		if (node && IsA(node, SelectStmt))
		{
			if (state == 'I')
			{
				/* Count up SELECT stats */
				pool_stats_count_up_num_selects(1);
				pool_reset_memqcache_buffer();
			}
			else
			{
				/* Count up temporary SELECT stats */
				pool_tmp_stats_count_up_num_selects();
			}
		}
		/*
		 * If the query is DROP DATABASE, discard both of caches in shmem/memcached and
		 * oidmap in memqcache_oiddir.
		 */
		else if (is_drop_database(node) && session_context->query_context->dboid != 0)
		{
			int dboid = session_context->query_context->dboid;
			num_oids = pool_get_dropdb_table_oids(&oids, dboid);

			if (num_oids > 0 && pool_config->memqcache_auto_cache_invalidation)
			{
				pool_shmem_lock();
				if (!pool_is_shmem_cache())
				{
					lock_memcached();
				}
				pool_invalidate_query_cache(num_oids, oids, true, dboid);
				pool_discard_oid_maps_by_db(dboid);
				pool_shmem_unlock();
				if (!pool_is_shmem_cache())
				{
					unlock_memcached();
				}
				pool_reset_memqcache_buffer();
				pfree(oids);
				ereport(DEBUG2,
					(errmsg("query cache handler for ReadyForQuery"),
						 errdetail("deleted all cache files for the DROPped DB")));
			}
		}
		else
		{
			/*
			 * DML/DCL/DDL case
			 */

			/* Extract table oids from buffer */
			num_oids = pool_get_dml_table_oid(&oids);
			if (num_oids > 0 && pool_config->memqcache_auto_cache_invalidation)
			{
				/*
				 * If we are not inside a transaction, we can
				 * immediately invalidate query cache.
				 */
				if (state == 'I')
				{
					POOL_SETMASK2(&BlockSig, &oldmask);
					pool_shmem_lock();
					if (!pool_is_shmem_cache())
					{
						lock_memcached();
					}
					pool_invalidate_query_cache(num_oids, oids, true, 0);
					if (!pool_is_shmem_cache())
					{
						unlock_memcached();
					}
					pool_shmem_unlock();
					POOL_SETMASK(&oldmask);
					pool_reset_memqcache_buffer();
				}
				else
				{
					/*
					 * If we are inside a transaction, we
					 * cannot invalidate query cache
					 * yet. However we can clear cache buffer,
					 * if DML/DDL modifies the TABLE which SELECT uses.
					 */
					pool_check_and_discard_cache_buffer(num_oids, oids);
				} 
			}
			else if (num_oids == 0)
			{
				/* 
				 * It is also necessary to clear cache buffers in case of
				 * no oid queries (like BEGIN, CHECKPOINT, VACUUM, etc) too.
				 */
				pool_reset_memqcache_buffer();
			}
		}
	}
}

/*
 * Create and initialize query cache stats
 */
POOL_QUERY_CACHE_STATS *stats;
int pool_init_memqcache_stats(void)
{
	stats = pool_shared_memory_create(sizeof(POOL_QUERY_CACHE_STATS));
	pool_reset_memqcache_stats();
	return 0;
}

/*
 * Returns copy of stats area. The copy is in static area and will be
 * overwritten by next call to this function.
 */
POOL_QUERY_CACHE_STATS *pool_get_memqcache_stats(void)
{
	static POOL_QUERY_CACHE_STATS mystats;
	pool_sigset_t oldmask;

	memset(&mystats, 0, sizeof(POOL_QUERY_CACHE_STATS));

	if (stats)
	{
		POOL_SETMASK2(&BlockSig, &oldmask);
		pool_semaphore_lock(QUERY_CACHE_STATS_SEM);
		memcpy(&mystats, stats, sizeof(POOL_QUERY_CACHE_STATS));
		pool_semaphore_unlock(QUERY_CACHE_STATS_SEM);
		POOL_SETMASK(&oldmask);
	}

	return &mystats;
}

/*
 * Reset query cache stats. Caller must lock QUERY_CACHE_STATS_SEM if
 * necessary.
 */
void pool_reset_memqcache_stats(void)
{
	memset(stats, 0, sizeof(POOL_QUERY_CACHE_STATS));
	stats->start_time = time(NULL);
}

/*
 * Count up number of successful SELECTs and returns the number.
 * QUERY_CACHE_STATS_SEM lock is acquired in this function.
 */
long long int pool_stats_count_up_num_selects(long long int num)
{
	pool_sigset_t oldmask;

	POOL_SETMASK2(&BlockSig, &oldmask);
	pool_semaphore_lock(QUERY_CACHE_STATS_SEM);
	stats->num_selects += num;
	pool_semaphore_unlock(QUERY_CACHE_STATS_SEM);
	POOL_SETMASK(&oldmask);
	return stats->num_selects;
}

/*
 * Count up number of successful SELECTs in temporary area and returns
 * the number.
 */
long long int pool_tmp_stats_count_up_num_selects(void)
{
	POOL_SESSION_CONTEXT *session_context;

	session_context = pool_get_session_context(false);
	session_context->num_selects++;
	return 	session_context->num_selects;
}

/*
 * Return number of successful SELECTs in temporary area.
 */
long long int pool_tmp_stats_get_num_selects(void)
{
	POOL_SESSION_CONTEXT *session_context;

	session_context = pool_get_session_context(false);
	return session_context->num_selects;
}

/*
 * Reset number of successful SELECTs in temporary area.
 */
void pool_tmp_stats_reset_num_selects(void)
{
	POOL_SESSION_CONTEXT *session_context;

	session_context = pool_get_session_context(false);
	session_context->num_selects = 0;
}

/*
 * Count up number of SELECTs extracted from cache returns the number.
 * QUERY_CACHE_STATS_SEM lock is acquired in this function.
 */
long long int pool_stats_count_up_num_cache_hits(void)
{
	pool_sigset_t oldmask;

	POOL_SETMASK2(&BlockSig, &oldmask);
	pool_semaphore_lock(QUERY_CACHE_STATS_SEM);
	stats->num_cache_hits++;
	pool_semaphore_unlock(QUERY_CACHE_STATS_SEM);
	POOL_SETMASK(&oldmask);
	return stats->num_cache_hits;
}

/*
 * Inject cached message to the target backend buffer to pretend as if backend
 * actually replies with Data row and Command Complete message.
 */
static void inject_cached_message(POOL_CONNECTION *backend, char *qcache, int qcachelen)
{
	char kind;
	int len;
	char *buf;
	int timeout;
	int i = 0;
	bool is_prepared_stmt = false;
	POOL_SESSION_CONTEXT *session_context;
	POOL_QUERY_CONTEXT* query_context;
	POOL_PENDING_MESSAGE *msg;

	session_context = pool_get_session_context(false);
	query_context = session_context->query_context;
	msg = pool_pending_message_find_lastest_by_query_context(query_context);

	if (msg)
	{
		/*
		 * If pending message found, we should extract target backend from it
		 */
		int backend_id;

		backend_id = pool_pending_message_get_target_backend_id(msg);
		backend = CONNECTION(session_context->backend, backend_id);
		timeout = -1;
	}
	else
		timeout = 0;

	/* Send flush messsage to backend to retrieve response of backend */
	pool_write(backend, "H", 1);		
	len = htonl(sizeof(len));
	pool_write_and_flush(backend, &len, sizeof(len));

	/*
	 * Push any response from backend
	 */
	for(;;)
	{
		pool_read(backend, &kind, 1);
		ereport(DEBUG1,
				(errmsg("inject_cached_message: push message kind: '%c'", kind)));
		if (msg &&
			((kind == 'T' && msg->type == POOL_DESCRIBE) ||
			 (kind == '2' && msg->type == POOL_BIND)))
		{
			/* Pending message seen. Now it is likely to end of pending data */
			timeout = 0;
		}
		pool_push(backend, &kind, sizeof(kind));
		pool_read(backend, &len, sizeof(len));
		pool_push(backend, &len, sizeof(len));
		if ((ntohl(len)-sizeof(len)) > 0)
		{
			buf = pool_read2(backend, ntohl(len)-sizeof(len));
			pool_push(backend, buf, ntohl(len)-sizeof(len));
		}

		/* check if there's any pending data */
		if (!pool_ssl_pending(backend) && pool_read_buffer_is_empty(backend))
		{
			pool_set_timeout(timeout);
			if (pool_check_fd(backend) != 0)
			{
				ereport(DEBUG1,
						(errmsg("inject_cached_message: select shows no pending data")));
				pool_set_timeout(-1);
				break;
			}
			pool_set_timeout(-1);
		}
	}

	/*
	 * Inject row data and command complete
	 */
	while (i < qcachelen)
	{
		char tmpkind;
		int tmplen;
		char *p;

		tmpkind = qcache[i];
		i++;

		memcpy(&tmplen, qcache+i, sizeof(tmplen));
		i += sizeof(tmplen);
		len = ntohl(tmplen);
		p = qcache + i;
		i += len - sizeof(tmplen);

		/* No need to cache PARSE and BIND responses */
		if (tmpkind == '1' || tmpkind == '2')
		{
			is_prepared_stmt = true;
			continue;
		}

		/*
		 * In the prepared statement execution, there is no need to send
		 * 'T' response to the frontend.
		 */
		if (is_prepared_stmt && tmpkind == 'T')
		{
			continue;
		}

		/* push message */
		ereport(DEBUG1,
			(errmsg("inject_cached_message: push cached messages: '%c' len: %d", tmpkind, len)));
		pool_push(backend, &tmpkind, 1);
		pool_push(backend, &tmplen, sizeof(tmplen));
		if (len > 0)
			pool_push(backend, p, len - sizeof(tmplen));
	}

	/*
	 * Pop data.
	 */
	pool_pop(backend, &len);
}
