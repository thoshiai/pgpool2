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
 * pool_oidmap.c: cached table oid module
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
#include "libmemcached/memcached.h"

extern int pool_get_dml_table_oid(int **oid);
extern int pool_get_dropdb_table_oids(int **oids, int dboid);
extern void pool_discard_dml_table_oid(void);
extern void pool_invalidate_query_cache(int num_table_oids, int *table_oid, bool unlink, int dboid);
static int pool_get_database_oid(void);
extern void pool_add_table_oid_map(POOL_CACHEKEY *cachkey, int num_table_oids, int *table_oids);

#define POOL_OIDBUF_SIZE 1024
#define DATABASE_TO_OID_QUERY "SELECT oid FROM pg_database WHERE datname = '%s'"

#ifdef USE_MEMCACHED
extern memcached_st *memc;
#endif

static int* oidbuf;
static int oidbufp;
static int oidbuf_size;

 /*
 * Extract table oid from INSERT/UPDATE/DELETE/TRUNCATE/
 * DROP TABLE/ALTER TABLE/COPY FROM statement.
 * Returns number of oids.
 * In case of error, returns 0 (InvalidOid).
 * oids buffer (oidsp) will be discarded by subsequent call.
 */
int pool_extract_table_oids(Node *node, int **oidsp)
{
#define POOL_MAX_DML_OIDS 128
	char *table;
	static int oids[POOL_MAX_DML_OIDS];
	int num_oids;
	int oid;

	if (node == NULL)
	{
		ereport(LOG,
				(errmsg("memcache: error while extracting table oids. statement is NULL")));
		return 0;
	}

	num_oids = 0;
	*oidsp = oids;

	if (IsA(node, InsertStmt))
	{
		InsertStmt *stmt = (InsertStmt *)node;
		table = make_table_name_from_rangevar(stmt->relation);
	}
	else if (IsA(node, UpdateStmt))
	{
		UpdateStmt *stmt = (UpdateStmt *)node;
		table = make_table_name_from_rangevar(stmt->relation);
	}
	else if (IsA(node, DeleteStmt))
	{
		DeleteStmt *stmt = (DeleteStmt *)node;
		table = make_table_name_from_rangevar(stmt->relation);
	}

#ifdef NOT_USED
	/*
	 * We do not handle CREATE TABLE here.  It is possible that
	 * pool_extract_table_oids() is called before CREATE TABLE gets
	 * executed.
	 */
	else if (IsA(node, CreateStmt))
	{
		CreateStmt *stmt = (CreateStmt *)node;
		table = make_table_name_from_rangevar(stmt->relation);
	}
#endif

	else if (IsA(node, AlterTableStmt))
	{
		AlterTableStmt *stmt = (AlterTableStmt *)node;
		table = make_table_name_from_rangevar(stmt->relation);
	}

	else if (IsA(node, CopyStmt))
	{
		CopyStmt *stmt = (CopyStmt *)node;
		if (stmt->is_from)		/* COPY FROM? */
		{
			table = make_table_name_from_rangevar(stmt->relation);
		}
		else
		{
			return 0;
		}
	}

	else if (IsA(node, DropStmt))
	{
		ListCell *cell;

		DropStmt *stmt = (DropStmt *)node;

		if (stmt->removeType != OBJECT_TABLE)
		{
			return 0;
		}

		/* Here, stmt->objects is list of target relation info.  The
		 * first cell of target relation info is a list (possibly)
		 * consists of database, schema and relation.  We need to call
		 * makeRangeVarFromNameList() before passing to
		 * make_table_name_from_rangevar. Otherwise we get weird excessively
		 * decorated relation name (''table_name'').
		 */
		foreach(cell, stmt->objects)
		{
			if (num_oids > POOL_MAX_DML_OIDS)
			{
				ereport(LOG,
						(errmsg("memcache: error while extracting table oids. too many oids:%d", num_oids)));
				return 0;
			}

			table = make_table_name_from_rangevar(makeRangeVarFromNameList(lfirst(cell)));
			oid = pool_table_name_to_oid(table);
			if (oid > 0)
			{
				oids[num_oids++] = pool_table_name_to_oid(table);
				ereport(DEBUG1,
						(errmsg("memcache: extracting table oids: table: \"%s\" oid:%d", table, oids[num_oids-1])));
			}
		}
		return num_oids;
	}
	else if (IsA(node, TruncateStmt))
	{
		ListCell *cell;

		TruncateStmt *stmt = (TruncateStmt *)node;

		foreach(cell, stmt->relations)
		{
			if (num_oids > POOL_MAX_DML_OIDS)
			{
				ereport(LOG,
						(errmsg("memcache: error while extracting table oids. too many oids:%d", num_oids)));
				return 0;
			}

			table = make_table_name_from_rangevar(lfirst(cell));
			oid = pool_table_name_to_oid(table);
			if (oid > 0)
			{
				oids[num_oids++] = pool_table_name_to_oid(table);
				ereport(DEBUG1,
						(errmsg("memcache: extracting table oids: table: \"%s\" oid:%d", table, oids[num_oids-1])));
			}
		}
		return num_oids;
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("memcache: extracting table oids: statment is different from INSERT/UPDATE/DELETE/TRUNCATE/DROP TABLE/ALTER TABLE")));
		return 0;
	}

	oid = pool_table_name_to_oid(table);
	if (oid > 0)
	{
		oids[num_oids++] = pool_table_name_to_oid(table);
		ereport(DEBUG1,
				(errmsg("memcache: extracting table oids: table: \"%s\" oid:%d", table, oid)));
	}
	return num_oids;
}

/*
 * Add table oid to internal buffer
 */
void pool_add_dml_table_oid(int oid)
{
	int i;
	int* tmp;

	if (oid == 0)
		return;

	if (oidbufp >= oidbuf_size)
	{
		MemoryContext oldcxt;
		oidbuf_size += POOL_OIDBUF_SIZE;
		/*
		 * This need to live throughout the life of child so home it in
		 * TopMemoryContext
		 */
		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		tmp = repalloc(oidbuf, sizeof(int) * oidbuf_size);
		MemoryContextSwitchTo(oldcxt);
		if (tmp == NULL)
			return;

		oidbuf = tmp;
	}

	for (i=0;i<oidbufp;i++)
	{
		if (oidbuf[i] == oid)
		/* Already same oid exists */
			return;
	}
	oidbuf[oidbufp++] = oid;
}


/*
 * Get table oid buffer
 */
int pool_get_dml_table_oid(int **oid)
{
	*oid = oidbuf;
	return oidbufp;
}

/*
 * Extract all table oids for specified dboid from oid table.
 * oids: pointer to a palloc'ed oid array
 * size of the array is returned from the function.
 */
int pool_get_dropdb_table_oids(int **oids, int dboid)
{
	int *rtn = 0;
	int oids_size = 0;
	int *tmp;

	int num_oids = 0;
	DIR *dir;
	struct dirent *dp;
	char path[1024];

	snprintf(path, sizeof(path), "%s/%d", pool_config->memqcache_oiddir, dboid);
	if ((dir = opendir(path)) == NULL)
	{
		ereport(DEBUG1,
			(errmsg("memcache: getting drop table oids"),
				 errdetail("Failed to open dir: %s", path)));
		return 0;
	}

	while ((dp = readdir(dir)) != NULL)
	{
		if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0)
			continue;

		if (num_oids >= oids_size)
		{
			oids_size += POOL_OIDBUF_SIZE;
			tmp = repalloc(rtn, sizeof(int) * oids_size);
			if (tmp == NULL)
			{
				closedir(dir);
				return 0;
			}
			rtn = tmp;
		}

		rtn[num_oids] = atol(dp->d_name);
		num_oids++;
	}

	closedir(dir);
	*oids = rtn;

	return num_oids;
}

/* Discard oid internal buffer */
void pool_discard_dml_table_oid(void)
{
	oidbufp = 0;
}

/*
 * Management modules for oid map.  When caching SELECT results, we
 * record table oids to file, which has following structure.
 *
 * memqcache_oiddir -+- database_oid -+-table_oid_file1
 *                                    |
 *                                    +-table_oid_file2
 *                                    |
 *                                    +-table_oid_file3...
 *
 * table_oid_file's name is table oid, which was used by the SELECT
 * statement. The file has 1 or more cacheid(s). When SELECT result is
 * cached, the file is created and cache id is appended. Later SELECT
 * using same table oid will add to the same file. If the SELECT uses
 * multiple tables, multiple table_oid_file will be created. When
 * INSERT/UPDATE/DELETE is executed, corresponding caches must be
 * deleted (cache invalidation) (when DROP TABLE, ALTER TABLE is
 * executed, the caches must be deleted as well). When database is
 * dropped, all caches belonging to the database must be deleted.
 */

/*
 * Get oid of current database
 */
static int pool_get_database_oid(void)
{
/*
 * Query to convert table name to oid
 */
	int oid = 0;
	static POOL_RELCACHE *relcache;
	POOL_CONNECTION_POOL *backend;

	backend = pool_get_session_context(false)->backend;

	/*
	 * If relcache does not exist, create it.
	 */
	if (!relcache)
	{
		relcache = pool_create_relcache(pool_config->relcache_size, DATABASE_TO_OID_QUERY,
										int_register_func, int_unregister_func,
										false);
		if (relcache == NULL)
		{
			ereport(LOG,
				(errmsg("memcache: error creating relcache while getting database OID")));
			return oid;
		}
	}

	/*
	 * Search relcache.
	 */
	oid = (int)(intptr_t)pool_search_relcache(relcache, backend,
											  MASTER_CONNECTION(backend)->sp->database);
	return oid;
}

/*
 * Get oid of current database for discarding cache files
 * after executing DROP DATABASE
 */
int pool_get_database_oid_from_dbname(char *dbname)
{
	int dboid = 0;
	POOL_SELECT_RESULT *res;
	char query[1024];

	POOL_CONNECTION_POOL *backend;
	backend = pool_get_session_context(false)->backend;

	snprintf(query, sizeof(query), DATABASE_TO_OID_QUERY, dbname); 
	do_query(MASTER(backend), query, &res, MAJOR(backend));

	if (res->numrows != 1)
	{
		ereport(DEBUG1,
			(errmsg("memcache: getting oid of current database"),
				 errdetail("received %d rows", res->numrows)));
		free_select_result(res);
		return 0;
	}    

	dboid = atol(res->data[0]);
	free_select_result(res);

	return dboid;
}

/*
 * Add cache id (shmem case) or hash key (memcached case) to table oid
 * map file.  Caller must hold shmem lock before calling this function
 * to avoid file extension conflict among different pgpool child
 * process.
 * As of pgpool-II 3.2, pool_handle_query_cache is responsible for that.
 * (pool_handle_query_cache -> pool_commit_cache -> pool_add_table_oid_map)
 */
void pool_add_table_oid_map(POOL_CACHEKEY *cachekey, int num_table_oids, int *table_oids)
{
	char *dir;
	int dboid;
	char path[1024];
	int i;
	int len;

	/*
	 * Create memqcache_oiddir
	 */
	dir = pool_config->memqcache_oiddir;

	if (mkdir(dir, S_IREAD|S_IWRITE|S_IEXEC) == -1)
	{
		if (errno != EEXIST)
		{
			ereport(WARNING,
				(errmsg("memcache: adding table oid maps, failed to create directory:\"%s\". error:\"%s\"", dir, strerror(errno))));
			return;
		}
	}

	/*
	 * Create memqcache_oiddir/database_oid
	 */
	dboid = pool_get_database_oid();
	ereport(DEBUG1,
		(errmsg("memcache: adding table oid maps"),
			 errdetail("dboid %d", dboid)));

	if (dboid <= 0)
	{
		ereport(WARNING,
				(errmsg("memcache: adding table oid maps, failed to get database OID")));
		return;
	}

	snprintf(path, sizeof(path), "%s/%d", dir, dboid);
	if (mkdir(path, S_IREAD|S_IWRITE|S_IEXEC) == -1)
	{
		if (errno != EEXIST)
		{
			ereport(WARNING,
					(errmsg("memcache: adding table oid maps, failed to create directory:\"%s\". error:\"%s\"", path, strerror(errno))));
			return;
		}
	}

	if (pool_is_shmem_cache())
	{
		len = sizeof(cachekey->cacheid);
	}
	else
	{
		len = sizeof(cachekey->hashkey);
	}

	for (i=0;i<num_table_oids;i++)
	{
		int fd;
		int oid = table_oids[i];
		int sts;
		struct flock fl;

		/*
		 * Create or open each memqcache_oiddir/database_oid/table_oid
		 */
		snprintf(path, sizeof(path), "%s/%d/%d", dir, dboid, oid);
		if ((fd = open(path, O_CREAT|O_RDWR, S_IRUSR|S_IWUSR)) == -1)
		{
			ereport(WARNING,
					(errmsg("memcache: adding table oid maps, failed to open file:\"%s\". error:\"%s\"", path, strerror(errno))));
			return;
		}

		fl.l_type   = F_WRLCK;
		fl.l_whence = SEEK_SET;
		fl.l_start  = 0;        	/* Offset from l_whence         */
		fl.l_len    = 0;        	/* length, 0 = to EOF           */

		sts = fcntl(fd, F_SETLKW, &fl);
		if (sts == -1)
		{
			ereport(WARNING,
					(errmsg("memcache: adding table oid maps, failed to lock file:\"%s\". error:\"%s\"", path, strerror(errno))));

			close(fd);
			return;
		}

		/*
		 * Below was ifdef-out because of a performance reason.
		 * Looking for duplicate cache entries in a file needed
		 * unacceptably high cost. So we gave up this and decided not
		 * to care about duplicate entries in the file.
		 */
#ifdef NOT_USED
		for (;;)
		{
			sts = read(fd, (char *)&buf, len);
			if (sts == -1)
			{
				ereport(WARNING,
						(errmsg("memcache: adding table oid maps, failed to read file:\"%s\". error:\"%s\"", path, strerror(errno))));
				close(fd);
				return;
			}
			else if (sts == len)
			{
				if (memcmp(cachekey, &buf, len) == 0)
				{
					/* Same key found. Skip this */
					close(fd);
					return;
				}
				continue;
			}
			/*
			 * Must be EOF
			 */
			if (sts != 0)
			{
				ereport(WARNING,
						(errmsg("memcache: adding table oid maps, invalid data length:%d in file:\"%s\". error:\"%s\"",sts, path)));
				close(fd);
				return;
			}
			break;
		}
#endif

		if (lseek(fd, 0, SEEK_END) == -1)
		{
			ereport(WARNING,
					(errmsg("memcache: adding table oid maps, failed seek on file:\"%s\". error:\"%s\"", path, strerror(errno))));
			close(fd);
			return;
		}

		/*
		 * Write cache_id or cache key at the end of file
		 */
		sts = write(fd, (char *)cachekey, len);
		if (sts == -1 || sts != len)
		{
			ereport(WARNING,
					(errmsg("memcache: adding table oid maps, failed to write file:\"%s\". error:\"%s\"", path, strerror(errno))));
			close(fd);
			return;
		}
		close(fd);
	}
}

/*
 * Discard all oid maps at pgpool-II startup.
 * This is necessary for shmem case.
 */
void pool_discard_oid_maps(void)
{
	char command[1024];

	snprintf(command, sizeof(command), "/bin/rm -fr %s/[0-9]*",
			 pool_config->memqcache_oiddir);
	if(system(command) == -1)
        ereport(WARNING,
            (errmsg("unable to execute command \"%s\"",command),
             errdetail("system() command failed with error \"%s\"",strerror(errno))));
    

}

/*
 * Discard oid map files by dboid
 */
void pool_discard_oid_maps_by_db(int dboid)
{
	char command[1024];

	if (pool_is_shmem_cache())
	{
		snprintf(command, sizeof(command), "/bin/rm -fr %s/%d/",
				 pool_config->memqcache_oiddir, dboid);

		ereport(DEBUG1,
				(errmsg("memcache: discarding oid maps by db"),
				 errdetail("command: '%s\'", command)));

		if(system(command) == -1)
            ereport(WARNING,
				(errmsg("unable to execute command \"%s\"",command),
                     errdetail("system() command failed with error \"%s\"",strerror(errno))));
	}
}

/*
 * Read cache id (shmem case) or hash key (memcached case) from table
 * oid map file according to table_oids and discard cache entries.  If
 * unlink is true, the file will be unlinked after successful cache
 * removal.
 */
void pool_invalidate_query_cache(int num_table_oids, int *table_oid, bool unlinkp, int dboid)
{
	char *dir;
	char path[1024];
	int i;
	int len;
	POOL_CACHEKEY buf;

	/*
	 * Create memqcache_oiddir
	 */
	dir = pool_config->memqcache_oiddir;
	if (mkdir(dir, S_IREAD|S_IWRITE|S_IEXEC) == -1)
	{
		if (errno != EEXIST)
		{
			ereport(WARNING,
					(errmsg("memcache: invalidating query cache, failed to create directory:\"%s\". error:\"%s\"", dir, strerror(errno))));
			return;
		}
	}

	/*
	 * Create memqcache_oiddir/database_oid
	 */
	if (dboid == 0)
	{
		dboid = pool_get_database_oid();
		ereport(DEBUG1,
			(errmsg("memcache invalidating query cache"),
				 errdetail("dboid %d", dboid)));

		if (dboid <= 0)
		{
			ereport(WARNING,
					(errmsg("memcache: invalidating query cache, could not get database OID")));
			return;
		}
	}

	snprintf(path, sizeof(path), "%s/%d", dir, dboid);
	if (mkdir(path, S_IREAD|S_IWRITE|S_IEXEC) == -1)
	{
		if (errno != EEXIST)
		{
			ereport(WARNING,
					(errmsg("memcache: invalidating query cache, failed to create directory:\"%s\". error:\"%s\"", path, strerror(errno))));
			return;
		}
	}

	if (pool_is_shmem_cache())
	{
		len = sizeof(buf.cacheid);
	}
	else
	{
		len = sizeof(buf.hashkey);
	}

	for (i=0;i<num_table_oids;i++)
	{
		int fd;
		int oid = table_oid[i];
		int sts;
		struct flock fl;

		/*
		 * Open each memqcache_oiddir/database_oid/table_oid
		 */
		snprintf(path, sizeof(path), "%s/%d/%d", dir, dboid, oid);
		if ((fd = open(path, O_RDONLY)) == -1)
		{
			/* This may be normal. It is possible that no SELECT has
			 * been issued since the table has been created or since
			 * pgpool-II started up.
			 */
			ereport(DEBUG1,
				(errmsg("memcache invalidating query cache"),
					errdetail("failed to open \"%s\". reason:\"%s\"",path, strerror(errno))));
			continue;
		}

		fl.l_type   = F_RDLCK;
		fl.l_whence = SEEK_SET;
		fl.l_start  = 0;        	/* Offset from l_whence         */
		fl.l_len    = 0;        	/* length, 0 = to EOF           */

		sts = fcntl(fd, F_SETLKW, &fl);
		if (sts == -1)
		{
			ereport(WARNING,
					(errmsg("memcache: invalidating query cache, failed to lock file:\"%s\". error:\"%s\"", path, strerror(errno))));
			close(fd);
			return;
		}
		for (;;)
		{
			sts = read(fd, (char *)&buf, len);
			if (sts == -1)
			{
				ereport(WARNING,
						(errmsg("memcache: invalidating query cache, failed to read file:\"%s\". error:\"%s\"", path, strerror(errno))));

				close(fd);
				return;
			}
			else if (sts == len)
			{
				if (pool_is_shmem_cache())
				{
					ereport(DEBUG1,
						(errmsg("memcache invalidating query cache"),
							errdetail("deleting cacheid:%d itemid:%d",
								   buf.cacheid.blockid, buf.cacheid.itemid)));
					pool_delete_item_shmem_cache(&buf.cacheid);
				}
#ifdef USE_MEMCACHED
				else
				{
					char delbuf[33];

					memcpy(delbuf, buf.hashkey, 32);
					delbuf[32] = 0;
					ereport(DEBUG1,
						(errmsg("memcache invalidating query cache"),
							 errdetail("deleting %s", delbuf)));

					delete_cache_on_memcached(delbuf);
				}
#endif
				continue;
			}

			/*
			 * Must be EOF
			 */
			if (sts != 0)
			{
				ereport(WARNING,
						(errmsg("memcache: invalidating query cache, invalid data length:%d in file:\"%s\"",sts, path)));
				close(fd);
				return;
			}
			break;
		}

		if (unlinkp)
		{
			unlink(path);
		}
		close(fd);
	}
#ifdef SHMEMCACHE_DEBUG
	dump_shmem_cache(0);
#endif
}

#ifdef USE_MEMCACHED
/*
 * Aquire lock on memcached by using memcached_add.
 */
void lock_memcached(void)
{
#define MEMCACHED_LOCK_EXPIRATION	10
#define MY_LOCK_KEY	"pgpool_my_lock_key"
#define MY_LOCK_KEY_SIZE	sizeof(MY_LOCK_KEY)
#define MY_LOCK_DATA	"pgpool_memq_cache"
#define MY_LOCK_DATA_LEN	sizeof(MY_LOCK_DATA)
#define	MY_SLEEP_TIME	100*1000*1000	/* 100 mili seconds */

	memcached_return rc;

	do
	{
		rc = memcached_set(memc, MY_LOCK_KEY, MY_LOCK_KEY_SIZE,
						   MY_LOCK_DATA, MY_LOCK_DATA_LEN, (time_t)MEMCACHED_LOCK_EXPIRATION, 0);
		usleep(MY_SLEEP_TIME);
	}
	while (rc != MEMCACHED_SUCCESS);
}

/*
 * Release lock on memcached by using memcached_add.
 */
void unlock_memcached(void)
{
	memcached_delete(memc, MY_LOCK_KEY, MY_LOCK_KEY_SIZE, 0);
}

#endif
