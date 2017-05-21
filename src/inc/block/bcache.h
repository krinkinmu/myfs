#ifndef __BUFFER_CACHE_H__
#define __BUFFER_CACHE_H__

#include <stddef.h>
#include <misc/list.h>


struct myfs_buffer {
	struct hlist_node hl;

	/* all of these protected by the bucket lock */
	uint64_t off;
	size_t refcnt;
	int detached;
	int accessed;

	void *buf;
	size_t size;
};


struct myfs_bcache {
	struct hlist_head *head;
	pthread_mutex_t *mtx;
	uint64_t a, b;
	size_t bits;
	size_t watermark;
};


void __myfs_bcache_setup(struct myfs_bcache *cache, size_t bits);
void myfs_bcache_setup(struct myfs_bcache *cache);
void myfs_bcache_release(struct myfs_bcache *cache);
void myfs_bcache_gc(struct myfs_bcache *cache);

struct myfs_buffer *myfs_bcache_create(struct myfs_bcache *cache, size_t size);
struct myfs_buffer *myfs_bcache_get(struct myfs_bcache *cache, uint64_t off);
void myfs_bcache_add(struct myfs_bcache *cache, struct myfs_buffer *buf);
void myfs_cache_put(struct myfs_bcache *cache, struct myfs_buffer *buf);


#endif /*__BUFFER_CACHE_H__*/
