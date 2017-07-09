#ifndef __TRANS_H__
#define __TRANS_H__

#include <pthread.h>
#include <types.h>

#define MYFS_TRANS_ENTRY	1
#define MYFS_TRANS_HUGE		2
#define MYFS_TRANS_JUMP		3


struct __myfs_wal_sb {
	le64_t head_offs;
	le64_t curr_offs;
	le32_t used;
};

struct myfs_wal_sb {
	uint64_t head_offs;
	uint64_t curr_offs;
	le32_t used;
};


static inline void myfs_wal_sb2disk(struct __myfs_wal_sb *disk,
			const struct myfs_wal_sb *mem)
{
	disk->head_offs = htole64(mem->head_offs);
	disk->curr_offs = htole64(mem->curr_offs);
	disk->used = htole32(mem->used);
}

static inline void myfs_wal_sb2mem(struct myfs_wal_sb *mem,
			const struct __myfs_wal_sb *disk)
{
	mem->head_offs = le64toh(disk->head_offs);
	mem->curr_offs = le64toh(disk->curr_offs);
	mem->used = le32toh(disk->used);
}


struct myfs_trans {
	struct myfs_trans *next;

	uint64_t trans_id;
	int status;
	pthread_mutex_t mtx;
	pthread_cond_t cv;
};


void myfs_trans_submit(struct myfs *myfs, struct myfs_trans *trans);
int myfs_trans_wait(struct myfs_trans *trans);
void myfs_trans_flusher(struct myfs *myfs);

#endif /*__TRANS_H__*/
