/*
   Copyright 2017, Mike Krinkin <krinkin.m.u@gmail.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#ifndef __LSM_H__
#define __LSM_H__

#include <types.h>
#include <lsm/ctree.h>

#include <pthread.h>


#define MYFS_MAX_TREES	4
#define MYFS_MTREE_SIZE 32768
#define MYFS_C0_SIZE	2097152
#define MYFS_CX_MULT	4		


struct __myfs_lsm_sb {
	struct __myfs_ctree_sb tree[MYFS_MAX_TREES];
} __attribute__((packed));

struct myfs_lsm_sb {
	struct myfs_ctree_sb tree[MYFS_MAX_TREES];
};


static inline void myfs_lsm_sb2disk(struct __myfs_lsm_sb *disk,
			const struct myfs_lsm_sb *mem)
{
	for (int i = 0; i != MYFS_MAX_TREES; ++i)
		myfs_ctree_sb2disk(&disk->tree[i], &mem->tree[i]);
}

static inline void myfs_lsm_sb2mem(struct myfs_lsm_sb *mem,
			const struct __myfs_lsm_sb *disk)
{
	for (int i = 0; i != MYFS_MAX_TREES; ++i)
		myfs_ctree_sb2mem(&mem->tree[i], &disk->tree[i]);
}


struct myfs_mtree {
	int (*insert)(struct myfs_mtree *, const struct myfs_key *,
				const struct myfs_value *);

	int (*lookup)(struct myfs_mtree *, struct myfs_query *);
	int (*range)(struct myfs_mtree *, struct myfs_query *);
	int (*scan)(struct myfs_mtree *, struct myfs_query *);

	size_t (*size)(const struct myfs_mtree *);
};


struct myfs_lsm;

struct myfs_lsm_policy {
	struct myfs_mtree *(*create)(struct myfs_lsm *);
	void (*destroy)(struct myfs_lsm *, struct myfs_mtree *);

	int (*flush)(struct myfs_lsm *, struct myfs_mtree *,
				const struct myfs_ctree_sb *,
				struct myfs_ctree_sb *);
	int (*merge)(struct myfs_lsm *, const struct myfs_ctree_sb *,
					const struct myfs_ctree_sb *,
					struct myfs_ctree_sb *);

	int (*insert)(struct myfs_lsm *, const struct myfs_key *,
				const struct myfs_value *);
	int (*lookup)(struct myfs_lsm *, struct myfs_query *);
	int (*range)(struct myfs_lsm *, struct myfs_query *);
};


struct myfs_key_ops {
	myfs_cmp_t cmp;
	myfs_del_t deleted;
};


struct myfs_lsm {
	struct myfs *myfs;

	const struct myfs_lsm_policy *policy;
	const struct myfs_key_ops *key_ops;

	struct myfs_lsm_sb sb;
	size_t size;

	struct myfs_mtree *c0;
	struct myfs_mtree *c1;

	pthread_rwlock_t sblock;
	pthread_rwlock_t mtlock;

	/* Used only by merge routines to guard against concurrent merges */
	pthread_mutex_t mtx;
	pthread_cond_t cv;
	int merge[MYFS_MAX_TREES];
};


extern const struct myfs_lsm_policy myfs_lsm_default_policy;


struct myfs_mtree *myfs_lsm_create_default(struct myfs_lsm *lsm);
void myfs_lsm_destroy_default(struct myfs_lsm *lsm, struct myfs_mtree *mtree);


int myfs_lsm_flush_default(struct myfs_lsm *lsm, struct myfs_mtree *new,
			const struct myfs_ctree_sb *old,
			struct myfs_ctree_sb *sb);
int myfs_lsm_merge_default(struct myfs_lsm *lsm,
			const struct myfs_ctree_sb *new,
			const struct myfs_ctree_sb *old,
			struct myfs_ctree_sb *sb);


int myfs_lsm_insert_default(struct myfs_lsm *lsm, const struct myfs_key *key,
			const struct myfs_value *value);
int myfs_lsm_lookup_default(struct myfs_lsm *lsm, struct myfs_query *query);
int myfs_lsm_range_default(struct myfs_lsm *lsm, struct myfs_query *query);


void myfs_lsm_setup(struct myfs_lsm *lsm, struct myfs *myfs,
			const struct myfs_lsm_policy *lops,
			const struct myfs_key_ops *kops,
			const struct myfs_lsm_sb *sb);
void myfs_lsm_release(struct myfs_lsm *lsm);
void myfs_lsm_get_root(struct myfs_lsm_sb *sb, struct myfs_lsm *lsm);


int myfs_lsm_insert(struct myfs_lsm *lsm, const struct myfs_key *key,
			const struct myfs_value *value);
int myfs_lsm_lookup(struct myfs_lsm *lsm, struct myfs_query *query);
int myfs_lsm_range(struct myfs_lsm *lsm, struct myfs_query *query);


int myfs_lsm_need_flush(struct myfs_lsm *lsm);
int myfs_lsm_need_merge(struct myfs_lsm *lsm, size_t i);

int myfs_lsm_merge(struct myfs_lsm *lsm, size_t i);
int myfs_lsm_flush_start(struct myfs_lsm *lsm);
int myfs_lsm_flush_finish(struct myfs_lsm *lsm);
int myfs_lsm_flush(struct myfs_lsm *lsm);

#endif /*__CTREE_H__*/
