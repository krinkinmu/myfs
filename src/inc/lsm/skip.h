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
#ifndef __SKIP_LIST_H__
#define __SKIP_LIST_H__

#include <lsm/lsm.h>


#define MYFS_MAX_MTREE_HIGHT	20


struct myfs_skip_node {
	struct myfs_key key;
	struct myfs_value value;
	size_t seq;
	struct myfs_skip_node * _Atomic next[1];
};

struct myfs_skiplist {
	struct myfs_mtree mtree;
	struct myfs_skip_node *head;
	size_t _Atomic size;
	int (*cmp)(const struct myfs_key *, const struct myfs_key *);
};


void myfs_skiplist_setup(struct myfs_skiplist *sl, myfs_cmp_t cmp);
void myfs_skiplist_release(struct myfs_skiplist *sl);


int myfs_skip_insert(struct myfs_skiplist *skip, const struct myfs_key *key,
			const struct myfs_value *value);
int myfs_skip_lookup(struct myfs_skiplist *skip, struct myfs_query *query);
int myfs_skip_range(struct myfs_skiplist *skip, struct myfs_query *query);
int myfs_skip_scan(struct myfs_skiplist *skip, struct myfs_query *query);
size_t myfs_skip_size(const struct myfs_skiplist *skip);

#endif /*__SKIP_LIST_H__*/
