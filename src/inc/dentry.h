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
#ifndef __DENTRY_H__
#define __DENTRY_H__

#include <types.h>


struct __myfs_dentry_key {
	le64_t parent;
	le32_t hash;
	le32_t size;
	char name[1];
} __attribute__((packed));

struct __myfs_dentry_value {
	le64_t inode;
	le32_t type;
} __attribute__((packed));

struct myfs_dentry {
	uint64_t parent;
	uint64_t inode;
	uint32_t hash;
	uint32_t type;
	uint32_t size;
	const char *name;
};


void myfs_dentry_key2disk(struct __myfs_dentry_key *disk,
			const struct myfs_dentry *mem);
void myfs_dentry_value2disk(struct __myfs_dentry_value *disk,
			const struct myfs_dentry *mem);
void myfs_dentry_key2mem(struct myfs_dentry *mem,
			const struct __myfs_dentry_key *disk);
void myfs_dentry_value2mem(struct myfs_dentry *mem,
			const struct __myfs_dentry_value *disk);


struct myfs_lsm_sb;
struct myfs_lsm;
struct myfs;

void myfs_dentry_map_setup(struct myfs_lsm *lsm, struct myfs *myfs,
			const struct myfs_lsm_sb *sb);
void myfs_dentry_map_release(struct myfs_lsm *lsm);


struct myfs_inode;

int myfs_dentry_read(struct myfs *myfs, struct myfs_inode *dir,
			const char *name, struct myfs_dentry *dentry);
int __myfs_dentry_write(struct myfs *myfs, const struct myfs_dentry *dentry);

#endif /*__DENTRY_H__*/
