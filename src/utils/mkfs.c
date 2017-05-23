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
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <myfs.h>
#include <dentry.h>
#include <inode.h>
#include <block/block.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>


struct myfs_config {
	const char *name;
	size_t page_size;
};


union myfs_sb_wrap {
	struct __myfs_sb sb;
	char buf[512];
};

static int format(const struct myfs_config *config)
{
	const int mode = O_WRONLY | O_CREAT | O_TRUNC;
	const int perm = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;

	const size_t check_size = myfs_align_up(sizeof(struct __myfs_check),
				config->page_size);
	const int fd = open(config->name, mode, perm);

	if (fd < 0) {
		fprintf(stderr, "failed to open %s\n", config->name);
		return -1;
	}

	const uint64_t now = myfs_now();
	struct hlist_node *prev;
	struct myfs_inode root = {
		.ll = { NULL, &prev },
		.inode = MYFS_FS_ROOT,
		.size = 0,
		.links = 2,
		.type = MYFS_TYPE_DIR,
		.uid = getuid(),
		.gid = getgid(),
		.ctime = now,
		.mtime = now,
		.perm = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH,
		.bmap = { 0, NULL },
	};
	union myfs_sb_wrap sb;
	struct sync_bdev bdev;
	struct myfs myfs;

	int ret = 0;

	prev = &root.ll;
	sync_bdev_setup(&bdev, fd);
	memset(&myfs, 0, sizeof(myfs));
	memset(&sb, 0, sizeof(sb));

	myfs.bdev = &bdev.bdev;
	myfs.page_size = config->page_size;
	myfs.fanout = MYFS_MIN_FANOUT;
	myfs.sb.magic = MYFS_FS_MAGIC;
	myfs.sb.page_size = config->page_size;
	myfs.sb.check_size = check_size / config->page_size;
	myfs.sb.check_offs = 1;
	myfs.sb.backup_check_offs = 1 + myfs.sb.check_size;
	myfs.sb.root = MYFS_FS_ROOT;
	myfs.check.ino = MYFS_FS_ROOT + 1;
	myfs.next_offs = myfs.sb.backup_check_offs + myfs.sb.check_size;
	atomic_store_explicit(&myfs.next_ino, myfs.check.ino,
				memory_order_relaxed);

	myfs_inode_map_setup(&myfs.inode_map, &myfs, &myfs.check.inode_sb);
	myfs_dentry_map_setup(&myfs.dentry_map, &myfs, &myfs.check.dentry_sb);

	ret = __myfs_inode_write(&myfs, &root);
	if (ret) {
		fprintf(stderr, "failed to create root inode\n");
		goto err;
	}

	ret = myfs_lsm_flush(&myfs.inode_map);
	if (ret) {
		fprintf(stderr, "failed to flush inode map\n");
		goto err;
	}

	ret = myfs_lsm_flush(&myfs.dentry_map);
	if (ret) {
		fprintf(stderr, "failed to flush dentry map\n");
		goto err;
	}

	myfs_sb2disk(&sb.sb, &myfs.sb);

	if (myfs_block_write(&myfs, &sb, sizeof(sb), 0)) {
		fprintf(stderr, "failed to write superblock\n");
		goto err;
	}

	if (myfs_checkpoint(&myfs)) {
		fprintf(stderr, "failed to create checkpoint\n");
		goto err;
	}

err:
	myfs_dentry_map_release(&myfs.dentry_map);
	myfs_inode_map_release(&myfs.inode_map);
	close(fd);
	return ret;
}


static const struct option opts[] = {
	{"page_size", required_argument, NULL, 's'},
	{"help", no_argument, NULL, 's'},
	{NULL, 0, NULL, 0},
};


static void usage(FILE *out, const char *name)
{
	fprintf(out, "Usage: %s [options] filename\n\n", name);
	fprintf(out, "\t--page_size, -s <num> - file system page size in bytes\n");
	fprintf(out, "\t--help, -h - show this message\n");
}

int main(int argc, char **argv)
{
	unsigned long page_size = 4096;
	char *endptr;
	int kind;

	while ((kind = getopt_long(argc, argv, "hs:", opts, NULL)) != -1) {
		switch (kind) {
		case 's':
			page_size = strtoul(optarg, &endptr, 10);
			if (*endptr != '\0') {
				fprintf(stderr, "page size must be a number\n");
				return -1;
			}

			if ((page_size & (page_size - 1)) != 0) {
				fprintf(stderr, "page size must be power of two\n");
				return -1;
			}

			if (page_size < 512) {
				fprintf(stderr, "page size must be at least 512\n");
				return -1;
			}
			break;
		case 'h':
			usage(stdout, argv[0]);
			return 0;
		default:
			usage(stderr, argv[0]);
			return -1;
		}
	}

	if (optind != argc - 1) {
		usage(stderr, argv[0]);
		return -1;
	}

	struct myfs_config config;

	config.name = argv[optind];
	config.page_size = page_size;

	if (format(&config)) {
		fprintf(stderr, "%s failed to create empty file system in %s\n",
					argv[0], argv[optind]);
		return -1;
	}
	return 0;
}
