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
#define FUSE_USE_VERSION 30

#include <fuse_lowlevel.h>
#include <block/block.h>
#include <dentry.h>
#include <inode.h>
#include <myfs.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>



static const double FUSE_TIMEOUT_INF = 24 * 60 * 60;


static void fuse_inode2attr(struct stat *attr, const struct myfs_inode *inode)
{
	attr->st_ino = inode->inode;
	attr->st_size = inode->size;
	attr->st_nlink = inode->links;
	attr->st_mode = inode->type | inode->perm;
	attr->st_uid = inode->uid;
	attr->st_gid = inode->gid;
	myfs_stamp2timespec(&attr->st_atim, inode->mtime);
	myfs_stamp2timespec(&attr->st_mtim, inode->mtime);
	myfs_stamp2timespec(&attr->st_ctim, inode->ctime);
}

static void fuse_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	struct myfs *myfs = fuse_req_userdata(req);
	struct fuse_entry_param entry;
	struct myfs_inode *dir = myfs_inode_get(myfs, parent);
	struct myfs_inode *inode = NULL;
	int err;

	assert(dir && !(dir->flags & MYFS_INODE_NEW));
	err = myfs_lookup(myfs, dir, name, &inode);
	myfs_inode_put(myfs, dir);

	if (err) {
		fuse_reply_err(req, -err);
		return;
	}

	memset(&entry, 0, sizeof(entry));

	assert(!pthread_rwlock_rdlock(&inode->rwlock));
	if (inode->type & MYFS_TYPE_DEL)
		err = -ENOENT;
	if (!err) {
		entry.ino = inode->inode;
		entry.generation = 1;
		entry.attr_timeout = FUSE_TIMEOUT_INF;
		entry.entry_timeout = FUSE_TIMEOUT_INF;
		fuse_inode2attr(&entry.attr, inode);
	}
	assert(!pthread_rwlock_unlock(&inode->rwlock));

	if (err) {
		myfs_inode_put(myfs, inode);
		fuse_reply_err(req, -err);
		return;
	}
	fuse_reply_entry(req, &entry);
}

static void fuse_getattr(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
{
	struct myfs *myfs = fuse_req_userdata(req);
	struct myfs_inode *inode = myfs_inode_get(myfs, ino);
	struct stat attr;
	int err = 0;

	(void) fi;
	assert(inode);

	memset(&attr, 0, sizeof(attr));
	err = myfs_inode_read(myfs, inode);

	assert(!pthread_rwlock_rdlock(&inode->rwlock));
	if (!err && (inode->type & MYFS_TYPE_DEL))
		err = -ENOENT;

	if (!err)
		fuse_inode2attr(&attr, inode);
	assert(!pthread_rwlock_unlock(&inode->rwlock));
	myfs_inode_put(myfs, inode);

	if (err) fuse_reply_err(req, -err);
	else fuse_reply_attr(req, &attr, 0);
}

static void fuse_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
			int to_set, struct fuse_file_info *fi)
{
	(void) fi;

	struct myfs *myfs = fuse_req_userdata(req);
	struct myfs_inode *inode = myfs_inode_get(myfs, ino);

	int err = myfs_inode_read(myfs, inode);

	if (err) {
		myfs_inode_put(myfs, inode);
		fuse_reply_err(req, -err);
		return;
	}

	assert(!pthread_rwlock_wrlock(&inode->rwlock));
	if (inode->type & MYFS_TYPE_DEL)
		err = -ENOENT;
	else {
		const unsigned long perm_mask = S_IRWXU | S_IRWXG | S_IRWXO;
		const int set_now = (FUSE_SET_ATTR_MTIME_NOW |
					FUSE_SET_ATTR_ATIME_NOW);

		if (to_set & FUSE_SET_ATTR_MODE)
			inode->perm = attr->st_mode & perm_mask;
		if (to_set & FUSE_SET_ATTR_UID)
			inode->uid = attr->st_uid;
		if (to_set & FUSE_SET_ATTR_GID)
			inode->gid = attr->st_gid;
		if (to_set & FUSE_SET_ATTR_SIZE)
			inode->size = attr->st_size;
		if (to_set & FUSE_SET_ATTR_ATIME)
			inode->mtime = myfs_timespec2stamp(&attr->st_atim);
		if (to_set & FUSE_SET_ATTR_MTIME)
			inode->mtime = myfs_timespec2stamp(&attr->st_mtim);
		if (to_set & set_now)
			inode->mtime = myfs_now();
		if (to_set & FUSE_SET_ATTR_CTIME)
			inode->ctime = myfs_timespec2stamp(&attr->st_ctim);
		err = __myfs_inode_write(myfs, inode);
		fuse_inode2attr(attr, inode);
	}
	assert(!pthread_rwlock_unlock(&inode->rwlock));
	myfs_inode_put(myfs, inode);

	if (err) fuse_reply_err(req, -err);
	else fuse_reply_attr(req, attr, 0);
}

static void fuse_forget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup)
{
	struct myfs *myfs = fuse_req_userdata(req);
	struct myfs_inode *inode = myfs_inode_get(myfs, ino);

	assert(inode && !(inode->flags & MYFS_INODE_NEW));
	__myfs_inode_put(myfs, inode, nlookup);
	myfs_inode_put(myfs, inode);
}

static void fuse_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
			mode_t mode, dev_t rdev)
{
	(void) rdev;

	if (!S_ISREG(mode) && !S_ISDIR(mode)) {
		fuse_reply_err(req, ENOTSUP);
		return;
	}

	struct myfs *myfs = fuse_req_userdata(req);
	const struct fuse_ctx *ctx = fuse_req_ctx(req);
	struct myfs_inode *dir = myfs_inode_get(myfs, parent);
	struct myfs_inode *child = NULL;
	struct fuse_entry_param entry;
	int err;

	assert(dir && !(dir->flags & MYFS_INODE_NEW));
	err = myfs_create(myfs, dir, name, ctx->uid, ctx->gid, mode, &child);
	myfs_inode_put(myfs, dir);

	if (err) {
		fuse_reply_err(req, -err);
		return;
	}

	memset(&entry, 0, sizeof(entry));
	assert(!pthread_rwlock_rdlock(&child->rwlock));
	entry.ino = child->inode;
	entry.generation = 1;
	entry.attr_timeout = FUSE_TIMEOUT_INF;
	entry.entry_timeout = FUSE_TIMEOUT_INF;
	fuse_inode2attr(&entry.attr, child);
	assert(!pthread_rwlock_unlock(&child->rwlock));
	fuse_reply_entry(req, &entry);
}

static void fuse_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
			mode_t mode)
{
	struct myfs *myfs = fuse_req_userdata(req);
	const struct fuse_ctx *ctx = fuse_req_ctx(req);
	struct myfs_inode *dir = myfs_inode_get(myfs, parent);
	struct myfs_inode *child = NULL;
	struct fuse_entry_param entry;
	int err;

	(void) mode;
	
	assert(dir && !(dir->flags & MYFS_INODE_NEW));
	err = myfs_create(myfs, dir, name, ctx->uid, ctx->gid, S_IFDIR, &child);
	myfs_inode_put(myfs, dir);

	if (err) {
		fuse_reply_err(req, -err);
		return;
	}

	memset(&entry, 0, sizeof(entry));
	assert(!pthread_rwlock_rdlock(&child->rwlock));
	entry.ino = child->inode;
	entry.generation = 1;
	entry.attr_timeout = FUSE_TIMEOUT_INF;
	entry.entry_timeout = FUSE_TIMEOUT_INF;
	fuse_inode2attr(&entry.attr, child);
	assert(!pthread_rwlock_unlock(&child->rwlock));
	fuse_reply_entry(req, &entry);
}

static void fuse_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	struct myfs *myfs = fuse_req_userdata(req);
	struct myfs_inode *dir = myfs_inode_get(myfs, parent);
	int err;
	
	assert(dir && !(dir->flags & MYFS_INODE_NEW));
	err = myfs_unlink(myfs, dir, name);
	myfs_inode_put(myfs, dir);
	fuse_reply_err(req, -err);
}

static void fuse_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	struct myfs *myfs = fuse_req_userdata(req);
	struct myfs_inode *dir = myfs_inode_get(myfs, parent);
	int err;
	
	assert(dir && !(dir->flags & MYFS_INODE_NEW));
	err = myfs_rmdir(myfs, dir, name);
	myfs_inode_put(myfs, dir);
	fuse_reply_err(req, -err);
}

static void fuse_rename(fuse_req_t req, fuse_ino_t parent, const char *oldname,
			fuse_ino_t newparent, const char *newname,
			unsigned flags)
{
	struct myfs *myfs = fuse_req_userdata(req);
	struct myfs_inode *old = myfs_inode_get(myfs, parent);
	struct myfs_inode *new = myfs_inode_get(myfs, newparent);
	int err;

	(void) flags;
	assert(old && !(old->flags & MYFS_INODE_NEW));
	assert(new && !(new->flags & MYFS_INODE_NEW));

	err = myfs_rename(myfs, old, oldname, new, newname);
	myfs_inode_put(myfs, old);
	myfs_inode_put(myfs, new);
	fuse_reply_err(req, -err);
}

static void fuse_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t parent,
			const char *newname)
{
	struct myfs *myfs = fuse_req_userdata(req);
	struct myfs_inode *inode = myfs_inode_get(myfs, ino);
	struct myfs_inode *dir = myfs_inode_get(myfs, parent);
	struct fuse_entry_param entry;
	int err;

	assert(inode && !(inode->flags & MYFS_INODE_NEW));
	assert(dir && !(dir->flags & MYFS_INODE_NEW));

	err = myfs_link(myfs, inode, dir, newname);
	myfs_inode_put(myfs, dir);

	if (err) {
		myfs_inode_put(myfs, inode);
		fuse_reply_err(req, -err);
		return;
	}

	memset(&entry, 0, sizeof(entry));
	assert(!pthread_rwlock_rdlock(&inode->rwlock));
	entry.ino = inode->inode;
	entry.generation = 1;
	entry.attr_timeout = FUSE_TIMEOUT_INF;
	entry.entry_timeout = FUSE_TIMEOUT_INF;
	fuse_inode2attr(&entry.attr, inode);
	assert(!pthread_rwlock_unlock(&inode->rwlock));
	fuse_reply_entry(req, &entry);
}


struct fuse_readdir_ctx {
	struct myfs_readdir_ctx ctx;
	fuse_req_t req;
	char *buf;
	size_t cap;
	size_t size;
};

static int fuse_readdir_emit(struct myfs_readdir_ctx *c,
			const struct myfs_dentry *dentry)
{
	char name[MYFS_FS_NAMEMAX + 1];
	struct fuse_readdir_ctx *ctx = (struct fuse_readdir_ctx *)c;
	const size_t rem = ctx->cap - ctx->size;
	char *buf = ctx->buf + ctx->size;
	struct stat attr;

	memset(&attr, 0, sizeof(attr));
	attr.st_ino = dentry->inode;
	attr.st_mode = dentry->type;

	memcpy(name, dentry->name, dentry->size);
	name[dentry->size] = '\0';

	const size_t req = fuse_add_direntry(ctx->req, buf, rem,
				name, &attr, dentry->hash);

	if (req > rem)
		return 1;
	ctx->size += req;
	return 0;
}

static void fuse_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
			struct fuse_file_info *fi)
{
	struct fuse_readdir_ctx ctx = {
		.ctx = { .emit = &fuse_readdir_emit },
		.req = req,
		.buf = malloc(size),
		.cap = size,
		.size = 0
	};
	struct myfs *myfs = fuse_req_userdata(req);
	struct myfs_inode *dir = myfs_inode_get(myfs, ino);
	int err;

	(void) fi;

	assert(ctx.buf);
	err = myfs_readdir(myfs, dir, &ctx.ctx, off);
	myfs_inode_put(myfs, dir);
	if (err < 0) {
		fuse_reply_err(req, -err);
		return;
	}
	fuse_reply_buf(req, ctx.buf, ctx.size);
	free(ctx.buf);
}

static void fuse_read(fuse_req_t req, fuse_ino_t ino,
			size_t size, off_t off, struct fuse_file_info *fi)
{
	(void) fi;

	struct myfs *myfs = fuse_req_userdata(req);
	struct myfs_inode *file = myfs_inode_get(myfs, ino);
	long ret = myfs_inode_read(myfs, file);
	void *buf = NULL;

	if (ret) {
		myfs_inode_put(myfs, file);
		fuse_reply_err(req, -ret);
		return;
	}

	assert(buf = malloc(size));
	ret = myfs_read(myfs, file, buf, size, off);
	if (ret < 0)
		fuse_reply_err(req, -ret);
	else
		fuse_reply_buf(req, buf, ret);
	myfs_inode_put(myfs, file);
	free(buf);
}

static void fuse_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
			size_t size, off_t off, struct fuse_file_info *fi)
{
	(void) fi;

	struct myfs *myfs = fuse_req_userdata(req);
	struct myfs_inode *file = myfs_inode_get(myfs, ino);
	long ret = myfs_inode_read(myfs, file);

	if (ret) {
		myfs_inode_put(myfs, file);
		fuse_reply_err(req, -ret);
		return;
	}

	ret = myfs_write(myfs, file, buf, size, off);
	if (ret < 0)
		fuse_reply_err(req, -ret);
	else
		fuse_reply_write(req, ret);
	myfs_inode_put(myfs, file);
}

static void fuse_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
			struct fuse_file_info *fi)
{
	(void) ino;
	(void) datasync;
	(void) fi;

	struct myfs *myfs = fuse_req_userdata(req);
	const int err = myfs_commit(myfs);

	fuse_reply_err(req, -err);
}

static void fuse_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
			struct fuse_file_info *fi)
{
	(void) ino;
	(void) datasync;
	(void) fi;

	struct myfs *myfs = fuse_req_userdata(req);
	const int err = myfs_commit(myfs);

	fuse_reply_err(req, -err);
}

static const struct fuse_lowlevel_ops myfs_ops = {
	.lookup = &fuse_lookup,
	.setattr = &fuse_setattr,
	.getattr = &fuse_getattr,
	.forget = &fuse_forget,
	.mknod = &fuse_mknod,
	.mkdir = &fuse_mkdir,
	.unlink = &fuse_unlink,
	.rmdir = &fuse_rmdir,
	.rename = &fuse_rename,
	.link = &fuse_link,
	.readdir = &fuse_readdir,
	.read = &fuse_read,
	.write = &fuse_write,
	.fsync = &fuse_fsync,
	.fsyncdir = &fuse_fsyncdir,
};


struct myfs_config {
	const char *path;
	int fd;
};

static const struct fuse_opt myfs_opts[] = {
	{"--image=%s", offsetof(struct myfs_config, path), 0},
	FUSE_OPT_END
};


static void version(void)
{
	fprintf(stderr, "FUSE library version %s\n", fuse_pkgversion());
	fuse_lowlevel_version();
}

static void usage(const char *name)
{
	fprintf(stderr, "usage: %s [options] <mountpoint>\n\n", name);
	fprintf(stderr, "\t--image=path path to the image file\n\n");
	fuse_cmdline_help();
	fuse_lowlevel_help();
}

int main(int argc, char **argv)
{
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_cmdline_opts opts;
	struct myfs_config config;
	int ret = -1;

	memset(&opts, 0, sizeof(opts));
	memset(&config, 0, sizeof(config));
	config.fd = -1;

	if (fuse_opt_parse(&args, &config, myfs_opts, NULL)) {
		fprintf(stderr, "failed to parse command line arguments\n");
		goto out;
	}

	if (fuse_parse_cmdline(&args, &opts)) {
		fprintf(stderr, "failed to parse command line arguments\n");
		goto out;
	}

	if (opts.show_help) {
		usage(argv[0]);
		goto out;
	}

	if (opts.show_version) {
		version();
		goto out;
	}

	if (!config.path) {
		fprintf(stderr, "image file path expected (--image option)\n");
		usage(argv[0]);
		goto out;
	}

	config.fd = open(config.path, O_RDWR);
	if (config.fd < 0) {
		fprintf(stderr, "failed to open %s\n", config.path);
		goto out;
	}

	struct fuse_session *se = NULL;
	struct sync_bdev bdev;
	struct myfs myfs;

	memset(&myfs, 0, sizeof(myfs));
	sync_bdev_setup(&bdev, config.fd);

	if (myfs_mount(&myfs, &bdev.bdev)) {
		fprintf(stderr, "failed to parse superblock\n");
		goto out;
	}

	se = fuse_session_new(&args, &myfs_ops, sizeof(myfs_ops), &myfs);
	if (!se) {
		fprintf(stderr, "failed to create fuse session\n");
		goto unmount;
	}

	if (fuse_set_signal_handlers(se)) {
		fprintf(stderr, "failed to setup fuse signal handlers\n");
		goto destroy_session;
	}

	if (fuse_session_mount(se, opts.mountpoint)) {
		fprintf(stderr, "failed to mount\n");
		goto remove_handlers;
	}

	fuse_daemonize(opts.foreground);

	if (opts.singlethread)
		ret = fuse_session_loop(se);
	else
		ret = fuse_session_loop_mt(se, opts.clone_fd);

	if (ret)
		fprintf(stderr, "failed to run fuse event loop");
	else
		ret = myfs_commit(&myfs);

remove_handlers:
	fuse_remove_signal_handlers(se);
destroy_session:
	fuse_session_destroy(se);
unmount:
	myfs_unmount(&myfs);
out:
	if (config.fd >= 0)
		close(config.fd);
	free((void *)config.path);
	free(opts.mountpoint);
	fuse_opt_free_args(&args);

	return ret ? -1 : 0;
}
