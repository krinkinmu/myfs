#ifndef __FLAT_MAP_H__
#define __FLAT_MAP_H__

#include <myfs.h>
#include <lsm/lsm.h>


struct myfs_array {
	size_t sz, cap;
	struct myfs_item *item;

	size_t buf_sz, buf_cap;
	void *buf;
};


void myfs_array_setup(struct myfs_array *array);
void myfs_array_release(struct myfs_array *array);

int myfs_array_append(struct myfs_array *array,
			const struct myfs_key *key,
			const struct myfs_value *value);

void myfs_array_key(const struct myfs_array *array,
			struct myfs_key *key);
void myfs_array_value(const struct myfs_array *array,
			struct myfs_value *value);
size_t myfs_array_size(const struct myfs_array *array);

#endif /*__FLAT_MAP_H__*/
