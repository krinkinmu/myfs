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
#include <alloc/alloc.h>
#include <myfs.h>


int myfs_reserve(struct myfs *myfs, uint64_t size, uint64_t *offs)
{
	*offs = atomic_fetch_add_explicit(&myfs->next_offs, size,
			memory_order_relaxed);
	return 0;
}

int myfs_cancel(struct myfs *myfs, uint64_t size, uint64_t offs)
{
	(void) myfs;
	(void) size;
	(void) offs;
	return 0;
}
