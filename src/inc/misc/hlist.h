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
#ifndef __HLIST_H__
#define __HLIST_H__


struct hlist_node;

struct hlist_head {
	struct hlist_node *head;
};

struct hlist_node {
	struct hlist_node *next;
	struct hlist_node **prev;
};


void hlist_setup(struct hlist_head *head);
int hlist_empty(const struct hlist_head *head);
void hlist_del(struct hlist_node *node);
void hlist_add(struct hlist_head *head, struct hlist_node *node);

#endif /*__HLIST_H__*/
