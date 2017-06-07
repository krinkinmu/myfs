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
#include <misc/hlist.h>
#include <stdlib.h>
#include <assert.h>


void hlist_setup(struct hlist_head *head)
{
	head->head = NULL;
}

int hlist_empty(const struct hlist_head *head)
{
	return head->head == NULL;
}

void hlist_del(struct hlist_node *node)
{
	struct hlist_node *next = node->next;
	struct hlist_node **prev = node->prev;

	*prev = next;
	if (next)
		next->prev = prev;
}

void hlist_add(struct hlist_head *head, struct hlist_node *node)
{
	struct hlist_node *first = head->head;

	node->prev = &head->head;
	node->next = first;
	head->head = node;
	if (first)
		first->prev = &node->next;
	assert(*node->prev == node);
}
