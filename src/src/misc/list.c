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
#include <misc/list.h>


void list_setup(struct list_head *head)
{
	head->next = head;
	head->prev = head;
}

int list_empty(const struct list_head *head)
{
	return head->next == head;
}

void list_append(struct list_head *head, struct list_head *node)
{
	struct list_head *prev = head->prev;
	struct list_head *next = head;

	node->prev = prev;
	node->next = next;
	prev->next = node;
	next->prev = node;
}

void list_del(struct list_head *node)
{
	struct list_head *prev = node->prev;
	struct list_head *next = node->next;

	prev->next = next;
	next->prev = prev;
}

void list_splice(struct list_head *head, struct list_head *pos)
{
	if (list_empty(head))
		return;

	struct list_head *first = head->next;
	struct list_head *last = head->prev;

	list_setup(head);

	struct list_head *prev = pos;
	struct list_head *next = pos->next;

	last->next = next;
	first->prev = prev;
	next->prev = last;
	prev->next = first;
}
