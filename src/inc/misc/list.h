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
#ifndef __LIST_H__
#define __LIST_H__


struct list_head {
	struct list_head *next;
	struct list_head *prev;
};


void list_setup(struct list_head *head);
int list_empty(const struct list_head *head);
void list_append(struct list_head *head, struct list_head *node);
void list_del(struct list_head *node);
void list_splice(struct list_head *head, struct list_head *pos);

#endif /*__LIST_H__*/
