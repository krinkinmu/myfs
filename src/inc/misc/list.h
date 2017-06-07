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
