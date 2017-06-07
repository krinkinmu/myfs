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
