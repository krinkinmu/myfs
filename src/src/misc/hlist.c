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
