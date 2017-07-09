#include <trans/trans.h>
#include <myfs.h>
#include <stdatomic.h>


void myfs_trans_submit(struct myfs *myfs, struct myfs_trans *trans)
{
	struct myfs_trans *next = atomic_load_explicit(&myfs->trans,
				memory_order_relaxed);

	trans->status = 0;
	do {
		trans->next = next;
	} while (!atomic_compare_exchange_strong_explicit(&myfs->trans,
				&next, trans,
				memory_order_release, memory_order_relaxed));

	if (!next) {
		assert(!pthread_mutex_lock(&myfs->trans_mtx));
		assert(!pthread_cond_signal(&myfs->trans_cv));
		assert(!pthread_mutex_unlock(&myfs->trans_mtx));
	}
}

int myfs_trans_wait(struct myfs_trans *trans)
{
	assert(!pthread_mutex_lock(&trans->mtx));
	while (!trans->status)
		assert(!pthread_cond_wait(&trans->cv, &trans->mtx));
	assert(!pthread_mutex_unlock(&trans->mtx));
	return trans->status;
}

static void myfs_trans_process_batch(struct myfs *myfs, struct myfs_trans *lst)
{
	(void) myfs;
	(void) lst;
}

void myfs_trans_flusher(struct myfs *myfs)
{
	while (1) {
		struct myfs_trans *list = atomic_load_explicit(&myfs->trans,
					memory_order_relaxed);
		struct myfs_trans *rev = NULL;

		if (!list) {
			assert(!pthread_mutex_lock(&myfs->trans_mtx));
			while (!(list = atomic_load_explicit(&myfs->trans,
						memory_order_relaxed)) &&
						!myfs->done)
				assert(!pthread_cond_wait(&myfs->trans_cv,
							&myfs->trans_mtx));
			assert(!pthread_mutex_unlock(&myfs->trans_mtx));
		}

		if (!list)
			return;

		while (!atomic_compare_exchange_strong_explicit(&myfs->trans,
					&list, NULL,
					memory_order_consume,
					memory_order_relaxed));

		while (list) {
			struct myfs_trans *next = list->next;

			list->next = rev;
			rev = list;
			list = next;
		}

		myfs_trans_process_batch(myfs, rev);
	}
}
