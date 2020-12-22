// `qatar` is a simple queuing system backed by an on-disk key-value store using
// `pebble`. You can enqueue or dequeue a payload of type `[]byte`
package qatar

import (
	"github.com/cockroachdb/pebble"
	"github.com/segmentio/ksuid"
)

// Struct corresponding to a queue.
type Q struct {
	db *pebble.DB
}

// Sets up a new queue, with the `pebble` store in directory `dirName`. Do
// remember to call `Close()` on the instance of `*Q` that is returned.
func NewQ(dirName string) (*Q, error) {
	db, err := pebble.Open(dirName, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Q{db}, nil
}

// Creates a queue in the specified directory. If this directory already
// exists, then we get an error. If opening an existing queue, use `OpenQ`
// instead.
func CreateQ(dirName string) (*Q, error) {
	db, err := pebble.Open(dirName, &pebble.Options{
		ErrorIfExists: true,
	})
	if err != nil {
		return nil, err
	}
	return &Q{db}, nil
}

// Opens an existing queue in the specified directory. If this directory does
// not exist, then an error is returned. To create a queue, use the `CreateQ`
// method.
func OpenQ(dirName string) (*Q, error) {
	db, err := pebble.Open(dirName, &pebble.Options{
		ErrorIfNotExists: true,
	})
	if err != nil {
		return nil, err
	}
	return &Q{db}, nil
}

// Cleans up by closing the underlying db
func (q *Q) Close() {
	q.db.Close()
}

// Enqueues data and returns the id of the corresponding entry in the key value
// store. This id can be used to directly delete the entry.
func (q *Q) Enqueue(data []byte) (id ksuid.KSUID, err error) {
	id = ksuid.New()
	err = q.EnqueueWithId(data, id)
	return
}

// Enqueue items with a specified id
func (q *Q) EnqueueWithId(data []byte, id ksuid.KSUID) error {
	err := q.db.Set(id.Bytes(), data, nil)
	if err != nil {
		return err
	}
	return nil
}

// Struct corresponding to an Item that is returned from the queue
type Item struct {
	Id   ksuid.KSUID
	Data []byte
}

// `Dequeue` operates as a `Peek()` and then a `Delete()` of the Item from the
// queue
func (q *Q) Dequeue() (*Item, error) {
	item, err := q.Peek()
	if err != nil {
		return nil, err
	}
	if item != nil {
		err := q.db.Delete(item.Id.Bytes(), nil)
		if err != nil {
			return nil, err
		}
	}
	return item, nil
}

// Peeks and deletes the first item after the provided id.
func (q *Q) DequeueAfter(id ksuid.KSUID) (*Item, error) {
	item, err := q.PeekAfter(id)
	if err != nil {
		return nil, err
	}
	if item != nil {
		err := q.db.Delete(item.Id.Bytes(), nil)
		if err != nil {
			return nil, err
		}
	}
	return item, nil
}

func itemFromIter(iter *pebble.Iterator) (*Item, error) {
	err := iter.Error()
	if err != nil {
		return nil, err
	}
	if !iter.Valid() {
		return nil, nil
	}
	id, err := ksuid.FromBytes(iter.Key())
	if err != nil {
		return nil, err
	}
	ival := iter.Value()
	if iter.Error() != nil {
		return nil, iter.Error()
	}
	v := make([]byte, len(ival))
	copy(v, ival)
	return &Item{id, v}, nil
}

func multiItemsFromIter(iter *pebble.Iterator, num int) ([]Item, error) {
	i := 0
	var items []Item
	for iter.Valid() && i < num {
		id, err := ksuid.FromBytes(iter.Key())
		if err != nil {
			return nil, err
		}
		ival := iter.Value()
		if iter.Error() != nil {
			return nil, iter.Error()
		}
		v := make([]byte, len(ival))
		copy(v, ival)
		items = append(items, Item{id, v})
		i += 1
		iter.Next()
	}
	if iter.Error() != nil {
		return nil, iter.Error()
	}
	return items, nil
}

// `Peek()` returns the first item if any. It does not dequeue the item
func (q *Q) Peek() (*Item, error) {
	iter := q.db.NewIter(&pebble.IterOptions{})
	defer iter.Close()
	iter.First()
	return itemFromIter(iter)
}

// Peeks atmost `num` items from the queue. It does not dequeue these.
func (q *Q) PeekMulti(num int) ([]Item, error) {
	iter := q.db.NewIter(&pebble.IterOptions{})
	defer iter.Close()
	iter.First()
	return multiItemsFromIter(iter, num)
}

func (q *Q) PeekAfter(id ksuid.KSUID) (*Item, error) {
	iter := q.db.NewIter(&pebble.IterOptions{})
	defer iter.Close()
	iter.SeekGE(id.Bytes())
	return itemFromIter(iter)
}

func (q *Q) PeekMultiAfter(num int, id ksuid.KSUID) ([]Item, error) {
	iter := q.db.NewIter(&pebble.IterOptions{})
	defer iter.Close()
	iter.SeekGE(id.Bytes())
	return multiItemsFromIter(iter, num)
}

// Deletes the specified `id` from the queue. This can be in any position, and
// need not be at the tail of the queue.
func (q *Q) Delete(id ksuid.KSUID) error {
	return q.db.Delete(id.Bytes(), nil)
}

// Returns the number of items in the queue. Note that this consists of
// scanning the entire queue, and can be very expensive. There is no caching
// provided.
func (q *Q) Count() (int, error) {
	iter := q.db.NewIter(&pebble.IterOptions{})
	defer iter.Close()
	iter.First()
	i := 0
	for iter.Valid() {
		i += 1
		iter.Next()
	}
	err := iter.Error()
	if err != nil {
		return 0, err
	}
	return i, nil
}
