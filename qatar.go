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

// Cleans up by closing the underlying db
func (q *Q) Close() {
	q.db.Close()
}

// Enqueues data and returns the id of the corresponding entry in the key value
// store. This id can be used to directly delete the entry.
func (q *Q) Enqueue(data []byte) (ksuid.KSUID, error) {
	id := ksuid.New()
	err := q.db.Set(id.Bytes(), data, nil)
	if err != nil {
		return id, err
	}
	return id, nil
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

// `Peek()` returns the first item if any. It does not dequeue the item
func (q *Q) Peek() (*Item, error) {
	iter := q.db.NewIter(&pebble.IterOptions{})
	defer iter.Close()
	if !iter.First() {
		return nil, iter.Error()
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

// Peeks atmost `num` items from the queue. It does not dequeue these.
func (q *Q) PeekMulti(num int) ([]Item, error) {
	iter := q.db.NewIter(&pebble.IterOptions{})
	defer iter.Close()
	iter.First()
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
