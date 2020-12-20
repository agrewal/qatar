package qatar

import (
	"github.com/cockroachdb/pebble"
	"github.com/segmentio/ksuid"
)

type Q struct {
	db *pebble.DB
}

func NewQ(dirName string) (*Q, error) {
	db, err := pebble.Open(dirName, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Q{db}, nil
}

func (q *Q) Close() {
	q.db.Close()
}

func (q *Q) Enqueue(data []byte) (ksuid.KSUID, error) {
	id := ksuid.New()
	err := q.db.Set(id.Bytes(), data, nil)
	if err != nil {
		return id, err
	}
	return id, nil
}

type Item struct {
	Id   ksuid.KSUID
	Data []byte
}

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

func (q *Q) Delete(id ksuid.KSUID) error {
	return q.db.Delete(id.Bytes(), nil)
}

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
