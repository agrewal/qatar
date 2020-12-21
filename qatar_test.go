package qatar

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/segmentio/ksuid"
)

var q *Q

func TestMain(m *testing.M) {
	// Setup
	name, err := ioutil.TempDir("", "qatar_test_")
	if err != nil {
		panic("Could not create tmp directory")
	}
	defer os.RemoveAll(name)
	q, err = NewQ(name)
	if err != nil {
		panic("Could not setup queue")
	}
	defer q.Close()

	code := m.Run()

	os.Exit(code)
}

func TestOrdering(t *testing.T) {
	id := ksuid.New()
	var ids []ksuid.KSUID
	var d [][]byte
	numItems := 10
	for i := 0; i < numItems; i++ {
		id = id.Next()
		ids = append(ids, id)
		data := []byte(fmt.Sprintf("test_%d", i))
		d = append(d, data)
	}
	for _, ix := range rand.Perm(numItems) {
		err := q.EnqueueWithId(d[ix], ids[ix])
		if err != nil {
			t.Error("Error while enqueueing", err)
		}
	}
	items, err := q.PeekMulti(numItems)
	if err != nil {
		t.Error("Failed while peeking", err)
	}
	if len(items) != numItems {
		t.Error("Did not retrieve the right set of items")
	}
	for i := 0; i < numItems; i++ {
		if items[i].Id != ids[i] || !bytes.Equal(items[i].Data, d[i]) {
			t.Errorf("Mismatch found at index %d", i)
		}
	}
	for _, id := range ids {
		err := q.Delete(id)
		if err != nil {
			t.Error("Error while deleting")
		}
	}
}

func TestPeek(t *testing.T) {
	data := [1]string{"test1"}
	for _, d := range data {
		_, err := q.Enqueue([]byte(d))
		if err != nil {
			t.Error("Error while enqueueing", err)
		}
	}

	cnt, err := q.Count()
	if err != nil {
		t.Error(err)
	}

	if cnt != 1 {
		t.Error("Count did not right number of items in the queue")
	}

	items, err := q.PeekMulti(1)
	if err != nil {
		t.Error(err)
	}
	if len(items) != 1 {
		t.Error("Did not find the right number of items in the queue")
	}

	for ix, item := range items {
		if data[ix] != string(item.Data) {
			t.Errorf("Expected %s, got %s", data[ix], string(item.Data))
		}
	}

	for _, item := range items {
		err = q.Delete(item.Id)
		if err != nil {
			t.Error(err)
		}
	}

	cnt, err = q.Count()
	if err != nil {
		t.Error(err)
	}

	if cnt != 0 {
		t.Error("Deletes are not visible")
	}
}

func TestDequeue(t *testing.T) {
	beforeCount, err := q.Count()
	if err != nil {
		t.Error(err)
	}

	_, err = q.Enqueue([]byte("testing"))
	if err != nil {
		t.Error("Error while enqueueing", err)
	}

	midCount, err := q.Count()
	if err != nil {
		t.Error(err)
	}
	if midCount != beforeCount+1 {
		t.Errorf("Expected mid count %d, found %d", beforeCount+1, midCount)
	}

	item, err := q.Dequeue()
	if err != nil {
		t.Error("Error while dequeueing", err)
	}

	if string(item.Data) != "testing" {
		t.Errorf("Item mismatch: expected %s, found %s", "testing", string(item.Data))
	}

	afterCount, err := q.Count()
	if err != nil {
		t.Error(err)
	}
	if afterCount != beforeCount {
		t.Errorf("Expected after count %d, found %d", beforeCount, afterCount)
	}
}
