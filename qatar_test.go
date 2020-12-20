package qatar

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

var q *Q

func TestMain(m *testing.M) {
	// Setup
	name, err := ioutil.TempDir("", "qatar_test_")
	if err != nil {
		panic("Could not create tmp directory")
	}
	q, err = NewQ(name)
	if err != nil {
		panic("Could not setup queue")
	}
	defer q.Close()
	code := m.Run()

	// Cleanup
	os.RemoveAll(name)

	os.Exit(code)
}

func TestPeek(t *testing.T) {
	data := [2]string{"test1", "test2"}
	for _, d := range data {
		_, err := q.Enqueue([]byte(d))
		if err != nil {
			t.Error("Error while enqueueing", err)
		}
		time.Sleep(2 * time.Second)
	}

	cnt, err := q.Count()
	if err != nil {
		t.Error(err)
	}

	if cnt != 2 {
		t.Error("Count did not right number of items in the queue")
	}

	items, err := q.PeekMulti(2)
	if err != nil {
		t.Error(err)
	}
	if len(items) != 2 {
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
