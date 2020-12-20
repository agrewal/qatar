# qatar

Qatar is a simple queuing system backed by an on-disk key-value store using
`pebble`. You can enqueue or dequeue a payload of type `[]byte`

# Usage

To create a queue, you just need to provide a directory.

```go
q, err := qatar.NewQ("/tmp/dir")
```

You can `Peek` without dequeueing by doing the following

```
// to peek a single item
item, err := q.Peek()

// To peek atmost 10 items
items, err := q.PeekMulti(10)
```

You can dequeue items like so

```go
item, err := q.Dequeue()
```

Items consist of an `Id` and a payload of `Data`

You can directly delete an item from the queue (in any position), by doing the
following

```
err := q.Delete(item.Id)
```
