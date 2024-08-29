# GO BTree
A fast, simple persistent BTree implementation in Go.

https://pkg.go.dev/github.com/guycipher/btree

## Features
- Easy to use API with Put, Get, Delete, Remove, Iterator, Range methods
- Disk based storage
- Supports keys with multiple values
- Supports large keys and values

> [!WARNING]
> Not thread safe.  You must handle concurrency control yourself.

## Usage
### Importing
```
import "github.com/guycipher/btree"
```

### Creating a new BTree

You can use the ``Open`` method to open an existing btree or create a new one.
You can specify the file, permission and T(degree)
```
// name of the file, flags, file mode, T(degree)
bt, err := btree.Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
if err != nil {
..
}
```

### Inserting a key-value pair

You can insert a value into a key using the ``Put`` method.  Keys can store many values.
```
err := bt.Put([]byte("key"), []byte("value"))
if err != nil {
..
}
```

### Getting a value

To get a value you can you the ``Get`` method.  The get method will return all the keys values.
```
values, err := bt.Get([]byte("key"))
if err != nil {
..
}
```

### Deleting a key

To delete a key and all of it's values you can use the ``Delete`` method.
```
err := bt.Delete([]byte("key"))
if err != nil {
..
}
```

### Removing a value within key

To remove a value from a key you can use the ``Remove`` method.
```
err := bt.Remove([]byte("key"), []byte("value"))
if err != nil {
..
}
```

### Iterator

The iterator is used to iterate over values of a key

```
iterator := key.Iterator()

for {
    value, ok := iterator()
    if !ok {
        break
    }

    fmt.Println(string(value))
}
```

Result
```
value1
value2
value3
```

### Range query
```
keys, err := bt.Range([]byte("key1"), []byte("key3"))
if err != nil {
..
}
```

### Closing the BTree

You can close the BTree by calling the Close function.

```
err := bt.Close()
if err != nil {
..
}
```

## Technical Details
This is an on disk btree implementation.  This btree has an underlying pager that handles reading and writing nodes to disk as well as overflows.
When an overflow is required for a page the overflow is created and the data is split between however many pages.
When a page gets deleted its page number gets placed into an in-memory slice as well as gets written to disk. These deleted pages are reused when new pages are needed.

A key on this btree can store many values.  Mind you a keys values are read into memory; So if you have a key like A with values Alex, Alice, Adam, and you call Get(A) all of those values will be read into memory.
You can use a key iterator to iterate over the values of a key.

The btree is not thread safe.  You must handle concurrency control yourself.

You can play with page size and degree(T) to see how it affects performance.  My recommendation is a smaller page size and smaller degree for faster reads and writes.

## License
View the [LICENSE](LICENSE) file
