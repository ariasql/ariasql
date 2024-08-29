// Package btree
// BTree implementation tests
// Copyright (C) Alex Gaetano Padula
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package btree

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

func TestOpen(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	// check for btree.db and btree.db.del files

	_, err = os.Stat("btree.db")
	if err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat("btree.db.del")
	if err != nil {
		t.Fatal(err)
	}

}

func TestBTree_Close(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	err = btree.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestBTree_Put(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {

		err := btree.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}

	}

	//btree.PrintTree()

	for i := 0; i < 500; i++ {
		key, err := btree.Get([]byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}

		if key == nil {
			t.Fatal("expected key to be not nil")
		}
	}
}

func TestBTree_Delete(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {

		err := btree.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}

	}

	//btree.PrintTree()

	for i := 0; i < 500; i++ {
		err := btree.Delete([]byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
		key, err := btree.Get([]byte(strconv.Itoa(i)))
		if key != nil {
			t.Fatalf("expected key to be nil")
		}
	}
}

func TestBTree_Range(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("%03d", i) // pad the key with leading zeros
		err := btree.Put([]byte(key), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, err := btree.Range([]byte("010"), []byte("020")) // use padded keys
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 11 {
		t.Fatalf("expected 11 keys, got %d", len(keys))
	}

}

func TestBTree_Remove(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	// put 100 values into a key

	for i := 0; i < 100; i++ {
		err := btree.Put([]byte("key"), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// remove 50 values from the key
	for i := 0; i < 50; i++ {
		err := btree.Remove([]byte("key"), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// get the key
	key, err := btree.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	if len(key.V) != 50 {
		t.Fatalf("expected 50 keys, got %d", len(key.V))
	}
}

func BenchmarkBTree_Put(b *testing.B) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		b.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < b.N; i++ {
		err := btree.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		if err != nil {
			b.Fatal(err)
		}
	}
}
