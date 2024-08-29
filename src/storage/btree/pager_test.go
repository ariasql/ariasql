// Package btree
// File pager tests
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
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestOpenPager(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")
	pager, err := OpenPager("btree.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	defer pager.Close()

	if pager == nil {
		t.Fatal("expected non-nil pager")
	}

}

func TestPager_Write(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	pager, err := OpenPager("btree.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer pager.Close()

	pageID, err := pager.Write([]byte("Hello World"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = pager.Write([]byte("Hello World 2"))
	if err != nil {
		t.Fatal(err)
	}

	// Get the page 0
	data, err := pager.GetPage(pageID)
	if err != nil {
		t.Fatal(err)
	}

	if string(bytes.ReplaceAll(data, []byte("\x00"), []byte(""))) != "Hello World" {
		t.Fatalf("expected Hello World, got %s", string(bytes.ReplaceAll(data, []byte("\x00"), []byte(""))))
	}

}

func TestPager_Write2(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	pager, err := OpenPager("btree.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer pager.Close()

	for i := 0; i < 10000; i++ {
		_, err := pager.Write([]byte(fmt.Sprintf("Hello World %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPager_Count(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	pager, err := OpenPager("btree.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer pager.Close()

	for i := 0; i < 1000; i++ {
		_, err := pager.Write([]byte(fmt.Sprintf("Hello World %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	count := pager.Count()

	if count != 1000 {
		t.Fatalf("expected 1000, got %d", count)
	}
}
