// Package btree
// A BTree implementation optimized for disk storage.
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
	"errors"
	"fmt"
	"github.com/hashicorp/go-msgpack/codec"
	"os"
	"strings"
)

// BTree is the main BTree struct
// ** not thread safe
type BTree struct {
	Pager *Pager // The pager for the btree
	T     int    // The order of the tree
}

// Key is the key struct for the BTree
type Key struct {
	K []byte   // The key
	V [][]byte // The values
}

// Node is the node struct for the BTree
type Node struct {
	Page     int64   // The page number of the node
	Keys     []*Key  // The keys in node
	Children []int64 // The children of the node
	Leaf     bool    // If the node is a leaf node
}

// Open opens a new or existing BTree
func Open(name string, flag, perm int, t int) (*BTree, error) {
	if t < 2 {
		return nil, errors.New("t must be greater than 1")

	}

	pager, err := OpenPager(name, flag, os.FileMode(perm))
	if err != nil {
		return nil, err
	}

	return &BTree{
		T:     t,
		Pager: pager,
	}, nil
}

// Close closes the BTree
func (b *BTree) Close() error {
	return b.Pager.Close()
}

// encodeNode encodes a node into a byte slice
func encodeNode(n *Node) ([]byte, error) {
	// Create a new msgpack handle
	handle := new(codec.MsgpackHandle)

	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, handle)
	err := enc.Encode(n)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

// newBTreeNode creates a new BTree node
func (b *BTree) newBTreeNode(leaf bool) (*Node, error) {
	var err error

	newNode := &Node{
		Leaf: leaf,
		Keys: make([]*Key, 0),
	}

	// we encode the new node
	encodedNode, err := encodeNode(newNode)
	if err != nil {
		return nil, err

	}

	// we write the new node to the pager
	newNode.Page, err = b.Pager.Write(encodedNode)
	if err != nil {
		return nil, err
	}

	encodedNode, err = encodeNode(newNode)
	if err != nil {
		return nil, err

	}

	// Write updated node
	err = b.Pager.WriteTo(newNode.Page, encodedNode)
	if err != nil {
		return nil, err
	}

	// we return the new node
	return newNode, nil
}

// decodeNode decodes a byte slice into a node
func decodeNode(data []byte) (*Node, error) {
	// Create a new msgpack handle
	handle := new(codec.MsgpackHandle)

	var n *Node

	dec := codec.NewDecoderBytes(data, handle)
	err := dec.Decode(&n)
	if err != nil {
		return nil, err

	}

	return n, nil

}

// getRoot returns the root of the BTree
func (b *BTree) getRoot() (*Node, error) {

	root, err := b.Pager.GetPage(0)
	if err != nil {
		if err.Error() == "EOF" {
			// create root
			// initial root if a leaf node and starts at page 0
			rootNode := &Node{
				Leaf:     true,
				Page:     0,
				Children: make([]int64, 0),
				Keys:     make([]*Key, 0),
			}

			// encode the root node
			encodedRoot, err := encodeNode(rootNode)
			if err != nil {
				return nil, err
			}

			// write the root to the file
			err = b.Pager.WriteTo(0, encodedRoot)
			if err != nil {

				return nil, err

			}

			return rootNode, nil
		} else {
			return nil, err
		}
	}

	// decode the root
	rootNode, err := decodeNode(root)
	if err != nil {

		return nil, err
	}

	return rootNode, nil
}

// splitRoot splits the root node
func (b *BTree) splitRoot() error {

	oldRoot, err := b.getRoot()
	if err != nil {
		return err
	}

	// Create new node (this will be the new "old root")
	newOldRoot, err := b.newBTreeNode(oldRoot.Leaf)
	if err != nil {
		return err
	}

	// Copy keys and children from old root to new old root
	newOldRoot.Keys = oldRoot.Keys
	newOldRoot.Children = oldRoot.Children

	// Create new root and make new old root a child of new root
	newRoot := &Node{
		Page:     0, // New root takes the old root's page number
		Children: []int64{newOldRoot.Page},
	}

	// Split new old root and move median key up to new root
	err = b.splitChild(newRoot, 0, newOldRoot)
	if err != nil {
		return err
	}

	// encoded new root
	encodedNewRoot, err := encodeNode(newRoot)
	if err != nil {
		return err

	}

	// Write new root and new old root to file
	err = b.Pager.WriteTo(newRoot.Page, encodedNewRoot)
	if err != nil {
		return err
	}

	// encoded new old root
	encodedNewOldRoot, err := encodeNode(newOldRoot)
	if err != nil {
		return err

	}

	err = b.Pager.WriteTo(newOldRoot.Page, encodedNewOldRoot)
	if err != nil {
		return err
	}

	return nil
}

// splitChild splits a child node of x at index i
func (b *BTree) splitChild(x *Node, i int, y *Node) error {
	z, err := b.newBTreeNode(y.Leaf)
	if err != nil {
		return err
	}

	z.Keys = append(z.Keys, y.Keys[b.T:]...)
	y.Keys = y.Keys[:b.T]

	if !y.Leaf {
		z.Children = append(z.Children, y.Children[b.T:]...)
		y.Children = y.Children[:b.T]
	}

	x.Keys = append(x.Keys, nil)
	x.Children = append(x.Children, 0)

	for j := len(x.Keys) - 1; j > i; j-- {
		x.Keys[j] = x.Keys[j-1]
	}
	x.Keys[i] = y.Keys[b.T-1]

	// remove the key from y
	y.Keys = y.Keys[:b.T-1]

	for j := len(x.Children) - 1; j > i+1; j-- {
		x.Children[j] = x.Children[j-1]
	}
	x.Children[i+1] = z.Page

	// encode y
	encodedY, err := encodeNode(y)
	if err != nil {
		return err
	}

	err = b.Pager.WriteTo(y.Page, encodedY)
	if err != nil {
		return err
	}

	// encode z
	encodedZ, err := encodeNode(z)
	if err != nil {
		return err
	}

	err = b.Pager.WriteTo(z.Page, encodedZ)
	if err != nil {
		return err
	}

	// encode x
	encodedX, err := encodeNode(x)
	if err != nil {
		return err
	}

	err = b.Pager.WriteTo(x.Page, encodedX)
	if err != nil {
		return err
	}

	return nil
}

// Put inserts a key into the BTree
// A key can have multiple values
// Put inserts a key value pair into the BTree
func (b *BTree) Put(key, value []byte) error {

	root, err := b.getRoot()
	if err != nil {
		return err
	}

	if len(root.Keys) == (2*b.T)-1 {

		err = b.splitRoot()
		if err != nil {
			return err
		}

		rootBytes, err := b.Pager.GetPage(0)
		if err != nil {
			return err
		}

		root, err = decodeNode(rootBytes)
		if err != nil {
			return err
		}
	}

	err = b.insertNonFull(root, key, value)
	if err != nil {
		return err
	}

	return nil

}

// insertNonFull inserts a key into a non-full node
func (b *BTree) insertNonFull(x *Node, key []byte, value []byte) error {
	i := len(x.Keys) - 1

	if x.Leaf {
		for i >= 0 && lessThan(key, x.Keys[i].K) {
			i--
		}

		// If key exists, append the value
		if i >= 0 && equal(key, x.Keys[i].K) {

			x.Keys[i].V = append(x.Keys[i].V, value)

			// encode the node
			encodedNode, err := encodeNode(x)
			if err != nil {
				return err
			}

			err = b.Pager.WriteTo(x.Page, encodedNode)
			if err != nil {
				return err
			}

			return nil
		} else {

			// If key doesn't exist, insert new key and value
			x.Keys = append(x.Keys, nil)
			j := len(x.Keys) - 1
			for j > i+1 {
				x.Keys[j] = x.Keys[j-1]
				j--
			}

			values := make([][]byte, 0)
			values = append(values, value)
			x.Keys[j] = &Key{K: key, V: values}

		}

		// encode the node
		encodedNode, err := encodeNode(x)
		if err != nil {
			return err
		}

		err = b.Pager.WriteTo(x.Page, encodedNode)
		if err != nil {
			return err
		}

		return nil

	} else {
		for i >= 0 && lessThan(key, x.Keys[i].K) {
			i--
		}
		i++
		childBytes, err := b.Pager.GetPage(x.Children[i])
		if err != nil {
			return err
		}

		child, err := decodeNode(childBytes)
		if err != nil {
			return err
		}

		if len(child.Keys) == (2*b.T)-1 {

			err = b.splitChild(x, i, child)
			if err != nil {
				return err
			}

			if greaterThan(key, x.Keys[i].K) {
				i++
			}

		}

		childBytes, err = b.Pager.GetPage(x.Children[i])
		if err != nil {
			return err
		}

		child, err = decodeNode(childBytes)
		if err != nil {
			return err
		}

		err = b.insertNonFull(child, key, value)
		if err != nil {
			return err
		}

	}
	return nil
}

// lessThan compares two values and returns true if a is less than b
func lessThan(a, b []byte) bool {

	return bytes.Compare(a, b) < 0

	return false
}

// greaterThan compares two values and returns true if a is greater than b
func greaterThan(a, b []byte) bool {

	return bytes.Compare(a, b) > 0

	return false
}

// equal compares two values and returns true if a is equal than b
func equal(a, b []byte) bool {

	return bytes.Equal(a, b)

	return false
}

// notEq compares two values and returns true if a is not equal to b
func notEq(a, b []byte) bool {

	return !bytes.Equal(a, b)

}

// PrintTree prints the tree (for debugging purposes ****)
func (b *BTree) PrintTree() error {
	root, err := b.getRoot()
	if err != nil {
		return err
	}
	err = b.printTree(root, "", true)
	if err != nil {
		return err
	}
	return nil
}

func (b *BTree) printTree(node *Node, indent string, last bool) error {
	fmt.Print(indent)
	if last {
		fmt.Print("└── ")
		indent += "    "
	} else {
		fmt.Print("├── ")
		indent += "│   "
	}

	for _, key := range node.Keys {
		fmt.Printf("%v ", string(key.K))
	}
	fmt.Println()

	for i, child := range node.Children {
		cBytes, err := b.Pager.GetPage(child)
		if err != nil {
			return err
		}

		c, err := decodeNode(cBytes)
		if err != nil {
			return err
		}

		b.printTree(c, indent, i == len(node.Children)-1)
	}

	return nil
}

// Get returns the values associated with a key
func (b *BTree) Get(k []byte) (*Key, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	return b.searchRecursive(root, k)

}

// searchRecursive searches for a key in the BTree
func (b *BTree) searchRecursive(x *Node, k []byte) (*Key, error) {

	i := 0

	x.Keys = removeNilFromKeys(x.Keys)

	for i < len(x.Keys) && greaterThan(k, x.Keys[i].K) {
		i++
	}

	// If the key is found in the node, return true
	if i < len(x.Keys) && equal(k, x.Keys[i].K) {
		return x.Keys[i], nil
	} else if x.Leaf {
		return nil, nil
	} else {
		childBytes, err := b.Pager.GetPage(x.Children[i])
		if err != nil {
			return nil, err
		}

		child, err := decodeNode(childBytes)
		if err != nil {
			return nil, err
		}

		return b.searchRecursive(child, k)
	}
}

// Remove removes a value from key
func (b *BTree) Remove(key, value []byte) error {
	root, err := b.getRoot()
	if err != nil {
		return err
	}

	return b.remove(root, key, value)

}

// remove removes a value from a key
func (b *BTree) remove(x *Node, key, value []byte) error {

	i := 0
	for i < len(x.Keys) && greaterThan(key, x.Keys[i].K) {
		i++
	}

	// If the key is found in the node, return true
	if i < len(x.Keys) && equal(key, x.Keys[i].K) {
		// remove the value from the key

		for j := 0; j < len(x.Keys[i].V); j++ {
			if bytes.Equal(x.Keys[i].V[j], value) {
				x.Keys[i].V = append(x.Keys[i].V[:j], x.Keys[i].V[j+1:]...)
				break
			}
		}

		// if the key has no values, remove the key
		if len(x.Keys[i].V) == 0 {
			// @TODO: remove the key from the node
			return nil
		}

		// encode the node
		encodedNode, err := encodeNode(x)
		if err != nil {
			return err
		}

		err = b.Pager.WriteTo(x.Page, encodedNode)
		if err != nil {
			return err
		}

		return nil
	} else if x.Leaf {
		return errors.New("key not found")
	} else {
		childBytes, err := b.Pager.GetPage(x.Children[i])
		if err != nil {
			return err
		}

		child, err := decodeNode(childBytes)
		if err != nil {
			return err
		}

		return b.remove(child, key, value)
	}
}

// Delete deletes a key from the BTree
func (b *BTree) Delete(k []byte) error {

	root, err := b.getRoot()
	if err != nil {
		return err
	}

	err = b.deleteRecursive(root, k)
	if err != nil {
		return err
	}

	return nil
}

// deleteRecursive deletes a key from the BTree
func (b *BTree) deleteRecursive(x *Node, k []byte) error {

	x.Keys = removeNilFromKeys(x.Keys)

	i := 0
	for i < len(x.Keys) && greaterThan(k, x.Keys[i].K) {
		i++
	}

	if i < len(x.Keys) && equal(k, x.Keys[i].K) {
		if x.Leaf {

			x.Keys = append(x.Keys[:i], x.Keys[i+1:]...)

			x.Keys = removeNilFromKeys(x.Keys)

			// encode the node
			encodedNode, err := encodeNode(x)
			if err != nil {
				return err
			}

			err = b.Pager.WriteTo(x.Page, encodedNode)
			if err != nil {
				return err
			}

			return nil
		} else {
			// x is not a leaf

			predecessor, err := b.findPredecessor(x, i)
			if err != nil {
				return err
			}

			x.Keys[i] = predecessor

			// encode the node
			encodedNode, err := encodeNode(x)
			if err != nil {
				return err
			}

			err = b.Pager.WriteTo(x.Page, encodedNode)
			if err != nil {
				return err
			}

			childBytes, err := b.Pager.GetPage(x.Children[i])
			if err != nil {
				if strings.Contains(err.Error(), "EOF") {

					return nil
				}
				return err
			}

			child, err := decodeNode(childBytes)
			if err != nil {
				return err
			}

			if predecessor == nil || child == nil {
				return nil
			}

			err = b.deleteRecursive(child, predecessor.K) // delete the predecessor
			if err != nil {
				return err
			}

			return nil // return without error if key is found
		}
	} else {
		if x.Leaf {
			return nil // return without error if key is not found
		} else {

			childBytes, err := b.Pager.GetPage(x.Children[i])
			if err != nil {
				if strings.Contains(err.Error(), "EOF") {
					return nil
				}
				return err
			}

			child, err := decodeNode(childBytes)
			if err != nil {
				return err
			}

			err = b.deleteRecursive(child, k)
			if err != nil {
				return err
			}

		}
	}

	if len(x.Children) > 0 {

		if i+1 < len(x.Children) {

			childBytes, err := b.Pager.GetPage(x.Children[i])
			if err != nil {
				if strings.Contains(err.Error(), "EOF") {
					return nil
				}
				return err
			}

			child, err := decodeNode(childBytes)
			if err != nil {
				return err
			}

			if !x.Leaf && len(child.Keys) < b.T-1 {

				err := b.mergeNodes(x, i)
				if err != nil {
					return err
				}

			}

			return nil

		}

	}

	return nil
}

func (b *BTree) findPredecessor(x *Node, i int) (*Key, error) {

	curBytes, err := b.Pager.GetPage(x.Children[i])
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			return nil, nil
		}
		return nil, err
	}

	cur, err := decodeNode(curBytes)
	if err != nil {
		return nil, err
	}

	for !cur.Leaf {

		curBytes, err = b.Pager.GetPage(cur.Children[len(cur.Children)-1])
		if err != nil {
			return nil, err
		}

		cur, err = decodeNode(curBytes)
		if err != nil {
			return nil, err
		}

		if len(cur.Keys) == 0 {
			return nil, nil
		}

		return cur.Keys[len(cur.Keys)-1], nil

	}

	if len(cur.Keys) == 0 {
		return nil, nil
	}

	return cur.Keys[len(cur.Keys)-1], nil
}

func (b *BTree) mergeNodes(x *Node, i int) error {

	if len(x.Children) == i+1 {
		return nil
	}

	child1Bytes, err := b.Pager.GetPage(x.Children[i])
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			return nil
		}
		return err
	}

	child2Bytes, err := b.Pager.GetPage(x.Children[i+1])
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			return nil
		}
		return err
	}

	child1, err := decodeNode(child1Bytes)
	if err != nil {
		return err

	}

	child2, err := decodeNode(child2Bytes)
	if err != nil {
		return err

	}

	child1.Keys = append(child1.Keys, x.Keys[i])
	child1.Keys = append(child1.Keys, child2.Keys...)
	child1.Children = append(child1.Children, child2.Children...)
	x.Keys = append(x.Keys[:i], x.Keys[i+1:]...)
	x.Children = append(x.Children[:i+1], x.Children[i+2:]...)

	x.Keys = removeNilFromKeys(x.Keys)

	// encode the node
	encodedNode, err := encodeNode(x)
	if err != nil {
		return err
	}

	err = b.Pager.WriteTo(x.Page, encodedNode)
	if err != nil {
		return err
	}

	child1.Keys = removeNilFromKeys(child1.Keys)

	// encode the node
	encodedNode, err = encodeNode(child1)
	if err != nil {
		return err
	}

	err = b.Pager.WriteTo(child1.Page, encodedNode)
	if err != nil {
		return err
	}

	child2.Keys = removeNilFromKeys(child2.Keys)

	return b.Pager.DeletePage(child2.Page)
}

// findNodeForKey finds the node for a key
func (b *BTree) findNodeForKey(x *Node, key []byte) (*Node, int, error) {
	i := 0
	for i < len(x.Keys) && lessThan(x.Keys[i].K, key) {
		i++
	}

	if i < len(x.Keys) && equal(key, x.Keys[i].K) {
		return x, i, nil
	} else if !x.Leaf {
		childBytes, err := b.Pager.GetPage(x.Children[i])
		if err != nil {
			return nil, 0, err
		}

		child, err := decodeNode(childBytes)
		if err != nil {
			return nil, 0, err
		}

		return b.findNodeForKey(child, key)
	}

	return nil, 0, errors.New("key not found")
}

// Iterator returns an iterator for a key
func (k *Key) Iterator() func() ([]byte, bool) {
	index := 0
	return func() ([]byte, bool) {
		if index >= len(k.V) {
			return nil, false
		}
		value := k.V[index]
		index++
		return value, true
	}
}

// Range returns all keys in the BTree that are within the range [start, end]
func (b *BTree) Range(start, end []byte) ([]interface{}, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	return b.rangeKeys(start, end, root)
}

// lessThanEq compares two values and returns true if a is less than or equal to b
func lessThanEq(a, b []byte) bool {
	return bytes.Compare(a, b) <= 0
	return false

}

// rangeKeys returns all keys in the BTree that are within the range [start, end]
func (b *BTree) rangeKeys(start, end []byte, x *Node) ([]interface{}, error) {
	keys := make([]interface{}, 0)
	if x != nil {

		i := 0
		for i < len(x.Keys) && lessThan(x.Keys[i].K, start) {
			i++
		}
		for i < len(x.Keys) && lessThanEq(x.Keys[i].K, end) {
			if !x.Leaf {
				childBytes, err := b.Pager.GetPage(x.Children[i])
				if err != nil {
					return nil, err
				}

				child, err := decodeNode(childBytes)
				if err != nil {
					return nil, err
				}

				childKeys, err := b.rangeKeys(start, end, child)
				if err != nil {
					return nil, err
				}
				keys = append(keys, childKeys...)
			}
			keys = append(keys, x.Keys[i])
			i++
		}
		if !x.Leaf && i < len(x.Children) {
			childBytes, err := b.Pager.GetPage(x.Children[i])
			if err != nil {
				return nil, err
			}

			child, err := decodeNode(childBytes)
			if err != nil {
				return nil, err
			}

			childKeys, err := b.rangeKeys(start, end, child)
			if err != nil {
				return nil, err
			}
			keys = append(keys, childKeys...)
		}
	}
	return keys, nil
}

// removeNilFromKeys removes nil keys from a slice of keys
func removeNilFromKeys(keys []*Key) []*Key {
	newKeys := make([]*Key, 0)
	for _, key := range keys {
		if key != nil {
			newKeys = append(newKeys, key)
		}
	}
	return newKeys
}
