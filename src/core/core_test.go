// Package core tests
// AriaSQL core package tests
// Copyright (C) AriaSQL
// Author(s): Alex Gaetano Padula
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
package core

import (
	"os"
	"testing"
)

func TestNew(t *testing.T) {
	defer os.Remove("wal.dat")
	defer os.Remove("wal.dat.del")
	defer os.Remove("ariaconf.yaml")
	aria, err := New(&Config{
		DataDir: "./",
	})
	if err != nil {
		t.Fatal(err)
	}

	if aria == nil {
		t.Fatal("expected non-nil AriaSQL")
	}

	if aria.Config.DataDir != "./" {
		t.Fatalf("expected ./, got %s", aria.Config.DataDir)
	}

}

func TestAriaSQL_OpenChannel(t *testing.T) {
	defer os.Remove("wal.dat")
	defer os.Remove("wal.dat.del")
	defer os.Remove("ariaconf.yaml")
	aria, err := New(&Config{
		DataDir: "./",
	})
	if err != nil {
		t.Fatal(err)

	}

	channel := aria.OpenChannel(nil)
	if channel == nil {
		t.Fatal("expected non-nil channel")
	}

	if channel.ChannelID != 1 {
		t.Fatalf("expected 1, got %d", channel.ChannelID)
	}

	if channel.Database != nil {
		t.Fatal("expected nil database")
	}

	if channel.User != nil {
		t.Fatal("expected nil user")
	}
}

func TestAriaSQL_RemoveChannel(t *testing.T) {
	defer os.Remove("wal.dat")
	defer os.Remove("wal.dat.del")
	defer os.Remove("ariaconf.yaml")
	aria, err := New(&Config{
		DataDir: "./",
	})

	if err != nil {
		t.Fatal(err)
	}

	channel := aria.OpenChannel(nil)
	err = aria.CloseChannel(channel)
	if err != nil {
		t.Fatal(err)
	}

	if len(aria.Channels) != 0 {
		t.Fatalf("expected 0, got %d", len(aria.Channels))
	}
}
