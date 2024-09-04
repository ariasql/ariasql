// Package server tests
// AriaSQL server package tests
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
package server

import (
	"ariasql/core"
	"net"
	"os"
	"testing"
	"time"
)

func TestNewTCPServer(t *testing.T) {
	aria := core.New(&core.Config{
		DataDir: "./",
	})

	server, err := NewTCPServer(3695, "0.0.0.0", aria, 1024)
	if err != nil {
		t.Fatalf("Failed to create new server: %v", err)
	}

	if server.Port != 3695 {
		t.Errorf("Expected port to be 3695, got %d", server.Port)
	}

	if server.Host != "0.0.0.0" {
		t.Errorf("Expected host to be 0.0.0.0, got %s", server.Host)
	}

	if server.BufferSize != 1024 {
		t.Errorf("Expected buffer size to be 1024, got %d", server.BufferSize)
	}
}

func TestTCPServer_Start(t *testing.T) {
	defer os.RemoveAll("databases")
	defer os.Remove("ariasql.log")
	defer os.Remove(".ariaconfig")
	defer os.Remove("ariaserver.yaml")
	aria := core.New(&core.Config{
		DataDir: "./",
	})

	err := aria.Catalog.Open()
	if err != nil {
		t.Fatal(err)
	}

	server, err := NewTCPServer(3695, "0.0.0.0", aria, 1024)
	if err != nil {
		t.Fatalf("Failed to create new server: %v", err)
	}

	go server.Start()

	// Wait for server to start
	time.Sleep(time.Second * 2)

	// Try to connect to the server
	conn, err := net.Dial("tcp", "0.0.0.0:3695")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// If we reach this point, it means we were able to connect to the server
	conn.Close()

	// Stop the server
	server.Stop()
	aria.Catalog.Close()
}
