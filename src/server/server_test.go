// Package server tests
// AriaSQL server package tests
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
package server

import (
	"ariasql/core"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestNewTCPServer(t *testing.T) {
	aria, err := core.New(&core.Config{
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}

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

func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func TestTCPServer_Start(t *testing.T) {
	aria, err := core.New(&core.Config{
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}

	err = aria.Catalog.Open()
	if err != nil {
		t.Fatal(err)
	}

	freePort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}

	server, err := NewTCPServer(freePort, "0.0.0.0", aria, 1024)
	if err != nil {
		t.Fatalf("Failed to create new server: %v", err)
	}

	go server.Start()

	// Wait for server to start
	time.Sleep(time.Second * 2)

	// Try to connect to the server
	conn, err := net.Dial("tcp", "0.0.0.0:"+strconv.Itoa(freePort))
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// If we reach this point, it means we were able to connect to the server
	conn.Close()

	// Stop the server
	server.Stop()
	aria.Catalog.Close()
}
