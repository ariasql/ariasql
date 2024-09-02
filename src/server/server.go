// Package server
// AriaSQL server package
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
	"ariasql/executor"
	"ariasql/parser"
	"bytes"
	"fmt"
	"gopkg.in/yaml.v3"
	"net"
	"os"
)

// TCPServer is the main AriaSQL Server structure
type TCPServer struct {
	Port       int    // Port to listen on, default is 3695
	Host       string // Host to listen on, default is 0.0.0.0
	listener   *net.TCPListener
	addr       *net.TCPAddr
	aria       *core.AriaSQL // AriaSQL instance pointer
	BufferSize int           // Buffer size for reading from the connection, default is 1024
	TLS        bool          // Enable TLS, default is false
	TLSCert    string        // TLS certificate file
	TLSKey     string        // TLS key file
}

// NewTCPServer creates a new TCPServer
func NewTCPServer(port int, host string, aria *core.AriaSQL, bufferSize int) (*TCPServer, error) {

	// if there is a server config file, read it and update the server struct values
	// if there is no server config file, create one with the default values as below

	// check if ariaserver.yaml exists
	// if it does, read it and update the server struct values

	// if it doesn't, create it with the default values

	if _, err := os.Stat(fmt.Sprintf("%sariaserver.yaml", aria.Config.DataDir)); os.IsNotExist(err) {
		// Resolve the string address to a TCP address
		tcpAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf("%s:%d", host, port))
		if err != nil {
			return nil, err
		}

		// Start listening for TCP connections on the given address
		listener, err := net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			return nil, err
		}
		server := &TCPServer{Port: port, Host: host, listener: listener, addr: tcpAddr, aria: aria, BufferSize: bufferSize}

		// create a new file
		f, err := os.Create(fmt.Sprintf("%sariaserver.yaml", aria.Config.DataDir))
		if err != nil {
			return nil, err
		}

		// marshal the server struct
		b, err := yaml.Marshal(server)
		if err != nil {
			return nil, err
		}

		// write the yaml to the file
		_, err = f.Write(b)

		return server, nil

	} else {
		// read the file and update the server struct values
		// if there is an error, return the error

		// if there is no error, update the server struct values

		b, err := os.ReadFile(fmt.Sprintf("%sariaserver.yaml", aria.Config.DataDir))
		if err != nil {
			return nil, err
		}

		// create a new server struct
		server := TCPServer{}

		// unmarshal the yaml file

		err = yaml.Unmarshal(b, &server)
		if err != nil {
			return nil, err
		}

		// Resolve the string address to a TCP address
		tcpAddr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf("%s:%d", server.Host, server.Port))
		if err != nil {
			return nil, err
		}

		// Start listening for TCP connections on the given address
		listener, err := net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			return nil, err
		}

		server.aria = aria
		server.listener = listener
		server.addr = tcpAddr

		return &server, nil

	}

}

// Start starts the server
func (s *TCPServer) Start() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			continue
		}

		go s.handleConnection(conn)
	}
}

// Stop stops the server
func (s *TCPServer) Stop() {
	s.listener.Close()
}

// handleConnection handles a connection
func (s *TCPServer) handleConnection(conn net.Conn) {

	// Defer closing the connection
	defer conn.Close()

	// Create a new buffer to read from the connection
	buf := make([]byte, s.BufferSize)

	// Open a new channel
	channel := s.aria.OpenChannel()
	defer s.aria.CloseChannel(channel)

	for {
		// Read from the connection
		n, err := conn.Read(buf)
		if err != nil {
			return
		}

		q := buf[:n]

		switch {
		case bytes.Equal([]byte("close"), q):
			// Close the connection
			return
		default:

			lexer := parser.NewLexer(q)

			p := parser.NewParser(lexer)
			ast, err := p.Parse()
			if err != nil {
				conn.Write(append([]byte(fmt.Sprintf("ERR: %s", err.Error())), []byte("\n")...))
				continue
			}

			exe := executor.New(s.aria, channel)
			err = exe.Execute(ast)
			if err != nil {
				// Write the error to the connection
				conn.Write(append([]byte(fmt.Sprintf("ERR: %s", err.Error())), []byte("\n")...))
				continue
			}

			// Write the response to the connection
			if len(exe.GetResultSet()) == 0 {
				conn.Write([]byte("OK\n"))
			} else {
				conn.Write(append(exe.GetResultSet(), []byte("\n")...))

			}

			// Clear the response buffer
			exe.Clear()

			continue

		}
	}

}
