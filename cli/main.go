// asql - AriaSQL CLI
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
package main

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/briandowns/spinner"
	"github.com/chzyer/readline"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

const PROMPT = "ariasql>"
const HISTORY_EXTENSION = ".asql_history"

// ASQL is the AriaSQL CLI structure
type ASQL struct {
	signalChannel chan os.Signal     // Channel to receive OS signals
	rl            *readline.Instance // Readline instance
	conn          *net.TCPConn       // Connection to the server
	secureConn    *tls.Conn          // Secure connection to the server
	addr          *net.TCPAddr       // Address to connect to
	authenticated bool               // Is the user authenticated?
	wg            *sync.WaitGroup    // WaitGroup to wait for goroutines to finish
	bufferSize    int                // Buffer size for reading from the connection
	header        []byte
}

// New creates a new ASQL instance
func New() (*ASQL, error) {

	return &ASQL{
		signalChannel: make(chan os.Signal, 1),
		authenticated: false,
		wg:            &sync.WaitGroup{},
		bufferSize:    0,
	}, nil
}

// Connect connects to the AriaSQL server
func (a *ASQL) connect(host string, port int, secure bool, username, password string, bufferSize int) error {
	var err error

	a.bufferSize = bufferSize

	// Resolve the string address to a TCP address
	a.addr, err = net.ResolveTCPAddr("tcp4", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}

	if secure {
		// Connect to the server using TLS
		a.secureConn, err = tls.Dial("tcp", fmt.Sprintf("%s:%d", host, port), &tls.Config{})
		if err != nil {
			return err
		}
	} else {

		// Connect to the server
		a.conn, err = net.DialTCP("tcp", nil, a.addr)
		if err != nil {
			return err
		}
	}

	// Authenticate the user
	encodedStr := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s\\0%s", username, password)))
	if a.conn != nil {
		_, err = a.conn.Write([]byte(encodedStr))
		if err != nil {
			return err
		}
	} else {
		_, err = a.secureConn.Write([]byte(encodedStr))
		if err != nil {
			return err
		}

	}

	// Get response
	response := make([]byte, a.bufferSize)
	if a.conn != nil {
		_, err = a.conn.Read(response)
		if err != nil {
			return err
		}
	} else {
		_, err = a.secureConn.Read(response)
		if err != nil {
			return err
		}

	}

	authOk := bytes.Split(response, []byte("\n"))[0]
	version := bytes.Split(response, []byte("\n"))[1]
	a.header = []byte(fmt.Sprintf(`
ARIASQL %s (c) %d all rights reserved
=================================================*
`, string(version), time.Now().Year()))

	if string(authOk) == "OK" {
		a.authenticated = true
	} else {
		return fmt.Errorf("authentication failed: %s", string(response))

	}

	return nil

}

// Close closes open connections and files
func (a *ASQL) close() {
	if a.conn != nil {
		a.conn.Close()
	}

	if a.secureConn != nil {
		a.secureConn.Close()
	}

}

// CLI entry point
func main() {
	var (
		host       = flag.String("host", "localhost", "Host of AriaSQL instance you want to connect to")
		port       = flag.Int("port", 3695, "Port of AriaSQL instance you want to connect to")
		tls        = flag.Bool("tls", false, "Use TLS to connect to AriaSQL instance")
		username   = flag.String("u", "", "AriaSQL user username")
		password   = flag.String("p", "", "ArilaSQL user password")
		bufferSize = flag.Int("buffer", 1024, "Buffer size for reading from the connection")
	)

	flag.Parse()

	if *username == "" || *password == "" {
		fmt.Println("Username and password are required")
		os.Exit(1)

	}

	asql, err := New()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	s := spinner.New(spinner.CharSets[12], 100*time.Millisecond)

	s.Color("blue", "bold")
	s.Start()
	time.Sleep(2 * time.Second)
	s.Stop()

	err = asql.connect(*host, *port, *tls, *username, *password, *bufferSize)
	if err != nil {
		fmt.Println("Unable to reach AriaSQL server: ", err.Error())
		os.Exit(1)
	}

	go func() {

		sig := <-asql.signalChannel
		switch sig {
		case syscall.SIGINT:
			asql.close()
			// Handling SIGINT (Ctrl+C) signal
			fmt.Println("\nReceived SIGINT, shutting down...")
			os.Exit(0)
		case syscall.SIGTERM:
			asql.close()
			// Handling SIGTERM signal
			fmt.Println("\nReceived SIGTERM, shutting down...")
			os.Exit(0)
		}
	}()

	fmt.Println(string(asql.header))

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 PROMPT,
		HistoryFile:            HISTORY_EXTENSION,
		DisableAutoSaveHistory: true,
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	var cmds []string
	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		cmds = append(cmds, line)
		if !strings.HasSuffix(line, ";") {
			rl.SetPrompt(">>> ")
			continue
		}
		cmd := strings.Join(cmds, " ")
		cmds = cmds[:0]
		rl.SetPrompt(PROMPT)
		rl.SaveHistory(cmd)

		tNow := time.Now()

		// Send the statement to the server
		if asql.conn != nil {
			_, err := asql.conn.Write([]byte(cmd))
			if err != nil {
				rl.Write([]byte(fmt.Sprintf("Error writing to server: %s\n", err.Error())))
				asql.signalChannel <- syscall.SIGINT
				break
			}
		} else {
			_, err := asql.secureConn.Write([]byte(cmd))
			if err != nil {
				rl.Write([]byte(fmt.Sprintf("Error writing to server: %s\n", err.Error())))
				asql.signalChannel <- syscall.SIGINT
				break
			}
		}

		// Get response
		response := make([]byte, asql.bufferSize)
		_, err = asql.conn.Read(response)
		if err != nil {
			rl.Write([]byte(fmt.Sprintf("Error reading from server: %s\n", err.Error())))
			asql.signalChannel <- syscall.SIGINT
			break
		}

		duration := fmt.Sprintf("Completed in %s\n", time.Since(tNow).String())

		fmt.Print(string(append(response, duration...)))

	}

}
