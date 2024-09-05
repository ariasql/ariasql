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
	term "github.com/nsf/termbox-go"
	"log"
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
	history       []string // History of statements
	historyFile   *os.File
	historyIndex  int             // Current history index (used for up and down arrow keys)
	signalChannel chan os.Signal  // Channel to receive OS signals
	buffer        []rune          // Buffer to store the current input
	conn          *net.TCPConn    // Connection to the server
	secureConn    *tls.Conn       // Secure connection to the server
	addr          *net.TCPAddr    // Address to connect to
	authenticated bool            // Is the user authenticated?
	wg            *sync.WaitGroup // WaitGroup to wait for goroutines to finish
	runeCh        chan rune       // Channel to send runes to the terminal
	bufferSize    int             // Buffer size for reading from the connection
	header        []byte
}

// New creates a new ASQL instance
func New() (*ASQL, error) {
	var historyFile *os.File

	// Check if HISTORY_EXTENSION file exists
	if _, err := os.Stat(HISTORY_EXTENSION); os.IsNotExist(err) {
		// Create the file
		historyFile, err = os.Create(HISTORY_EXTENSION)
		if err != nil {
			return nil, err
		}
	} else {
		// Open the file
		historyFile, err = os.OpenFile(HISTORY_EXTENSION, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

	}

	buffer := make([]rune, 0)

	return &ASQL{
		history:       make([]string, 0),
		historyIndex:  0,
		signalChannel: make(chan os.Signal, 1),
		buffer:        buffer,
		authenticated: false,
		historyFile:   historyFile,
		wg:            &sync.WaitGroup{},
		runeCh:        make(chan rune),
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
%s`, string(version), time.Now().Year(), PROMPT))

	if string(authOk) == "OK" {
		for i := 0; i < len(a.header); i++ {
			a.buffer = append(a.buffer, rune(a.header[i]))
		}

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

	if a.historyFile != nil {
		a.historyFile.Close()
	}
}

// LoadHistory loads the history from the history file
func (a *ASQL) LoadHistory() error {
	_, err := a.historyFile.Seek(0, 0)
	if err != nil {
		return err
	}

	var line string
	for {
		_, err := fmt.Fscanln(a.historyFile, &line)
		if err != nil {
			break
		}

		a.history = append(a.history, line)
	}

	// We should set index to the last item in the history
	a.historyIndex = len(a.history)

	return nil

}

// AddHistory adds a statement to the history
func (a *ASQL) addHistory(statement string) error {
	a.history = append(a.history, statement)

	// seek to the end of the file
	_, err := a.historyFile.Seek(0, 2)
	if err != nil {
		return err
	}

	_, err = a.historyFile.WriteString(statement + "\n")
	if err != nil {
		return err
	}
	return nil
}

// nextHistory moves to the next history item
func (a *ASQL) nextHistory() string {
	if a.historyIndex+1 < len(a.history) {
		a.historyIndex++
	}

	return a.history[a.historyIndex]
}

// previousHistory moves to the previous history item
func (a *ASQL) previousHistory() string {
	if a.historyIndex > 0 {
		a.historyIndex--
	}

	return a.history[a.historyIndex]

}

// handleKeys handles key events as well as communication with the server
func (a *ASQL) handle() {
	defer a.wg.Done()

	err := term.Init()
	if err != nil {
		fmt.Println(err.Error())
		a.signalChannel <- syscall.SIGINT
		return
	}

	defer term.Close()

	for {
		switch ev := term.PollEvent(); ev.Type {
		case term.EventKey:
			switch ev.Key {
			case term.KeyCtrlC:
				term.Close()
				a.signalChannel <- syscall.SIGINT
				break
			case term.KeyEsc:
				term.Sync()
			case term.KeyArrowDown:
				// Get the next item in the history
				if len(a.history) > 0 {
					// Get the next item
					nextItem := a.nextHistory()

					// Clear the current buffer
					a.buffer = []rune{}

					for i := 0; i < len(PROMPT); i++ {
						a.runeCh <- rune(PROMPT[i])
						term.Sync()
					}

					for _, r := range nextItem {
						a.runeCh <- r
						term.Sync()
					}
				}
			case term.KeyArrowUp:
				// Get the last item in the history
				if len(a.history) > 0 {
					// Get the last item
					lastItem := a.previousHistory()

					// Clear the current buffer
					a.buffer = []rune{}

					// Clear the current buffer
					a.buffer = []rune{}

					for i := 0; i < len(PROMPT); i++ {
						a.runeCh <- rune(PROMPT[i])
						term.Sync()
					}

					for _, r := range lastItem {
						a.runeCh <- r
						term.Sync()
					}

				}
			case term.KeySpace:
				a.runeCh <- ' '
			case term.KeyBackspace2, term.KeyBackspace:
				if len(a.buffer) > len(PROMPT) {
					a.runeCh <- '\b'
				}

			case term.KeyEnter:
				if strings.HasSuffix(string(a.buffer), ";") && !strings.HasSuffix(string(a.buffer), "\";") && !strings.HasSuffix(string(a.buffer), "';") {
					if len(a.buffer) > len(a.header) {
						if string(a.buffer[:len(a.header)]) == string(a.header) {
							// remove header
							a.buffer = a.buffer[len(a.header):]
						}

					}

					// remove prompt
					if bytes.HasPrefix([]byte(string(a.buffer)), []byte(PROMPT)) {
						a.buffer = a.buffer[len(PROMPT):]

					}

					// Add the statement to the history
					err = a.addHistory(string(a.buffer))
					if err != nil {
						log.Println(err)
						e := "Error adding to history: " + err.Error()
						for i := 0; i < len(e); i++ {
							a.runeCh <- rune(e[i])
							term.Sync()
						}

						a.signalChannel <- syscall.SIGINT
						return
					}

					a.historyIndex = len(a.history)

					term.Sync()

					// Send the statement to the server
					if a.conn != nil {
						_, err := a.conn.Write([]byte(string(a.buffer)))
						if err != nil {
							log.Println(err)
							e := "Error writing to server: " + err.Error()
							for i := 0; i < len(e); i++ {
								a.runeCh <- rune(e[i])
								term.Sync()
							}
							a.signalChannel <- syscall.SIGINT
							break
						}
					} else {
						_, err := a.secureConn.Write([]byte(string(len(a.buffer))))
						if err != nil {
							log.Println(err)
							e := "Error writing to server: " + err.Error()
							for i := 0; i < len(e); i++ {
								a.runeCh <- rune(e[i])
								term.Sync()
							}
							a.signalChannel <- syscall.SIGINT
							break
						}
					}

					// Get response
					response := make([]byte, a.bufferSize)
					n, err := a.conn.Read(response)
					if err != nil {
						log.Println(err)
						e := "Error writing to server: " + err.Error()
						for i := 0; i < len(e); i++ {
							a.runeCh <- rune(e[i])
							term.Sync()
						}
						a.signalChannel <- syscall.SIGINT
						break
					}

					a.buffer = []rune{}

					for i := 0; i < len(append(response[:n], []byte(PROMPT)...)); i++ {
						a.runeCh <- rune(response[i])
						term.Sync()

					}

				} else {
					term.Sync()
					a.runeCh <- '\n'
				}

			default:
				term.Sync()
				a.runeCh <- ev.Ch

			}
		case term.EventError:
			log.Println("Error: ", ev.Err)
			a.signalChannel <- syscall.SIGINT
		}
	}
}

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

	err = asql.LoadHistory()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	asql.wg.Add(1)
	go asql.handle()
	s := spinner.New(spinner.CharSets[12], 100*time.Millisecond)
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
			log.Println("wtf")
			asql.close()
			term.Close()
			// Handling SIGINT (Ctrl+C) signal
			fmt.Println("\nReceived SIGINT, shutting down...")
			os.Exit(0)
		case syscall.SIGTERM:
			asql.close()
			term.Close()
			// Handling SIGTERM signal
			fmt.Println("\nReceived SIGTERM, shutting down...")
			os.Exit(0)
		}
	}()

	for {
		term.Sync()
		fmt.Print(string(asql.buffer))
		select {
		case r := <-asql.runeCh:
			if r == '\b' {
				asql.buffer = asql.buffer[:len(asql.buffer)-1]
			} else {
				asql.buffer = append(asql.buffer, r)
			}
		}

	}

}
