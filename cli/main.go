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
	"crypto/tls"
	"fmt"
	"net"
	"os"
)

const PROMPT = "ariasql>"

// ASQL is the AriaSQL CLI structure
type ASQL struct {
	history       []string        // History of statements
	historyIndex  int             // Current history index (used for up and down arrow keys)
	signalChannel chan *os.Signal // Channel to receive OS signals
	buffer        []rune          // Buffer to store the current input
	conn          *net.TCPConn    // Connection to the server
	secureConn    *tls.Conn       // Secure connection to the server
	addr          *net.TCPAddr    // Address to connect to
	authenticated bool            // Is the user authenticated?
}

// New creates a new ASQL instance
func New() *ASQL {
	return &ASQL{
		history:       make([]string, 0),
		historyIndex:  0,
		signalChannel: make(chan *os.Signal, 1),
		buffer:        make([]rune, 0),
		authenticated: false,
	}
}

// Connect connects to the AriaSQL server
func (a *ASQL) connect(host string, port int, secure bool) error {
	var err error

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

	return nil

}

// WIP!
func main() {
	//history := make([]string, 0)
	//// Create a channel to receive OS signals
	//sigs := make(chan os.Signal, 1)
	//
	//// Register the channel to receive specific signals
	//signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	//
	//err := term.Init()
	//if err != nil {
	//	panic(err)
	//}
	//
	//go func() {
	//
	//	sig := <-sigs
	//	switch sig {
	//	case syscall.SIGINT:
	//		term.Close()
	//		// Handling SIGINT (Ctrl+C) signal
	//		fmt.Println("\nReceived SIGINT, shutting down...")
	//		os.Exit(0)
	//	case syscall.SIGTERM:
	//		term.Close()
	//		// Handling SIGTERM signal
	//		fmt.Println("\nReceived SIGTERM, shutting down...")
	//		os.Exit(0)
	//	}
	//}()
	//
	//runeCh := make(chan rune)
	//
	//buffer := make([]rune, 0)
	//
	//prompt := "ariasql>"
	//
	//go func() {
	//
	//	defer term.Close()
	//
	//	for {
	//		switch ev := term.PollEvent(); ev.Type {
	//		case term.EventKey:
	//			switch ev.Key {
	//			case term.KeyCtrlC:
	//				term.Close()
	//				sigs <- syscall.SIGINT
	//				break
	//			case term.KeyEsc:
	//				term.Sync()
	//			case term.KeyArrowDown:
	//				term.Sync()
	//			case term.KeyArrowUp:
	//				// Get the last item in the history
	//				if len(history) > 0 {
	//					// Get the last item in the history
	//					lastItem := history[len(history)-1]
	//
	//					// Clear the current buffer
	//					buffer = []rune{}
	//
	//					for i := 0; i < len(prompt); i++ {
	//						runeCh <- rune(prompt[i])
	//						term.Sync()
	//					}
	//
	//					for _, r := range lastItem {
	//						runeCh <- r
	//						term.Sync()
	//					}
	//
	//				}
	//			case term.KeySpace:
	//				runeCh <- ' '
	//			case term.KeyBackspace2, term.KeyBackspace:
	//				if len(buffer) > len(prompt) {
	//					runeCh <- '\b'
	//				}
	//
	//			case term.KeyEnter:
	//				if strings.HasSuffix(string(buffer), ";") && !strings.HasSuffix(string(buffer), "\";") && !strings.HasSuffix(string(buffer), "';") {
	//					history = append(history, string(buffer[len(prompt):len(buffer)]))
	//					buffer = []rune{}
	//
	//					term.Sync()
	//
	//					// response
	//					response := []byte("OK\n")
	//
	//					for i := 0; i < len(response); i++ {
	//						runeCh <- rune(response[i])
	//						term.Sync()
	//					}
	//
	//					for i := 0; i < len(prompt); i++ {
	//						runeCh <- rune(prompt[i])
	//						term.Sync()
	//
	//					}
	//
	//				} else {
	//					term.Sync()
	//					runeCh <- '\n'
	//				}
	//
	//			default:
	//				term.Sync()
	//				runeCh <- ev.Ch
	//
	//			}
	//		case term.EventError:
	//			panic(ev.Err)
	//		}
	//	}
	//}()
	//
	//for i := 0; i < len(prompt); i++ {
	//	buffer = append(buffer, rune(prompt[i]))
	//
	//}
	//
	//for {
	//	term.Sync()
	//	fmt.Print(string(buffer))
	//	select {
	//	case r := <-runeCh:
	//		if r == '\b' {
	//			buffer = buffer[:len(buffer)-1]
	//		} else {
	//			buffer = append(buffer, r)
	//		}
	//	}
	//
	//}

}
