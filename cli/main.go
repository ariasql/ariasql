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
	"fmt"
	term "github.com/nsf/termbox-go"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	history := make([]string, 0)
	// Create a channel to receive OS signals
	sigs := make(chan os.Signal, 1)

	// Register the channel to receive specific signals
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	err := term.Init()
	if err != nil {
		panic(err)
	}

	go func() {

		sig := <-sigs
		switch sig {
		case syscall.SIGINT:
			term.Close()
			// Handling SIGINT (Ctrl+C) signal
			fmt.Println("\nReceived SIGINT, shutting down...")
			os.Exit(0)
		case syscall.SIGTERM:
			term.Close()
			// Handling SIGTERM signal
			fmt.Println("\nReceived SIGTERM, shutting down...")
			os.Exit(0)
		}
	}()

	runeCh := make(chan rune)

	buffer := make([]rune, 0)

	prompt := "ariasql>"

	go func() {

		defer term.Close()

		for {
			switch ev := term.PollEvent(); ev.Type {
			case term.EventKey:
				switch ev.Key {
				case term.KeyCtrlC:
					term.Close()
					sigs <- syscall.SIGINT
					break
				case term.KeyEsc:
					term.Sync()
				case term.KeyArrowDown:
					term.Sync()
				case term.KeyArrowUp:
					// Get the last item in the history
					if len(history) > 0 {
						// Get the last item in the history
						lastItem := history[len(history)-1]

						// Clear the current buffer
						buffer = []rune{}

						// Append the last item to the buffer
						for _, r := range lastItem {
							buffer = append(buffer, r)
						}

						for i := 0; i < len(prompt); i++ {
							runeCh <- rune(prompt[i])
							term.Sync()
						}

						for _, r := range buffer {
							runeCh <- r
							term.Sync()
						}

					}
				case term.KeySpace:
					runeCh <- ' '
				case term.KeyBackspace2, term.KeyBackspace:
					if len(buffer) > len(prompt) {
						runeCh <- '\b'
					}

				case term.KeyEnter:
					if strings.HasSuffix(string(buffer), ";") && !strings.HasSuffix(string(buffer), "\";") && !strings.HasSuffix(string(buffer), "';") {
						history = append(history, string(buffer))
						buffer = []rune{}

						term.Sync()
						for i := 0; i < len(prompt); i++ {
							runeCh <- rune(prompt[i])
							term.Sync()

						}

					} else {
						term.Sync()
						runeCh <- '\n'
					}

				default:
					term.Sync()
					runeCh <- ev.Ch

				}
			case term.EventError:
				panic(ev.Err)
			}
		}
	}()

	for i := 0; i < len(prompt); i++ {
		buffer = append(buffer, rune(prompt[i]))

	}

	for {
		term.Sync()
		log.Println(string(buffer))
		select {
		case r := <-runeCh:
			if r == '\b' {
				buffer = buffer[:len(buffer)-1]
			} else {
				buffer = append(buffer, r)
			}
		}

	}

	//// Get input from the user
	//reader := bufio.NewReader(os.Stdin)
	//
	//// Prompt the user
	//fmt.Print("Enter input (type 'exit' to quit)\n")
	//fmt.Print("ariasql>")
	//
	//for {
	//	var inputBuilder strings.Builder
	//	for {
	//
	//		// Get input from the user
	//		// If the user types "exit", break the loop
	//
	//		// Read a line of input
	//		line, err := reader.ReadString('\n')
	//		if err != nil {
	//			fmt.Println("Error reading input:", err)
	//			continue
	//		}
	//
	//		// Append the line to the inputBuilder
	//		inputBuilder.WriteString(line)
	//
	//		// Check if the line contains a semicolon
	//		if strings.HasSuffix(line, ";\n") && !strings.HasSuffix(line, "\";\n") && !strings.HasSuffix(line, "';\n") {
	//			break
	//		}
	//
	//	}
	//
	//	// Get the complete input and trim the trailing semicolon and newline
	//	fullInput := inputBuilder.String()
	//	fullInput = strings.TrimSpace(fullInput)
	//	fullInput = strings.TrimSuffix(fullInput, ";")
	//
	//	log.Println("Full input:", fullInput)
	//
	//}

}
