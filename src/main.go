// main
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
	"ariasql/catalog"
	"ariasql/core"
	"ariasql/executor"
	"ariasql/server"
	"ariasql/shared"
	"ariasql/wal"
	"flag"
	"fmt"
	"github.com/briandowns/spinner"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// The main function starts the AriaSQL server
// you can pass the -recover flag to recover the AriaSQL instance from the WAL if it was not shut down properly, crashed, etc
func main() {

	var (
		recov     = flag.Bool("recover", false, "Recover AriaSQL instance from WAL")
		recovFile = flag.String("wal", "wal.dat", "Recover AriaSQL instance from WAL file")
	)

	flag.Parse()

	if *recov {
		fmt.Println("Recovering AriaSQL instance from WAL...")

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			var w *wal.WAL
			var err error

			// Will look in default data directory for wal.dat unless specified
			if *recovFile == "" {
				w, err = wal.OpenWAL(shared.GetDefaultDataDir()+shared.GetOsPathSeparator()+"wal.dat", os.O_CREATE|os.O_RDWR, 0644)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			} else {
				w, err = wal.OpenWAL(*recovFile, os.O_CREATE|os.O_RDWR, 0644)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}

			defer w.Close()

			ex := executor.New(nil, nil)
			ex.SetRecover(true) // set true to avoid checking permissions

			asts, err := w.RecoverASTs()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			err = ex.Recover(asts)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}()

		s := spinner.New(spinner.CharSets[12], 100*time.Millisecond)

		s.Color("blue", "bold")
		s.Start()
		wg.Wait()
		s.Stop()

		fmt.Sprintf("AriaSQL instance recovered from WAL successfully")

		os.Exit(0)

	} else {

		// Create a channel to receive OS signals
		sigs := make(chan os.Signal, 1)

		// Register the channel to receive specific signals
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		// Create a new AriaSQL instance
		aria, err := core.New(nil)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		aria.Catalog = catalog.New(aria.Config.DataDir)

		if err := aria.Catalog.Open(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		aria.Channels = make([]*core.Channel, 0)
		aria.ChannelsLock = &sync.Mutex{}

		server, err := server.NewTCPServer(3695, "0.0.0.0", aria, 1024)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		go func() {
			sig := <-sigs
			switch sig {
			case syscall.SIGINT:
				// Handling SIGINT (Ctrl+C) signal
				fmt.Println("Received SIGINT, shutting down...")
				server.Stop()
				aria.Catalog.Close()
				aria.WAL.Close()
				os.Exit(0)
			case syscall.SIGTERM:
				// Handling SIGTERM signal
				fmt.Println("Received SIGTERM, shutting down...")
				server.Stop()
				aria.Catalog.Close()
				aria.WAL.Close()
				os.Exit(0)
			}
		}()

		server.Start()
	}

}
