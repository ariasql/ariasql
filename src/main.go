package main

import (
	"ariasql/catalog"
	"ariasql/core"
	"ariasql/server"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// The main function starts the AriaSQL server
func main() {
	// Create a channel to receive OS signals
	sigs := make(chan os.Signal, 1)

	// Register the channel to receive specific signals
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./", // For now, can be set in aria config file
	})

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
			os.Exit(0)
		case syscall.SIGTERM:
			// Handling SIGTERM signal
			fmt.Println("Received SIGTERM, shutting down...")
			server.Stop()
			aria.Catalog.Close()
			os.Exit(0)
		}
	}()

	server.Start()

}
