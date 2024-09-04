// Package core
// AriaSQL core package
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
package core

import (
	"ariasql/catalog"
	"ariasql/shared"
	"ariasql/wal"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
)

// AriaSQL is the core of the database system
type AriaSQL struct {
	Config       *Config          // DataDir is the directory where the data is stored
	Catalog      *catalog.Catalog // Catalog is the root of the database catalog
	Channels     []*Channel       // Channel to the database, could be through shell or network
	ChannelsLock *sync.Mutex      // Channels lock
	WAL          *wal.WAL         // Write ahead log
}

// Channel is a connection to the database
type Channel struct {
	ChannelID uint64
	Database  *catalog.Database // Current database, this would be a result of using the USE command
	User      *catalog.User     // Current user, this would be a result of using the USE command
}

// Config is the configuration for AriaSQL
type Config struct {
	// The path to the data directory
	DataDir string
}

// New creates a new AriaSQL object
// Can pass nil to use default configuration
func New(config *Config) *AriaSQL {

	if config == nil {
		config = &Config{}
		config.DataDir = shared.GetDefaultDataDir()
	}

	// check if data directory exists
	if _, err := os.Stat(config.DataDir); os.IsNotExist(err) {
		os.Mkdir(config.DataDir, os.ModePerm)

	}

	wal, err := wal.OpenWAL(fmt.Sprintf("%swal.dat", config.DataDir), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println(err)
		return nil

	}

	//gob.Register(parser.ShowStmt{})
	//gob.Register(parser.DescribeStmt{})
	//gob.Register(parser.BeginStmt{})
	//gob.Register(parser.CommitStmt{})
	//gob.Register(parser.RollbackStmt{})
	//gob.Register(parser.AlterTableStmt{})
	//gob.Register(parser.DropColumnStmt{})

	return &AriaSQL{
		Config: config,
		Catalog: &catalog.Catalog{
			Directory: config.DataDir,
		},
		WAL:          wal,
		ChannelsLock: &sync.Mutex{},
	}
}

// OpenChannel opens a new channel to database
func (ariasql *AriaSQL) OpenChannel() *Channel {
	ariasql.ChannelsLock.Lock()
	defer ariasql.ChannelsLock.Unlock()
	channel := &Channel{
		ChannelID: uint64(len(ariasql.Channels) + 1),
	}

	ariasql.Channels = append(ariasql.Channels, channel)

	return channel
}

// CloseChannel closes a channel
func (ariasql *AriaSQL) CloseChannel(channel *Channel) error {
	ariasql.ChannelsLock.Lock()
	defer ariasql.ChannelsLock.Unlock()

	for i, ch := range ariasql.Channels {
		if ch.ChannelID == channel.ChannelID {
			ariasql.Channels = append(ariasql.Channels[:i], ariasql.Channels[i+1:]...)
			return nil
		}
	}

	return errors.New("channel not found")
}

// GetChannel returns a channel by ID
func (ariasql *AriaSQL) GetChannel(channelID uint64) *Channel {
	for _, ch := range ariasql.Channels {
		if ch.ChannelID == channelID {
			return ch
		}
	}

	return nil

}
