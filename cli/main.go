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

import "flag"

func main() {
	var (
		host = flag.String("host", "localhost", "Host of AriaSQL instance you want to connect to")
		port = flag.Int("port", 3695, "Port of AriaSQL instance you want to connect to")
		tls  = flag.Bool("tls", false, "Use TLS to connect to AriaSQL instance")
	)

	flag.Parse()

}
