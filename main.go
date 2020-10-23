package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/lotsa"
)

type simplePoint struct {
	lat, lon float64
}
type simpleConn struct {
	mu      sync.Mutex //
	conn    redis.Conn // connection to server
	plmax   int        //
	plcount int        // number of commands in pipeline
}

func (conn *simpleConn) send(command string, args ...interface{}) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if err := conn.conn.Send(command, args...); err != nil {
		panic(err)
	}
	conn.plcount++
	if conn.plcount == conn.plmax {
		conn.mu.Unlock()
		defer conn.mu.Lock()
		conn.flush()
	}
}

func (conn *simpleConn) flush() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.plcount > 0 {
		if err := conn.conn.Flush(); err != nil {
			panic(err)
		}
		for conn.plcount > 0 {
			if _, err := conn.conn.Receive(); err != nil {
				panic(err)
			}
			conn.plcount--
		}
	}
}

func main() {
	addr := "localhost:9851"
	numFences := 23_000 // num geofences, SETCHAN
	radius := 5_000     // radius in meters
	pipeline := 1       // packet pipelining
	clients := 20       // number of clients
	flag.StringVar(&addr, "a", addr, "server address")
	flag.IntVar(&numFences, "n", numFences, "number of geofences")
	flag.IntVar(&radius, "r", radius, "radius in meters")
	flag.IntVar(&pipeline, "P", pipeline, "pipeline")
	flag.IntVar(&clients, "c", clients, "clients")
	flag.Parse()

	fmt.Printf(">> numFences: %d, radius: %dm, pipeline %d, clients: %d <<\n",
		numFences, radius, pipeline, clients,
	)
	///////////////////
	lotsa.Output = os.Stdout
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// prepare random points used for SETCHAN and SET
	points := make([]simplePoint, numFences)
	for i := 0; i < len(points); i++ {
		points[i] = simplePoint{
			lat: rng.Float64()*180 - 90,
			lon: rng.Float64()*360 - 180,
		}
	}

	// create client connections
	conns := make([]simpleConn, clients)
	for i := 0; i < clients; i++ {
		conn, err := redis.Dial("tcp", "localhost:9851")
		if err != nil {
			panic(err)
		}
		conns[i] = simpleConn{conn: conn, plmax: pipeline}

		defer conn.Close()
	}
	flushAllConns := func() {
		for i := 0; i < clients; i++ {
			conns[i].flush()
		}
	}

	fmt.Printf("SETCHAN      ")
	lotsa.Ops(numFences, clients, func(i, j int) {
		conns[j].send(
			"SETCHAN", i,
			"NEARBY", "fleet", "DETECT", "enter,exit",
			"POINT", points[i].lat, points[i].lon, radius)
		if i == numFences-1 {
			flushAllConns()
		}
	})

	fmt.Printf("SET-POINTS   ")
	lotsa.Ops(numFences, clients, func(i, j int) {
		conns[j].send("SET", "fleet", i, "POINT", points[i].lat, points[i].lon)
		if i == numFences-1 {
			flushAllConns()
		}
	})

	fmt.Printf("SET-SAME     ")
	lotsa.Ops(numFences, clients, func(i, j int) {
		conns[j].send("SET", "fleet", i, "POINT", points[i].lat, points[i].lon)
		if i == numFences-1 {
			flushAllConns()
		}
	})

	fmt.Printf("SET-REVERSE  ")
	lotsa.Ops(numFences, clients, func(i, j int) {
		conns[j].send(
			"SET", "fleet", i,
			"POINT", points[i].lon, points[i].lat,
		)
		if i == numFences-1 {
			flushAllConns()
		}
	})

}
