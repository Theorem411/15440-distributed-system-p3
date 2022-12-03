// Package kvserver implements the backend server for a
// geographically distributed, highly available, NoSQL key-value store.
package kvserver

import (
	"fmt"
	"github.com/cmu440/actor"
	"net"
	"net/rpc"
	"runtime/debug"
	"strconv"
)

// A single server in the key-value store, running some number of
// query actors - nominally one per CPU core. Each query actor
// provides a key/value storage service on its own port.
//
// Different query actors (both within this server and across connected
// servers) periodically sync updates (Puts) following an eventually
// consistent, last-writer-wins strategy.
type Server struct {
	// TODO (3A, 3B): implement this!
	listeners   []net.Listener
	system      *actor.ActorSystem
	remoteDescs []string
}

// OPTIONAL: Error handler for ActorSystem.OnError.
//
// Print the error or call debug.PrintStack() in this function.
// When starting an ActorSystem, call ActorSystem.OnError(errorHandler).
// This can help debug server-side errors more easily.
func errorHandler(err error) {
	fmt.Println("error: %v", err)
	debug.PrintStack()
}

// Starts a server running queryActorCount query actors.
//
// The server's actor system listens for remote messages (from other actor
// systems) on startPort. The server listens for RPCs from kvclient.Clients
// on ports [startPort + 1, startPort + 2, ..., startPort + queryActorCount].
// Each of these "query RPC servers" answers queries by asking a specific
// query actor.
//
// remoteDescs contains a "description" string for each existing server in the
// key-value store. Specifically, each slice entry is the desc returned by
// an existing server's own NewServer call. The description strings are opaque
// to callers, but typically an implementation uses JSON-encoded data containing,
// e.g., actor.ActorRef's that remote servers' actors should contact.
//
// Before returning, NewServer starts the ActorSystem, all query actors, and
// all query RPC servers. If there is an error starting anything, that error is
// returned instead.
func NewServer(startPort int, queryActorCount int, remoteDescs []string) (server *Server, desc string, err error) {
	// TODO (3A, 3B): implement this!
	system, err := actor.NewActorSystem(startPort)
	// system.OnError(errorHandler)
	if err != nil {
		return nil, "", err // (3B): change desc to something else
	}
	listeners := make([]net.Listener, 0)
	peerActors := make([]*actor.ActorRef, 0)
	for i := 1; i <= queryActorCount; i++ {
		ref := system.StartActor(newQueryActor)
		receiver := &queryReceiver{ref, system}
		// for each port = startPort + i, register an rpc svr and starts serving
		ln, err := registerReceiver(receiver, startPort+i)
		if err != nil {
			return nil, "", err
		}
		// collect actorRefs and listeners
		listeners = append(listeners, ln)
		peerActors = append(peerActors, ref)
	}
	// Send the only MInit and the first-ever MInitSynch 
	broadcastInit(system, peerActors)
	// Return a new server instance // state mainly for close purpose
	svr := &Server{
		listeners:   listeners,
		system:      system,
		remoteDescs: remoteDescs,
	}
	return svr, "", nil
}

// OPTIONAL: Closes the server, including its actor system
// and all RPC servers.
//
// You are not required to implement this function for full credit; the tests end
// by calling Close but do not check that it does anything. However, you
// may find it useful to implement this so that you can run multiple/repeated
// tests in the same "go test" command without cross-test interference (in
// particular, old test servers' squatting on ports.)
//
// Likewise, you may find it useful to close a partially-started server's
// resources if there is an error in NewServer.
func (server *Server) Close() {
	server.system.Close()
	// for ln := range server.listeners {
	// 	ln.Close()
	// }
}

// ============================= helper function ==============================
func serve(rpcServer *rpc.Server, ln net.Listener) {
	for {
		conn, err := ln.Accept() // will be shut down by Close()
		if err != nil {
			return
		}
		go rpcServer.ServeConn(conn)
	}
}

func registerReceiver(receiver *queryReceiver, port int) (net.Listener, error) {
	rpcServer := rpc.NewServer()
	err := rpcServer.RegisterName("QueryReceiver", receiver)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return ln, err
	}
	go serve(rpcServer, ln)
	return ln, nil
}

func broadcastInit(system *actor.ActorSystem, peerActors []*actor.ActorRef) {
	// system.NewChannelRef() // no need to answ
	for _, ref := range peerActors {
		fmt.Printf("init actor %v\n", ref.Counter)
		system.Tell(ref, MInit{ref, peerActors})
		system.Tell(ref, MSynchInit{})
	}
}

