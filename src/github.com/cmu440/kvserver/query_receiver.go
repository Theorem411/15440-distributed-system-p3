package kvserver

import (
	"fmt"
	"github.com/cmu440/actor"
	"github.com/cmu440/kvcommon"
)

// RPC handler implementing the kvcommon.QueryReceiver interface.
// There is one queryReceiver per queryActor, each running on its own port,
// created and registered for RPCs in NewServer.
//
// A queryReceiver MUST answer RPCs by sending a message to its query
// actor and getting a response message from that query actor (via
// ActorSystem's NewChannelRef). It must NOT attempt to answer queries
// using its own state, and it must NOT directly coordinate with other
// queryReceivers - all coordination is done within the actor system
// by its query actor.
type queryReceiver struct {
	// TODO (3A): implement this!
	ref    *actor.ActorRef
	system *actor.ActorSystem
}

// Get implements kvcommon.QueryReceiver.Get.
func (rcvr *queryReceiver) Get(args kvcommon.GetArgs, reply *kvcommon.GetReply) error {
	// TODO (3A): implement this!
	fmt.Printf("Receiver: client.Get called with args:%v\n", args)
	key := args.Key
	// getCh := make(chan *kvcommon.GetReply)
	chanRef, respCh := rcvr.system.NewChannelRef() // chanRef
	rcvr.system.Tell(rcvr.ref, MGet{key, chanRef})
	fmt.Printf("Receiver: client.Get system.Tell succeeded!\n")
	ans := (<-respCh).(kvcommon.GetReply)
	fmt.Printf("Receiver: client.Get answer received!\n")
	reply.Value, reply.Ok = ans.Value, ans.Ok
	return nil
}

// List implements kvcommon.QueryReceiver.List.
func (rcvr *queryReceiver) List(args kvcommon.ListArgs, reply *kvcommon.ListReply) error {
	// TODO (3A): implement this!
	fmt.Printf("Receiver: client.List called with args:%v:%v\n", args)
	prefix := args.Prefix
	// listCh := make(chan *kvcommon.ListReply)
	chanRef, respCh := rcvr.system.NewChannelRef()
	rcvr.system.Tell(rcvr.ref, MList{prefix, chanRef})
	fmt.Printf("Receiver: client.List system.Tell succeeded!\n")
	ans := (<-respCh).(kvcommon.ListReply)
	fmt.Printf("Receiver: client.List answer received!\n")
	reply.Entries = ans.Entries
	return nil
}

// Put implements kvcommon.QueryReceiver.Put.
func (rcvr *queryReceiver) Put(args kvcommon.PutArgs, reply *kvcommon.PutReply) error {
	// TODO (3A): implement this!
	key, value := args.Key, args.Value
	chanRef, respCh := rcvr.system.NewChannelRef()
	rcvr.system.Tell(rcvr.ref, MPut{key, value, chanRef})
	*reply = (<-respCh).(kvcommon.PutReply)
	// *reply = kvcommon.PutReply{}
	return nil
}
