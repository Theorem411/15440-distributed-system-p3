package actor

import (
	"net/rpc"
)

// Calls system.tellFromRemote(ref, mars) on the remote ActorSystem listening
// on ref.Address.
//
// This function should NOT wait for a reply from the remote system before
// returning, to allow sending multiple messages in a row more quickly.
// It should ensure that messages are delivered in-order to the remote system.
// (You may assume that remoteTell is not called multiple times
// concurrently with the same ref.Address).
func remoteTell(client *rpc.Client, ref *ActorRef, mars []byte) {
	// TODO (3B): implement this!
	args := RTellArgs{ref, mars}
	reply := &RTellReply{}
	client.Go("RemoteTeller.RTell", args, reply, nil)
}

// Registers an RPC handler on server for remoteTell calls to system.
//
// You do not need to start the server's listening on the network;
// just register a handler struct that handles remoteTell RPCs by calling
// system.tellFromRemote(ref, mars).
func registerRemoteTells(system *ActorSystem, server *rpc.Server) error {
	// TODO (3B): implement this!
	rt := &RemoteTeller{system}
	err := server.RegisterName("RemoteTeller", rt)
	if err != nil {
		return err
	}
	return nil
}

// TODO (3B): implement your remoteTell RPC handler below
type RemoteTeller struct {
	System	*ActorSystem
}

type RTellArgs struct {
	Ref 	*ActorRef
	Mars 	[]byte
}

type RTellReply struct {
}

func (rt *RemoteTeller) RTell(args RTellArgs, reply *RTellArgs) error {
	ref, mars := args.Ref, args.Mars
	rt.System.tellFromRemote(ref, mars)
	return nil
}
