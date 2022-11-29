package kvserver

import (
	"encoding/gob"
	"fmt"
	"github.com/cmu440/actor"
	"github.com/cmu440/kvcommon"
)

// Implement your queryActor in this file.
// See example/counter_actor.go for an example actor using the
// github.com/cmu440/actor package.

// TODO (3A, 3B): define your message types as structs

func init() {
	// TODO (3A, 3B): Register message types, e.g.:
	gob.Register(MGet{})
	gob.Register(MList{})
	gob.Register(MPut{})
	gob.Register(kvcommon.GetReply{})
	gob.Register(kvcommon.ListReply{})
	gob.Register(kvcommon.PutReply{})
}

type queryActor struct {
	context *actor.ActorContext
	// TODO (3A, 3B): implement this!
	kvstore map[string]string
}

// "Constructor" for queryActors, used in ActorSystem.StartActor.
func newQueryActor(context *actor.ActorContext) actor.Actor {
	return &queryActor{
		context: context,
		// TODO (3A, 3B): implement this!
		kvstore: make(map[string]string),
	}
}

// OnMessage implements actor.Actor.OnMessage.
func (actor *queryActor) OnMessage(message any) error {
	// TODO (3A, 3B): implement this!
	switch m := message.(type) {
	case MGet:
		// fmt.Printf("Actor received client.Get!\n")
		key := m.Key
		getReply := kvcommon.GetReply{}
		if value, ok := actor.kvstore[key]; ok {
			getReply.Value = value
			getReply.Ok = true
		} else {
			getReply.Ok = false
		}
		actor.context.Tell(m.Sender, getReply)
		// fmt.Printf("Actor answered client.Get!\n")
	case MList:
		pref := m.Prefix
		entries := make(map[string]string)
		for k, v := range actor.kvstore {
			if isPrefix(pref, k) {
				entries[k] = v
			}
		}
		listReply := kvcommon.ListReply{entries}
		actor.context.Tell(m.Sender, listReply)
	case MPut:
		key, value := m.Key, m.Value
		actor.kvstore[key] = value
		actor.context.Tell(m.Sender, kvcommon.PutReply{})
	default:
		return fmt.Errorf("Unexpected queryActor message type: %T", m)
	}
	return nil
}

// ======================== actor message types ==============

type MGet struct {
	Key    string
	Sender *actor.ActorRef
}

type MList struct {
	Prefix string
	Sender *actor.ActorRef
}

type MPut struct {
	Key    string
	Value  string
	Sender *actor.ActorRef
}

// ==================== Helper functions ======================
func isPrefix(prefix string, key string) bool {
	if len(prefix) > len(key) {
		return false
	} // len(prefix) <= len(key)
	return prefix == key[:len(prefix)]
}
