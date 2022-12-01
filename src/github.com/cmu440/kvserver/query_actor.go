package kvserver

import (
	"encoding/gob"
	"fmt"
	"time"
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
	gob.Register()
	gob.Register()
}

type queryActor struct {
	context *actor.ActorContext
	// TODO (3A, 3B): implement this!
	me			*actor.ActorRef
	localPeers []*actor.ActorRef // every local actors 
	kvstore map[string]*timedValue
	synchBuffer map[string]*timedValue 
}

type timedValue struct {
	timestamp 	time.Time
	value		string
}

// "Constructor" for queryActors, used in ActorSystem.StartActor.
func newQueryActor(context *actor.ActorContext) actor.Actor {
	return &queryActor{
		context: context,
		// TODO (3A, 3B): implement this!
		me: nil,
		localPeers: make([]*actor.ActorRef),
		kvstore: make(map[string]*timedValue),
		synchBuffer: make(map[string]*timedValue)
	}
}

// OnMessage implements actor.Actor.OnMessage.
func (actor *queryActor) OnMessage(message any) error {
	// TODO (3A, 3B): implement this!
	switch m := message.(type) {
	case MInit: 
		actor.me = m.Me
		actor.localPeers = m.LocalPeers
	case MSynch: // timestamp
		entries := m.Entries 
		for k, tv := range entries {
			t, v := tv.timestamp, tv.value
			if tvLocal, ok := actor.kvstore[k]; !ok {
				actor.kvstore[k] = tv
			} else {
				tLocal, vLocal := tvLocal.timestamp, tvLocal.value
				if t.After(tLocal) { 
					actor.kvstore[k] = tv
				}
			}
		}
	case MGet:
		key := m.Key
		getReply := kvcommon.GetReply{}
		if value, ok := actor.kvstore[key]; ok {
			getReply.Value = value
			getReply.Ok = true
		} else {
			getReply.Ok = false
		}
		actor.context.Tell(m.Sender, getReply)
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
		// update buffer
		actor.synchBuffer[key] = &timedValue{time.Now(), value}
		// broadcast 
		actor.context.TellAfter
		// trivial putreply
		actor.context.Tell(m.Sender, kvcommon.PutReply{})
	default:
		return fmt.Errorf("Unexpected queryActor message type: %T", m)
	}
	return nil
}

// ======================== actor message types ==============

type MGet struct {
	Key    string
	Sender *actor.ActorRef // receiver end
}

type MList struct {
	Prefix string
	Sender *actor.ActorRef // receiver end
}

type MPut struct {
	Key    string
	Value  string
	Sender *actor.ActorRef // receiver end
}

type MInit struct {
	Me			*actor.ActorRef
	LocalPeers []*actor.ActorRef
}

type InitReply struct {
}

type MSynch struct {
	Entries map[string]*timedValue
}
// ==================== Helper functions ======================
func isPrefix(prefix string, key string) bool {
	if len(prefix) > len(key) {
		return false
	} // len(prefix) <= len(key)
	return prefix == key[:len(prefix)]
}
