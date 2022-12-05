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
	gob.Register(MInit{})
	gob.Register(MSyncInit{})
	gob.Register(MSyncRecv{})
	gob.Register(kvcommon.GetReply{})
	gob.Register(kvcommon.ListReply{})
	gob.Register(kvcommon.PutReply{})
}

type queryActor struct {
	context *actor.ActorContext
	// TODO (3A, 3B): implement this!
	me				*actor.ActorRef
	LocalActors 		[]*actor.ActorRef // every local actors 
	remoteActor 	*actor.ActorRef
	kvstore 		map[string]TimedValue
	synchBuffer 	map[string]TimedValue 
	synchDuration 	time.Duration
}

type TimedValue struct {
	TimeStamp 	time.Time
	Value		string
}

// "Constructor" for queryActors, used in ActorSystem.StartActor.
func newQueryActor(context *actor.ActorContext) actor.Actor {
	return &queryActor{
		context: context,
		// TODO (3A, 3B): implement this!
		me: nil,
		LocalActors: make([]*actor.ActorRef, 0),
		remoteActor: nil,
		kvstore: make(map[string]TimedValue, 0),
		synchBuffer: make(map[string]TimedValue, 0),
		synchDuration: 200 * time.Millisecond,
	}
}

// OnMessage implements actor.Actor.OnMessage.
func (actor *queryActor) OnMessage(message any) error {
	// TODO (3A, 3B): implement this!
	switch m := message.(type) {
	case MInit: // should receive exactly once
		actor.me = m.Me
		actor.LocalActors = m.LocalActors
		actor.remoteActor = m.RemoteActor
		// fmt.Printf("actor %v received LocalActors {%v, %v} \n", actor.me.Counter, actor.LocalActors[0].Counter, actor.LocalActors[1].Counter)
	case MSyncInit: // initially sent by NewServer 
		// if have something interesting to send
		// fmt.Printf("actor %v's current buffer is: %v\n", actor.me.Counter, actor.synchBuffer)
		actor.broadcastUpdates()
		// successive MInitSynch to itself
		actor.context.TellAfter(actor.me, MSyncInit{}, actor.synchDuration)
	case MSyncRecv: 
		entries := m.Entries 
		// fmt.Printf("actor %v received update entries: %v\n", actor.me.Counter, entries)
		actor.mergeUpdates(entries)
	case MGet:
		key := m.Key
		getReply := kvcommon.GetReply{}
		if tv, ok := actor.kvstore[key]; ok {
			getReply.Value = tv.Value
			getReply.Ok = true
		} else {
			getReply.Ok = false
		}
		actor.context.Tell(m.Sender, getReply)
	case MList:
		pref := m.Prefix
		entries := make(map[string]string, 0)
		for k, tv := range actor.kvstore {
			if isPrefix(pref, k) {
				entries[k] = tv.Value
			}
		}
		listReply := kvcommon.ListReply{entries}
		actor.context.Tell(m.Sender, listReply)
	case MPut:
		key, value := m.Key, m.Value
		t := time.Now()
		// update itself: might cause temporary inconsistency
		actor.kvstore[key] = TimedValue{t, value}
		// update buffer
		actor.synchBuffer[key] = TimedValue{t, value}
		// trivial putreply
		actor.context.Tell(m.Sender, kvcommon.PutReply{})
		
		// fmt.Printf("actor %v received Put{k:%v, v:%v}\n", actor.me.Counter, key, value)
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
	LocalActors []*actor.ActorRef
	RemoteActor *actor.ActorRef
}

type MSyncInit struct {
}

type MSyncRecv struct {
	Entries map[string]TimedValue
}
// ==================== Helper functions ======================
func isPrefix(prefix string, key string) bool {
	if len(prefix) > len(key) {
		return false
	} // len(prefix) <= len(key)
	return prefix == key[:len(prefix)]
}

func (actor *queryActor) mergeUpdates(entries map[string]TimedValue) {
	for k, tv := range entries {
		if tvLocal, ok := actor.kvstore[k]; !ok {
			actor.kvstore[k] = tv
		} else {
			t, tLocal := tv.TimeStamp, tvLocal.TimeStamp
			if t.After(tLocal) {  // LWW rule
				actor.kvstore[k] = tv
				// fmt.Printf("actor %v's kvstore entries of key [%v] changed to %v\n", actor.me.Counter, k, tv.Value)
			}
		}
	}
}

func (actor *queryActor) broadcastUpdates() {
	if len(actor.synchBuffer) > 0 {
		for _, ref := range actor.LocalActors {
			if ref.Counter != actor.me.Counter {
				actor.context.Tell(ref, MSyncRecv{actor.synchBuffer})
				// fmt.Printf("actor %v tell actor %v to update itself with %v\n", actor.me.Counter, ref.Counter, actor.synchBuffer)
			}
		}
		actor.context.Tell(actor.remoteActor, MSyncRecv{actor.synchBuffer})
		// fmt.Printf("local actor (%v:%v) tell remote actor (%v:%v) to update itself with %v\n", actor.me.Address, actor.me.Counter, actor.remoteActor.Address, actor.remoteActor.Counter, actor.synchBuffer)
		actor.synchBuffer = make(map[string]TimedValue, 0)
	}
}