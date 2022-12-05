package kvserver 

import (
	"encoding/gob"
	"fmt"
	"time"
	"github.com/cmu440/actor"
)

func init() {
	gob.Register(MRemoteContact{})
	gob.Register(MSyncInitRemote{})
	gob.Register(MSyncRecvRemote{})
}

type remoteActor struct {
	// Q: mutex?
	context 		*actor.ActorContext
	me				*actor.ActorRef
	localActors		[]*actor.ActorRef
	remoteActors 	[]*actor.ActorRef // 
	synchBuffer		map[string]TimedValue
	hasRemote		bool
	synchDuration 	time.Duration
}

func newRemoteActor(context *actor.ActorContext) actor.Actor {
	return &remoteActor{
		context: context,
		me: nil,
		localActors: make([]*actor.ActorRef, 0),
		remoteActors: make([]*actor.ActorRef, 0), 
		synchBuffer: make(map[string]TimedValue, 0),
		hasRemote: false,
		synchDuration: 500 * time.Millisecond,
	}
}

func (actor *remoteActor) OnMessage(message any) error {
	switch m := message.(type) {
	case MInit:
		actor.me, actor.localActors = m.Me, m.LocalActors
		// fmt.Printf("remote actor (%v:%v) gets init'ed with local contact list: %v\n", actor.me.Address, actor.me.Counter, actor.localActors)
	case MRemoteContact:
		actor.remoteActors = m.RemoteActors
		// fmt.Printf("remote actor (%v:%v) gets init'ed with remote contact: %v and remote contact: %v\n", actor.me.Address, actor.me.Counter, actor.remoteActors, actor.localActors)
	case MSyncInitRemote: // MSyncInitCkpt
		actor.context.TellAfter(actor.me, MSyncInitRemote{}, actor.synchDuration)
		// broadcast current version of buffer to all remote actor
		err := actor.broadcastUpdate()
		if err != nil {
			return err
		}
	case MSyncRecv: // receive update from local actors
		entries := m.Entries
		// LWW-rule of buffers: merge buffers 
		actor.mergeUpdates(entries)
	case MSyncRecvRemote: // receive update from remote actors
		entries := m.Entries
		actor.hasRemote = true
		actor.mergeUpdates(entries)
	default: 
		return fmt.Errorf("Unexpected remoteActor message type: %T", m)
	}
	return nil
}

type MRemoteContact struct {
	RemoteActors []*actor.ActorRef
}

type MSyncInitRemote struct {
}

type MSyncRecvRemote struct {
	Entries 	map[string]TimedValue
}


// ====================== Helper functions ================================
func (actor *remoteActor) mergeUpdates(entries map[string]TimedValue) {
	for k, tv := range entries {
		if tvLocal, ok := actor.synchBuffer[k]; !ok {
			actor.synchBuffer[k] = tv
			// fmt.Printf("remote actor (%v:%v)'s buffer at key=%v init to %v\n", actor.me.Address, actor.me.Counter, k, tv.Value)
		} else {
			t, tLocal := tv.TimeStamp, tvLocal.TimeStamp
			if t.After(tLocal) {  // LWW rule
				actor.synchBuffer[k] = tv
				// fmt.Printf("remote actor (%v:%v)'s buffer entries of key [%v] changed to %v\n", actor.me.Address, actor.me.Counter, k, tv.Value)
			}
		}
	}
}

func (actor *remoteActor) broadcastUpdate() error {
	if len(actor.synchBuffer) > 0 {	
		// fmt.Printf("remote actor (%v:%v) received instruction to send updates\n", actor.me.Address, actor.me.Counter)
		// fmt.Printf("remote actor (%v:%v) has remote contact list: %v\n", actor.me.Address, actor.me.Counter, actor.remoteActors)
		for _, ref := range actor.remoteActors {
			// remoteTell(client, ref, mars)
			// fmt.Printf("remote actor (%v:%v) tells (%v:%v) to update itself with Entries=%v\n", actor.me.Address, actor.me.Counter, ref.Address, ref.Counter, actor.synchBuffer)
			actor.context.Tell(ref, MSyncRecvRemote{actor.synchBuffer})
		}
		if actor.hasRemote {
			for _, ref := range actor.localActors {
				actor.context.Tell(ref, MSyncRecv{actor.synchBuffer})
			}
			actor.hasRemote = false
		}
		actor.synchBuffer = make(map[string]TimedValue, 0)
	}
	return nil
}