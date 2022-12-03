package kvserver 

import (
	"encoding/gob"
	"fmt"
	"time"
	"github.com/cmu440/actor"
)

func init() {
	gob.Register(MInitRemote{})
	gob.Register(MSynchInitRemote{})
	gob.Register(MSynchRecvRemote{})
}

type remoteActor struct {
	// Q: mutex?
	context *actor.ActorContext
	me				*actor.ActorRef
	remoteActors 	[]*actor.ActorRef // 
	synchBuffer		map[string]*timedValue
	synchDuration 	time.Duration
}

func newRemoteActor(context *actor.ActorContext) actor.Actor {
	return &remoteActor{
		context: context,
		me: nil,
		remoteActors: make([]*actor.ActorRef, 0), 
		synchBuffer: make(map[string]*timedValue),
		synchDuration: 500 * time.Millisecond,
	}
}

func (actor *remoteActor) OnMessage(message any) error {
	switch m := message.(type) {
	case MInitRemote:
		actor.me, actor.remoteActors = m.Me, m.RemoteActors
	case MSynchInitRemote: 
		actor.context.TellAfter(actor.me, MSynchInitRemote{}, actor.synchDuration)
		// broadcast current version of buffer to all remote actor
		err := actor.broadcastRemoteUpdates()
		if err != nil {
			return err
		}
	case MSynchRecvRemote: 
		entries := m.Entries
		// LWW-rule of buffers: merge buffers 
		actor.mergeRemoteUpdates(entries)
	default: 
		return fmt.Errorf("Unexpected remoteActor message type: %T", m)
	}
	return nil
}

type MInitRemote struct {
	Me 			*actor.ActorRef
	RemoteActors []*actor.ActorRef
}

type MSynchInitRemote struct {
}

type MSynchRecvRemote struct {
	Entries 	map[string]*timedValue
}

// ====================== Helper functions ================================
func (actor *remoteActor) mergeRemoteUpdates(entries map[string]*timedValue) {
	for k, tv := range entries {
		if tvLocal, ok := actor.synchBuffer[k]; !ok {
			actor.synchBuffer[k] = tv
		} else {
			t, tLocal := tv.timestamp, tvLocal.timestamp
			if t.After(tLocal) {  // LWW rule
				actor.synchBuffer[k] = tv
				fmt.Printf("remotesynch actor %v's buffer entries of key [%v] changed to %v\n", actor.me.Counter, k, tv.value)
			}
		}
	}
}

func (actor *remoteActor) broadcastRemoteUpdates() error {
	for _, ref := range actor.remoteActors {
		client := // *rpc.Client
		mars, err := marshal(actor.synchBuffer)
		if err != nil {
			return err
		}
		remoteTell(client, ref, mars)
	}
	return nil
}