package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/memberlist"
)

const desiredServers = 20

var colors = []string{
	"red",
	"orange",
	"yellow",
	"green",
	"blue",
	"indigo",
	"violet",
}

func main() {
	var updates = make(chan struct{})
	var servers = []*server{}
	for i := 0; i < desiredServers; i++ {
		s := newServer(updates)
		go s.start()
		servers = append(servers, s)
	}

	logger := log.New(os.Stderr, "reporter", log.Lshortfile|log.Ltime)

	go mutate(servers)

	for range updates {
		counts := make(map[string]int)
		var newest state
		for _, s := range servers {
			if s.state.TS.After(newest.TS) {
				newest = s.state
			}
			counts[s.state.Value]++
		}

		score := 100 * (float32(counts[newest.Value]) / float32(len(servers)))
		logger.Printf("score: %.0f%%; expect: %s; colors: %v",
			score,
			newest.Value,
			counts,
		)
	}
}

func mutate(servers []*server) {
	time.Sleep(10 * time.Second)
	for range time.Tick(time.Second / 4) {
		i := rand.Intn(len(servers))
		go servers[i].mutate()
	}
}

type state struct {
	TS    time.Time
	Value string
}

type server struct {
	updates chan<- struct{}
	logger  *log.Logger
	list    *memberlist.Memberlist
	queue   *memberlist.TransmitLimitedQueue
	state   state
	name    string
	port    int
}

func nextState() state {
	return state{
		TS:    time.Now(),
		Value: colors[rand.Intn(len(colors))],
	}
}

var numServers = 0

const defaultPort = 7946

func newServer(updates chan<- struct{}) *server {
	name := fmt.Sprintf("[ s%d ] ", numServers)
	s := &server{
		updates: updates,
		logger:  log.New(io.Discard, name, log.Lshortfile|log.Ltime),
		port:    defaultPort + numServers,
		name:    name,
		state:   nextState(),
	}
	numServers++
	return s
}

func (s *server) mutate() {
	s.processStateChange(nextState())
}

func (s *server) start() {
	/* Create the initial memberlist from a safe configuration.
		   Please reference the godoc for other default config types.
	http://godoc.org/github.com/hashicorp/memberlist#Config
	*/
	config := memberlist.DefaultLocalConfig()
	config.Name = s.name
	config.BindPort = s.port
	config.AdvertisePort = s.port
	config.Delegate = s
	config.LogOutput = io.Discard

	var err error
	s.list, err = memberlist.Create(config)
	if err != nil {
		s.logger.Fatal("Failed to create memberlist: " + err.Error())
	}

	s.queue = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return len(s.list.Members())
		},
		RetransmitMult: 1,
	}

	// Join an existing cluster by specifying at least one known member.
	_, err = s.list.Join([]string{"localhost:" + strconv.Itoa(defaultPort)})
	if err != nil {
		s.logger.Fatal("Failed to join cluster: " + err.Error())
	}

}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (s *server) NodeMeta(limit int) []byte {
	return nil
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed
func (s *server) NotifyMsg(buf []byte) {
	s.logger.Printf("NotifyMsg(%q)", buf)
	var remoteState state
	err := json.Unmarshal(buf, &remoteState)
	if err != nil {
		s.logger.Fatal(err)
	}
	s.processStateChange(remoteState)
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
func (s *server) GetBroadcasts(overhead, limit int) [][]byte {
	return s.queue.GetBroadcasts(overhead, limit)
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (s *server) LocalState(join bool) []byte {
	s.logger.Printf("LocalState(%v)", join)
	buf, _ := json.Marshal(s.state)
	return buf
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (s *server) MergeRemoteState(buf []byte, join bool) {
	s.logger.Printf("MergeRemoteState(%q, %v)", buf, join)
	var remoteState state
	err := json.Unmarshal(buf, &remoteState)
	if err != nil {
		s.logger.Fatal(err)
	}
	s.processStateChange(remoteState)
}

func (s *server) processStateChange(newState state) {
	if newState.TS.Before(s.state.TS) {
		return
	}

	s.state = newState
	s.updates <- struct{}{}
	s.queue.QueueBroadcast(StateChange{s.state})
}

type StateChange struct {
	state
}

func (s StateChange) Invalidates(b memberlist.Broadcast) bool {
	o, ok := b.(StateChange)
	if !ok {
		return false
	}
	return s.TS.After(o.TS)
}

func (s StateChange) Message() []byte {
	buf, _ := json.Marshal(s.state)
	return buf
}

func (s StateChange) Finished() {}
