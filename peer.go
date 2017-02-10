/*
Implements the solution to assignment 3 for UBC CS 416 2016 W2.

Usage:

Bootstrapping:
go run peer.go -b [physical-peerID] [peer-IP:port] [server ip:port]

Joining:
go run peer.go -j [physical-peerID] [peer-IP:port] [other-peer ip:port]

Example:
go run peer.go -b 0 127.0.0.1:19000 127.0.0.1:20000
go run peer.go -j 0 127.0.0.1:19001 127.0.0.1:19000

See run.sh for example running script.
*/

package main

import (
	"fmt"
	"os"
	"strconv"
	//can be deleted, only for example printing
	//"math/rand"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"
	// TODO
)

//Modes of operation
const (
	BOOTSTRAP = iota
	JOIN
)

// Resource server type.
type RServer int

type Peer int

type Join struct {
	Resources      []Resource
	PeersIpPorts   map[int]string
	SID            int
	ResourceServer string
}

type JoinArg struct {
	NewPeerId     int
	NewPeerIpPort string
}

// Request that peer sends in call to RServer.InitSession
// Return value is int
type Init struct {
	IPaddr string // Must be set to ""
}

// Request that peer sends in call to RServer.GetResource
type ResourceRequest struct {
	SessionID int
	IPaddr    string // Must be set to ""
}

// Response that the server returns from RServer.GetResource
type Resource struct {
	Resource      string
	LogicalPeerID int
	NumRemaining  int
}

//An array of resources
type Resources []Resource

var LOCALPID int
var LOCALIPPORT string
var CAPTAIN bool

var RESOURCES Resources
var PEERSIPPORTS = make(map[int]string)
var SID int
var RESOURCESERVER string

var mutex = &sync.Mutex{}

// Main workhorse method.
func main() {
	var nextCap string
	var last int
	var first int
	var peerIds []int
	var i int
	nextCap = "nil"
	// Parse the command line args, panic if error
	mode, physicalPeerId, peerIpPort, otherIpPort, err := ParseArguments()
	if err != nil {
		panic(err)
	}
	go func() {
		pServer := rpc.NewServer()
		p := new(Peer)
		pServer.Register(p)

		l, _ := net.Listen("tcp", peerIpPort)
		for {
			conn, _ := l.Accept()
			go pServer.ServeConn(conn)
		}
	}()

	LOCALPID = physicalPeerId
	LOCALIPPORT = peerIpPort

	if mode == BOOTSTRAP {
		CAPTAIN = true
		RESOURCESERVER = otherIpPort
		resourceClient, _ := rpc.Dial("tcp", RESOURCESERVER)
		initArgs := &Init{""}
		err = resourceClient.Call("RServer.InitSession", initArgs, &SID)

		PEERSIPPORTS[LOCALPID] = peerIpPort
		resourceClient.Close()
	}
	if mode == JOIN {
		CAPTAIN = false
		peerClient, _ := rpc.Dial("tcp", otherIpPort)
		joinArgs := &JoinArg{LOCALPID, LOCALIPPORT}
		var joinReply Join
		err = peerClient.Call("Peer.JoinPeer", joinArgs, &joinReply)
		RESOURCES = joinReply.Resources
		PEERSIPPORTS = joinReply.PeersIpPorts
		SID = joinReply.SID
		RESOURCESERVER = joinReply.ResourceServer

		peerClient.Close()
	}

	//fmt.Println(PEERSIPPORTS)
	JoinPrint(LOCALPID)

	for {
		mutex.Lock()
		for !CAPTAIN {
			//monitor captain heartbeat
		}
		mutex.Unlock()

		time.Sleep(4 * time.Second)

		mutex.Lock()
		if nextCap != "" {
			//fmt.Println("getting resource ...")
			resourceArgs := &ResourceRequest{SID, ""}
			var resourceReply Resource
			rr := &resourceReply
			resourceClient, _ := rpc.Dial("tcp", RESOURCESERVER)
			err = resourceClient.Call("RServer.GetResource", resourceArgs, rr)
			resourceClient.Close()
			RESOURCES = append(RESOURCES, *rr)
			sendOutResources(rr)
		}

		//relinquish captainhood
		peerIds = getPeerIds(PEERSIPPORTS)
		last = len(PEERSIPPORTS) - 1
		first = 0
		for i = 0; i <= last; i++ {
			if peerIds[i] == LOCALPID {
				break
			}
		}
		numRemaining := RESOURCES[len(RESOURCES)-1].NumRemaining
		switch {
		case last == first && numRemaining >= 1:
			nextCap = ""
		case i == last && last != first && numRemaining >= 1:
			nextCap = PEERSIPPORTS[peerIds[first]]
		case numRemaining < 1:
			RESOURCES.FinalPrint(LOCALPID)
			//fmt.Println(PEERSIPPORTS)
			closePeers()
		default:
			nextCap = PEERSIPPORTS[peerIds[i+1]]
		}
		if nextCap != "" {
			CAPTAIN = false
			newCapClient, _ := rpc.Dial("tcp", nextCap)
			var changeCapArgs string
			var changeCapReply string
			err = newCapClient.Call("Peer.ChangeCap", &changeCapArgs, &changeCapReply)
			newCapClient.Close()
		}
		mutex.Unlock()
	}
}

func (t *Peer) JoinPeer(newPeer *JoinArg, reply *Join) error {
	mutex.Lock()
	PEERSIPPORTS[newPeer.NewPeerId] = newPeer.NewPeerIpPort
	//fmt.Println(PEERSIPPORTS)
	//fmt.Println("joining ...")
	var sendPeersReply string
	for id, ip := range PEERSIPPORTS {
		if (ip != LOCALIPPORT) && (ip != newPeer.NewPeerIpPort) {
			peerClient, err := rpc.Dial("tcp", ip)
			if err != nil {
				delete(PEERSIPPORTS, id)
				continue
			}
			err = peerClient.Call("Peer.SendPeers", &PEERSIPPORTS, &sendPeersReply)
			peerClient.Close()
		}
	}
	mutex.Unlock()

	reply.Resources = RESOURCES
	reply.PeersIpPorts = PEERSIPPORTS
	reply.SID = SID
	reply.ResourceServer = RESOURCESERVER

	return nil
}

func sendOutResources(newResource *Resource) {
	for id, ip := range PEERSIPPORTS {
		if ip != LOCALIPPORT {
			peerClient, err := rpc.Dial("tcp", ip)
			if err != nil {
				delete(PEERSIPPORTS, id)
				continue
			}
			forwardResourceReply := ""
			err = peerClient.Call("Peer.ForwardResource", newResource, &forwardResourceReply)
			peerClient.Close()
		}
	}
}

func getPeerIds(peersIpPorts map[int]string) []int {
	peerIds := make([]int, 0, len(peersIpPorts))
	for id := range peersIpPorts {
		peerIds = append(peerIds, id)
	}
	sort.Ints(peerIds)
	return peerIds
}

func closePeers() {
	str := ""
	for _, ip := range PEERSIPPORTS {
		if ip != LOCALIPPORT {
			conn, err := rpc.Dial("tcp", ip)
			if err != nil {
				continue
			}
			err = conn.Call("Peer.ClosePeer", &str, &str)
			if err != nil {
				continue
			}
		}

	}
	os.Exit(0)
}

func (t *Peer) ClosePeer(args *string, reply *string) error {
	os.Exit(0)
	return nil
}

func (t *Peer) ChangeCap(args *string, reply *string) error {
	CAPTAIN = true
	return nil
}

func (t *Peer) ForwardResource(newResource *Resource, reply *string) error {
	RESOURCES = append(RESOURCES, *newResource)
	return nil
}

func (t *Peer) SendPeers(peers *map[int]string, reply *string) error {
	PEERSIPPORTS = *peers
	//fmt.Println(PEERSIPPORTS)
	*reply = ""
	return nil
}

// Parses the command line arguments, two cases are valid
func ParseArguments() (mode int, physicalPeerId int, peerIpPort string, serverOrJoinerIpPort string, err error) {
	args := os.Args[1:]
	if len(args) != 4 {
		err = fmt.Errorf("Please supply 4 command line arguments as seen in the spec http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/assign3/index.html")
		return
	}
	switch args[0] {
	case "-b":
		mode = BOOTSTRAP
	case "-j":
		mode = JOIN
	default:
		err = fmt.Errorf("Valid modes are bootstrapping -b and joining -j not %s", args[0])
		return
	}
	physicalPeerId, err = strconv.Atoi(args[1])
	if err != nil {
		err = fmt.Errorf("unable to parse physical peer id (argument 2) please supply an integer Error:%s", err.Error())
	}
	peerIpPort = args[2]
	serverOrJoinerIpPort = args[3]
	return
}

/////////////////// Use functions below for state notifications (DO NOT MODIFY).

func JoinPrint(physicalPeerId int) {
	fmt.Printf("JOINED: using %d\n", physicalPeerId)
}

//
func (r Resource) String() string {
	return fmt.Sprintf("Resource: %s\tLogicalPeerID: %d\tNumRemaining: %d", r.Resource, r.LogicalPeerID, r.NumRemaining)
}

//
func (r Resources) String() (rString string) {
	for _, resource := range r {
		rString += resource.String() + "\n"
	}
	return
}

//
func (r Resources) FinalPrint(physicalPeerId int) {
	for _, resource := range r {
		fmt.Printf("ALLOC: %d %d %s\n", physicalPeerId, resource.LogicalPeerID, resource.Resource)
	}
}

// Example illustrating JoinPrint and FinalPrint usage:
//
// var res Resources
// for i := 0; i < 5; i++ {
// 	res = append(res, Resource{Resource: fmt.Sprintf("%d", i), LogicalPeerID: rand.Int(), NumRemaining: 5 - i})
// }
// JoinPrint(physicalPeerId)
// res.FinalPrint(physicalPeerId)

//for {
/*numPeers := len(PEERSIPPORTS)
lockRequestArgs := ""
lockRequestReply := false
for i := 0; i < numPeers; i++ {
	if PEERSIPPORTS[i] != LOCALIPPORT {
		peerClient, err := rpc.Dial("tcp", PEERSIPPORTS[i])
		if err != nil {
			fmt.Println("DEAD")
			os.Exit(0)
		}
		err = peerClient.Call("Peer.RequestLock", &lockRequestArgs, &lockRequestReply)
		if !(lockRequestReply) {
			break
		}
		peerClient.Close()
	}
}
if numPeers == 1 {
	lockRequestReply = true
}
if lockRequestReply {
	mutex.Lock()
	mutex.Unlock()
}*/

//}
//for {
//}

/*numPeers := len(PEERSIPPORTS)
lockRequestArgs := ""
lockRequestReply := false
//lock newPeer
peerClient, err := rpc.Dial("tcp", *newPeer)
if err != nil {
	fmt.Println("DEAD")
	os.Exit(0)
}
err = peerClient.Call("Peer.RequestLock", &lockRequestArgs, &lockRequestReply)
peerClient.Close()

for i := 0; i < numPeers; i++ {
	if PEERSIPPORTS[i] != LOCALIPPORT {
		peerClient, err = rpc.Dial("tcp", PEERSIPPORTS[i])
		err = peerClient.Call("Peer.RequestLock", &lockRequestArgs, &lockRequestReply)
		if !(lockRequestReply) {
			break
		}
		peerClient.Close()
	}
}
if numPeers == 1 {
	lockRequestReply = true
}
if lockRequestReply {
	mutex.Lock()
	mutex.Unlock()
}*/

/*func (t *Peer) NewCaptainRequest(newCapIp *string, reply *bool) error {
	mutex.Lock()
	Captain = false
	CAPTAINSNAME = *newCapIp
	*reply = true
	return nil
}*/

/*if CAPTAINSNAME != LOCALIPPORT{
	lockRequestArgs := LOCALIPPORT
	lockRequestReply := false
	captainsClient, err := rpc.Dial("tcp", CAPTAINSNAME)
	err = captainsClient.Call("Peer.NewCaptainRequest", &lockRequestArgs, &lockRequestReply)
	if lockRequestReply {
		CAPTAINSNAME = LOCALIPPORT
		CAPTAIN = true
		mutex.Unlock()
	}
}*/

/*for id, ip := range PEERSIPPORTS {
	if (ip != LOCALIPPORT) && (ip != newPeer.NewPeerIpPort) {
		peerClient, err := rpc.Dial("tcp", ip)
		if err != nil {}
		forwardResourceReply := ""
		err = peerClient.Call("Peer.ForwardResource", &resourceReply, &forwardResourceReply)
		if err != nil {
			delete(PEERSIPPORTS, id)
		}
		peerClient.Close()
	}
}*/
