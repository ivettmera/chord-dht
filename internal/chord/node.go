package chord

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"chord-dht/pkg/hash"
	pb "chord-dht/proto"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

const (
	// FingerTableSize is the size of the finger table (M bits)
	FingerTableSize = hash.M // 160 bits for SHA-1
	// StabilizeInterval is how often to run stabilization
	StabilizeInterval = 5 * time.Second
	// FixFingersInterval is how often to fix finger table entries
	FixFingersInterval = 10 * time.Second
	// CheckPredecessorInterval is how often to check predecessor
	CheckPredecessorInterval = 15 * time.Second
	// RPCTimeout is the timeout for RPC calls
	RPCTimeout = 10 * time.Second
)

// Node represents a Chord DHT node
type Node struct {
	// Embed the unimplemented server for gRPC compatibility
	pb.UnimplementedChordServiceServer
	
	// Node identification
	id         *hash.Hash
	address    string // Address advertised to other nodes
	listenAddr string // Address to bind/listen on
	
	// Chord state
	predecessor *NodeInfo
	successor   *NodeInfo
	fingers     []*NodeInfo
	next        int // next finger to fix
	
	// Network
	server      *grpc.Server
	listener    net.Listener
	clients     map[string]pb.ChordServiceClient
	connections map[string]*grpc.ClientConn
	
	// Synchronization
	mu sync.RWMutex
	
	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	
	// Metrics (will be used by metrics module)
	MessageCount int64
	LookupCount  int64
	
	// Storage (simple key-value store)
	data map[string][]byte
}

// NodeInfo represents information about a Chord node
type NodeInfo struct {
	ID      *hash.Hash
	Address string
}

// NewNode creates a new Chord node
func NewNode(address string, id *hash.Hash) *Node {
	return NewNodeWithAdvertise(address, address, id)
}

// NewNodeWithAdvertise creates a new Chord node with separate listen and advertise addresses
func NewNodeWithAdvertise(listenAddr, advertiseAddr string, id *hash.Hash) *Node {
	if id == nil {
		id = hash.GenerateID(advertiseAddr)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	node := &Node{
		id:          id,
		address:     advertiseAddr, // Use advertise address for node identity
		fingers:     make([]*NodeInfo, FingerTableSize),
		clients:     make(map[string]pb.ChordServiceClient),
		connections: make(map[string]*grpc.ClientConn),
		ctx:         ctx,
		cancel:      cancel,
		data:        make(map[string][]byte),
	}
	
	// Store listen address separately for binding
	node.listenAddr = listenAddr
	
	// Initialize finger table with advertise address
	selfInfo := &NodeInfo{ID: id, Address: advertiseAddr}
	for i := 0; i < FingerTableSize; i++ {
		node.fingers[i] = selfInfo
	}
	
	return node
}

// Start starts the Chord node
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	// Use listenAddr if set, otherwise use address
	bindAddr := n.address
	if n.listenAddr != "" {
		bindAddr = n.listenAddr
	}
	
	// Start gRPC server
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", bindAddr, err)
	}
	
	n.listener = listener
	n.server = grpc.NewServer()
	pb.RegisterChordServiceServer(n.server, n)
	
	// Enable reflection for grpcurl compatibility
	reflection.Register(n.server)
	
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := n.server.Serve(listener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()
	
	// Start maintenance routines
	n.startMaintenance()
	
	log.Printf("Node %s listening on %s, advertising %s", n.id.String()[:8], bindAddr, n.address)
	return nil
}

// Stop stops the Chord node
func (n *Node) Stop() {
	n.cancel()
	
	if n.server != nil {
		n.server.GracefulStop()
	}
	
	if n.listener != nil {
		n.listener.Close()
	}
	
	n.wg.Wait()
	log.Printf("Node %s stopped", n.id.String()[:8])
}

// Join joins the Chord ring via a bootstrap node
func (n *Node) Join(bootstrapAddr string) error {
	if bootstrapAddr == "" {
		// This is the first node, create ring
		n.mu.Lock()
		defer n.mu.Unlock()
		selfInfo := &NodeInfo{ID: n.id, Address: n.address}
		n.successor = selfInfo
		n.predecessor = nil
		log.Printf("Node %s created ring", n.id.String()[:8])
		return nil
	}
	
	// Join existing ring
	client, err := n.getClient(bootstrapAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to bootstrap node: %w", err)
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()
	
	// Find our successor
	resp, err := client.FindSuccessor(ctx, &pb.FindSuccessorRequest{
		Key: n.id.String(),
		Requester: &pb.Node{
			Id:      n.id.String(),
			Address: n.address,
		},
	})
	
	if err != nil {
		return fmt.Errorf("failed to find successor: %w", err)
	}
	
	if !resp.Success {
		return fmt.Errorf("join failed: %s", resp.Error)
	}
	
	n.mu.Lock()
	defer n.mu.Unlock()
	
	successorID, err := hash.NewHashFromHex(resp.Successor.Id)
	if err != nil {
		return fmt.Errorf("invalid successor ID: %w", err)
	}
	
	n.successor = &NodeInfo{
		ID:      successorID,
		Address: resp.Successor.Address,
	}
	
	// Initialize predecessor as nil (will be set by stabilization)
	n.predecessor = nil

	log.Printf("Node %s joined ring, successor: %s", 
		n.id.String()[:8], n.successor.ID.String()[:8])
	
	// Notify successor about us immediately after join
	if err := n.remoteNotify(n.successor.Address); err != nil {
		log.Printf("Node %s: failed to notify successor after join: %v", n.id.String()[:8], err)
	}
	
	return nil
}

// findSuccessor finds the successor of a given key
func (n *Node) findSuccessor(key *hash.Hash) (*NodeInfo, error) {
	n.LookupCount++
	
	n.mu.RLock()
	// Check if key is between us and our successor
	if n.successor != nil && key.InRange(n.id, n.successor.ID) {
		successor := n.successor
		n.mu.RUnlock()
		return successor, nil
	}
	n.mu.RUnlock()
	
	// Find the closest preceding node and ask it
	preceding := n.closestPrecedingFinger(key)
	
	// If the closest preceding finger is ourselves, return our successor
	if preceding.ID.Equal(n.id) {
		n.mu.RLock()
		successor := n.successor
		n.mu.RUnlock()
		return successor, nil
	}
	
	// Ask the closest preceding finger
	return n.remoteFindSuccessor(preceding.Address, key)
}

// closestPrecedingFinger finds the closest preceding finger for a key
func (n *Node) closestPrecedingFinger(key *hash.Hash) *NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	// Search finger table from highest to lowest
	for i := FingerTableSize - 1; i >= 0; i-- {
		finger := n.fingers[i]
		if finger != nil && finger.ID.InRangeExclusive(n.id, key) {
			// Verify the finger is still alive
			if n.remotePing(finger.Address) == nil {
				return finger
			}
		}
	}
	
	// If no finger found, return self
	return &NodeInfo{ID: n.id, Address: n.address}
}

// notify is called when a node thinks it might be our predecessor
func (n *Node) notify(node *NodeInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	// If we have no predecessor, or the new node is between our predecessor and us
	if n.predecessor == nil || node.ID.InRangeExclusive(n.predecessor.ID, n.id) {
		n.predecessor = node
		log.Printf("Node %s: new predecessor %s", n.id.String()[:8], node.ID.String()[:8])
	}
}

// stabilize is called periodically to verify and update successor and predecessor
func (n *Node) stabilize() {
	n.mu.RLock()
	successor := n.successor
	n.mu.RUnlock()
	
	if successor == nil {
		return
	}
	
	// Get predecessor of our successor
	client, err := n.getClient(successor.Address)
	if err != nil {
		log.Printf("Node %s: failed to connect to successor %s: %v", 
			n.id.String()[:8], successor.Address, err)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()
	
	resp, err := client.GetInfo(ctx, &pb.GetInfoRequest{})
	if err != nil {
		log.Printf("Node %s: failed to get info from successor: %v", n.id.String()[:8], err)
		return
	}
	
	if !resp.Success {
		return
	}
	
	// If successor has a predecessor, check if we should update our successor
	if resp.Predecessor != nil {
		predID, err := hash.NewHashFromHex(resp.Predecessor.Id)
		if err != nil {
			log.Printf("Node %s: invalid predecessor ID from successor: %v", n.id.String()[:8], err)
			return
		}
		
		// If successor's predecessor is between us and our successor, update successor
		if predID.InRangeExclusive(n.id, successor.ID) {
			n.mu.Lock()
			n.successor = &NodeInfo{
				ID:      predID,
				Address: resp.Predecessor.Address,
			}
			n.mu.Unlock()
		}
	}
	
	// Notify our successor about us
	n.remoteNotify(n.successor.Address)
}

// fixFingers is called periodically to update finger table entries
func (n *Node) fixFingers() {
	n.mu.Lock()
	n.next = (n.next + 1) % FingerTableSize
	fingerStart := hash.FingerStart(n.id, n.next+1)
	n.mu.Unlock()
	
	// Find successor of finger start
	successor, err := n.findSuccessor(fingerStart)
	if err != nil {
		log.Printf("Node %s: failed to fix finger %d: %v", n.id.String()[:8], n.next, err)
		return
	}
	
	n.mu.Lock()
	n.fingers[n.next] = successor
	n.mu.Unlock()
}

// checkPredecessor is called periodically to check if predecessor is alive
func (n *Node) checkPredecessor() {
	n.mu.RLock()
	predecessor := n.predecessor
	n.mu.RUnlock()
	
	if predecessor == nil {
		return
	}
	
	// Ping predecessor
	if n.remotePing(predecessor.Address) != nil {
		n.mu.Lock()
		n.predecessor = nil
		n.mu.Unlock()
		log.Printf("Node %s: predecessor %s failed, cleared", 
			n.id.String()[:8], predecessor.ID.String()[:8])
	}
}

// startMaintenance starts the periodic maintenance routines
func (n *Node) startMaintenance() {
	// Stabilization
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(StabilizeInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-ticker.C:
				n.stabilize()
			}
		}
	}()
	
	// Fix fingers
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(FixFingersInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-ticker.C:
				n.fixFingers()
			}
		}
	}()
	
	// Check predecessor
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(CheckPredecessorInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-ticker.C:
				n.checkPredecessor()
			}
		}
	}()
}

// GetID returns the node's ID
func (n *Node) GetID() *hash.Hash {
	return n.id
}

// GetAddress returns the node's address
func (n *Node) GetAddress() string {
	return n.address
}

// GetSuccessor returns the node's successor
func (n *Node) GetSuccessor() *NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.successor
}

// GetPredecessor returns the node's predecessor
func (n *Node) GetPredecessor() *NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.predecessor
}

// GetFingers returns a copy of the finger table
func (n *Node) GetFingers() []*NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	fingers := make([]*NodeInfo, len(n.fingers))
	copy(fingers, n.fingers)
	return fingers
}

// GetStats returns basic statistics about the node
func (n *Node) GetStats() (int64, int64) {
	return n.MessageCount, n.LookupCount
}

// FindSuccessor finds the successor of the given ID
func (n *Node) FindSuccessor(ctx context.Context, req *pb.FindSuccessorRequest) (*pb.FindSuccessorResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	n.MessageCount++
	n.LookupCount++
	
	targetID, err := hash.NewHashFromHex(req.Key)
	if err != nil {
		return &pb.FindSuccessorResponse{
			Success: false,
			Error:   "invalid key format",
		}, nil
	}
	
	// If target is between us and our successor, return successor
	if n.successor != nil && targetID.InRange(n.id, n.successor.ID) {
		return &pb.FindSuccessorResponse{
			Successor: &pb.Node{
				Id:      n.successor.ID.String(),
				Address: n.successor.Address,
			},
			Success: true,
		}, nil
	}
	
	// Find closest preceding node and ask it
	precedingNode := n.closestPrecedingFinger(targetID)
	if precedingNode.Address == n.address {
		// We are the closest, return our successor
		return &pb.FindSuccessorResponse{
			Successor: &pb.Node{
				Id:      n.successor.ID.String(),
				Address: n.successor.Address,
			},
			Success: true,
		}, nil
	}
	
	// Forward request to closest preceding node
	client, err := n.getClient(precedingNode.Address)
	if err != nil {
		return &pb.FindSuccessorResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to connect to node: %v", err),
		}, nil
	}
	
	return client.FindSuccessor(ctx, req)
}

// Notify is called by another node that thinks it might be our predecessor
func (n *Node) Notify(ctx context.Context, req *pb.NotifyRequest) (*pb.NotifyResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	n.MessageCount++
	
	notifierID, err := hash.NewHashFromHex(req.Node.Id)
	if err != nil {
		return &pb.NotifyResponse{Success: false, Error: "invalid node ID"}, nil
	}
	
	// If we don't have a predecessor or the notifier is between our predecessor and us
	if n.predecessor == nil || notifierID.InRangeExclusive(n.predecessor.ID, n.id) {
		n.predecessor = &NodeInfo{
			ID:      notifierID,
			Address: req.Node.Address,
		}
		log.Printf("Node %s updated predecessor to %s", 
			n.id.String()[:8], n.predecessor.ID.String()[:8])
	}
	
	return &pb.NotifyResponse{Success: true}, nil
}

// GetInfo returns information about this node
func (n *Node) GetInfo(ctx context.Context, req *pb.GetInfoRequest) (*pb.GetInfoResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	n.MessageCount++
	
	response := &pb.GetInfoResponse{
		Node: &pb.Node{
			Id:      n.id.String(),
			Address: n.address,
		},
		Success: true,
	}
	
	if n.predecessor != nil {
		response.Predecessor = &pb.Node{
			Id:      n.predecessor.ID.String(),
			Address: n.predecessor.Address,
		}
	}
	
	if n.successor != nil {
		response.Successor = &pb.Node{
			Id:      n.successor.ID.String(),
			Address: n.successor.Address,
		}
	}
	
	// Add finger table entries
	for _, finger := range n.fingers {
		if finger != nil {
			response.Fingers = append(response.Fingers, &pb.Node{
				Id:      finger.ID.String(),
				Address: finger.Address,
			})
		}
	}
	
	return response, nil
}

// Ping responds to ping requests
func (n *Node) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	n.MessageCount++
	
	return &pb.PingResponse{
		Alive:     true,
		Timestamp: time.Now().UnixNano(),
	}, nil
}

// ClosestPrecedingFinger finds the closest preceding finger for a key
func (n *Node) ClosestPrecedingFinger(ctx context.Context, req *pb.ClosestPrecedingFingerRequest) (*pb.ClosestPrecedingFingerResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	n.MessageCount++
	
	key, err := hash.NewHashFromHex(req.Key)
	if err != nil {
		return &pb.ClosestPrecedingFingerResponse{
			Success: false,
			Error:   "invalid key format",
		}, nil
	}
	
	closest := n.closestPrecedingFinger(key)
	if closest == nil {
		closest = &NodeInfo{
			ID:      n.id,
			Address: n.address,
		}
	}
	
	return &pb.ClosestPrecedingFingerResponse{
		Node: &pb.Node{
			Id:      closest.ID.String(),
			Address: closest.Address,
		},
		Success: true,
	}, nil
}

// getClient returns a gRPC client for the given address
func (n *Node) getClient(address string) (pb.ChordServiceClient, error) {
	n.mu.RLock()
	client, exists := n.clients[address]
	n.mu.RUnlock()
	
	if exists {
		return client, nil
	}
	
	// Create new connection
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}
	
	client = pb.NewChordServiceClient(conn)
	
	n.mu.Lock()
	n.clients[address] = client
	n.connections[address] = conn
	n.mu.Unlock()
	
	return client, nil
}

// MustEmbedUnimplementedChordServiceServer is required by the generated gRPC code
func (n *Node) mustEmbedUnimplementedChordServiceServer() {}

// remoteFindSuccessor calls FindSuccessor on a remote node
func (n *Node) remoteFindSuccessor(address string, key *hash.Hash) (*NodeInfo, error) {
	client, err := n.getClient(address)
	if err != nil {
		return nil, err
	}
	
	req := &pb.FindSuccessorRequest{
		Key: key.String(),
		Requester: &pb.Node{
			Id:      n.id.String(),
			Address: n.address,
		},
	}
	
	resp, err := client.FindSuccessor(context.Background(), req)
	if err != nil {
		return nil, err
	}
	
	if !resp.Success {
		return nil, fmt.Errorf("remote error: %s", resp.Error)
	}
	
	successorID, err := hash.NewHashFromHex(resp.Successor.Id)
	if err != nil {
		return nil, err
	}
	
	return &NodeInfo{
		ID:      successorID,
		Address: resp.Successor.Address,
	}, nil
}

// remotePing calls Ping on a remote node
func (n *Node) remotePing(address string) error {
	client, err := n.getClient(address)
	if err != nil {
		return err
	}
	
	req := &pb.PingRequest{
		Requester: &pb.Node{
			Id:      n.id.String(),
			Address: n.address,
		},
	}
	
	_, err = client.Ping(context.Background(), req)
	return err
}

// remoteNotify calls Notify on a remote node
func (n *Node) remoteNotify(address string) error {
	client, err := n.getClient(address)
	if err != nil {
		return err
	}
	
	req := &pb.NotifyRequest{
		Node: &pb.Node{
			Id:      n.id.String(),
			Address: n.address,
		},
	}
	
	_, err = client.Notify(context.Background(), req)
	return err
}
