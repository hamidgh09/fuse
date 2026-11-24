package fs

// Kernel-free tests for the node reference bookkeeping and the NodeRetirer
// hooks. These run against a Server with no connection; saveNode, dropNode,
// saveLookup and forgetOne never touch conn.

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"

	"bazil.org/fuse"
)

type plainTestNode struct {
	forgets int32
}

func (n *plainTestNode) Attr(ctx context.Context, a *fuse.Attr) error { return nil }
func (n *plainTestNode) Forget()                                      { atomic.AddInt32(&n.forgets, 1) }

var _ Node = (*plainTestNode)(nil)
var _ NodeForgetter = (*plainTestNode)(nil)

type retirerTestNode struct {
	plainTestNode
	registers int32
	retires   int32
	retired   bool
}

func (n *retirerTestNode) RegisterLocked() bool {
	atomic.AddInt32(&n.registers, 1)
	return !n.retired
}

func (n *retirerTestNode) RetireLocked() {
	atomic.AddInt32(&n.retires, 1)
	n.retired = true
}

var _ NodeRetirer = (*retirerTestNode)(nil)

func newInternalTestServer() *Server {
	s := New(nil, &Config{Debug: func(interface{}) {}})
	// Serve() reserves index 0 and the root at index 1; mimic that.
	s.node = append(s.node, nil, nil)
	return s
}

func TestInternalSaveNodeRegistersOnce(t *testing.T) {
	s := newInternalTestServer()
	n := &retirerTestNode{}

	id1, gen1, ok1 := s.saveNode(42, n)
	id2, gen2, ok2 := s.saveNode(42, n)

	if !ok1 || !ok2 {
		t.Fatalf("saveNode refused a live node: %v %v", ok1, ok2)
	}
	if id1 != id2 || gen1 != gen2 {
		t.Fatalf("second saveNode must re-reference: got (%v,%v) and (%v,%v)", id1, gen1, id2, gen2)
	}
	if got := atomic.LoadInt32(&n.registers); got != 1 {
		t.Fatalf("RegisterLocked called %d times, want 1 (not on re-reference)", got)
	}
	if s.node[id1].refs != 2 {
		t.Fatalf("refs = %d, want 2", s.node[id1].refs)
	}
}

func TestInternalDropNodeRetiresUnderMeta(t *testing.T) {
	s := newInternalTestServer()
	n := &retirerTestNode{}
	id, _, _ := s.saveNode(42, n)

	node, forget := s.dropNode(id, 1)

	if node != Node(n) {
		t.Fatalf("dropNode returned node %v, want the retirer", node)
	}
	if forget {
		t.Fatalf("dropNode must return forget=false for a retirer so callers do not also call Forget")
	}
	if got := atomic.LoadInt32(&n.retires); got != 1 {
		t.Fatalf("RetireLocked called %d times, want 1", got)
	}
	if got := atomic.LoadInt32(&n.forgets); got != 0 {
		t.Fatalf("Forget called %d times on a retirer, want 0", got)
	}
	if _, ok := s.nodeRef[n]; ok {
		t.Fatalf("nodeRef still holds the retired node")
	}
	if s.node[id] != nil {
		t.Fatalf("node slot %v not freed", id)
	}
}

func TestInternalDropNodePartialDoesNotRetire(t *testing.T) {
	s := newInternalTestServer()
	n := &retirerTestNode{}
	id, _, _ := s.saveNode(42, n)
	s.saveNode(42, n)

	_, forget := s.dropNode(id, 1)

	if forget || atomic.LoadInt32(&n.retires) != 0 {
		t.Fatalf("node with remaining refs must not retire")
	}
	if s.node[id].refs != 1 {
		t.Fatalf("refs = %d, want 1", s.node[id].refs)
	}
}

// The race the contract exists for: an entry reply selected the node, the
// kernel's last reference went, and only then does the reply try to register.
func TestInternalRetiredNodeIsNotRegisteredAgain(t *testing.T) {
	s := newInternalTestServer()
	n := &retirerTestNode{}
	id, _, _ := s.saveNode(42, n)
	s.dropNode(id, 1)
	slots := len(s.node)
	free := len(s.freeNode)

	_, _, ok := s.saveNode(42, n)

	if ok {
		t.Fatalf("saveNode registered a retired node")
	}
	if _, tracked := s.nodeRef[n]; tracked {
		t.Fatalf("nodeRef holds a node whose registration was refused")
	}
	if len(s.node) != slots || len(s.freeNode) != free {
		t.Fatalf("refused registration consumed a node slot")
	}
}

func TestInternalSaveLookupRefusedIsESTALE(t *testing.T) {
	s := newInternalTestServer()
	parent := &serveNode{inode: 1, node: &plainTestNode{}}
	n := &retirerTestNode{retired: true}

	err := s.saveLookup(context.Background(), &fuse.LookupResponse{}, parent, "child", n)

	if !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("saveLookup of a retired node: got %v, want ESTALE", err)
	}
}

func TestInternalForgetOnePlainNodeGetsForget(t *testing.T) {
	s := newInternalTestServer()
	n := &plainTestNode{}
	id, _, _ := s.saveNode(42, n)

	s.forgetOne(id, 1)

	if got := atomic.LoadInt32(&n.forgets); got != 1 {
		t.Fatalf("Forget called %d times, want 1", got)
	}
	if _, ok := s.nodeRef[n]; ok {
		t.Fatalf("nodeRef still holds the forgotten node")
	}
}

func TestInternalForgetOneRetirerNoForget(t *testing.T) {
	s := newInternalTestServer()
	n := &retirerTestNode{}
	id, _, _ := s.saveNode(42, n)

	s.forgetOne(id, 1)

	if atomic.LoadInt32(&n.retires) != 1 || atomic.LoadInt32(&n.forgets) != 0 {
		t.Fatalf("retirer must be retired exactly once and never Forget-ed: retires=%d forgets=%d",
			n.retires, n.forgets)
	}
}

func TestInternalDropNodeOutOfRangeDoesNotPanic(t *testing.T) {
	s := newInternalTestServer()

	node, forget := s.dropNode(fuse.NodeID(len(s.node)+10), 1)

	if node != nil || forget {
		t.Fatalf("out-of-range id must be ignored, got node=%v forget=%v", node, forget)
	}
	s.forgetOne(fuse.NodeID(len(s.node)+10), 1)
}

func TestInternalDropNodeStaleSlot(t *testing.T) {
	s := newInternalTestServer()
	n := &plainTestNode{}
	id, _, _ := s.saveNode(42, n)
	s.dropNode(id, 1)

	// Second drop on an already-freed slot: refcount disagreement path.
	node, forget := s.dropNode(id, 1)
	if node != nil || !forget {
		t.Fatalf("stale slot: want (nil, true) got (%v, %v)", node, forget)
	}
	s.forgetOne(id, 1)
	if n.forgets != 0 {
		t.Fatalf("Forget must not run for a nil node, got %d", n.forgets)
	}
}

func TestInternalInlineForgetRefusesDeadlockingNotifications(t *testing.T) {
	s := New(nil, &Config{InlineForget: true})
	node := &plainTestNode{}

	if err := s.NotifyDelete(node, node, "name"); !errors.Is(err, ErrInlineForget) {
		t.Errorf("NotifyDelete: got %v, want ErrInlineForget", err)
	}
	if _, err := s.NotifyRetrieve(node, 0, 1); !errors.Is(err, ErrInlineForget) {
		t.Errorf("NotifyRetrieve: got %v, want ErrInlineForget", err)
	}
	if err := s.InvalidateEntry(node, "name"); !errors.Is(err, fuse.ErrNotCached) {
		t.Errorf("InvalidateEntry: got %v, want ErrNotCached", err)
	}
}
