package scoutsubs

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSubAndUnsub just subscribes to certain
// events and then unsubscribes to them
// Test composition: 3 nodes
// >> 1 Subscriber that subscribes and unsubscribes
// >> 2 nodes that saves the other's subscription
func TestSubAndUnsub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize kademlia dhts
	dhts := setupDHTS(t, ctx, 4)
	var pubsubs [4]*PubSub
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.Host().Close()
		}

		for _, pubsub := range pubsubs {
			pubsub.TerminateService()
		}
	}()

	// One dht will act has bootstrapper
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[0], dhts[2])
	connect(t, ctx, dhts[0], dhts[3])

	// Connect peers to achieve a dissemination chain for the attribute
	// portugal and remove bootstrapper from routing table
	helper := bootstrapHelper(dhts, "portugal")
	dhts[1].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[2].RoutingTable().RemovePeer(dhts[0].PeerID())
	dhts[3].RoutingTable().RemovePeer(dhts[0].PeerID())
	connect(t, ctx, dhts[helper[0]], dhts[helper[1]])
	connect(t, ctx, dhts[helper[1]], dhts[helper[2]])

	// Initialize pub-subs
	for i, dht := range dhts {
		pubsubs[i] = NewPubSub(dht, DefaultConfig("PT", 10))
		pubsubs[i].SetHasOldPeer()
	}

	// The peer at the edge of the chain subscribes
	err := pubsubs[helper[2]].MySubscribe("portugal T")
	if err != nil {
		t.Fatal(err)
	} else if len(pubsubs[helper[2]].myFilters.filters[1]) != 1 {
		t.Fatal("Error Subscribing!")
	}

	// Wait for subscriptions to be disseminated
	time.Sleep(2 * time.Second)

	// Confirm that all intermidiate nodes plus the rendezvous have a filter
	if len(pubsubs[helper[1]].currentFilterTable.routes[peer.Encode(pubsubs[helper[2]].ipfsDHT.PeerID())].filters[1]) != 1 ||
		len(pubsubs[helper[0]].currentFilterTable.routes[peer.Encode(pubsubs[helper[1]].ipfsDHT.PeerID())].filters[1]) != 1 {

		t.Fatal("Failed Unsubscribing!")
	}

	// Unsubscribing operation
	pubsubs[helper[2]].MyUnsubscribe("portugal T")
	// Wait for subscriptions to be disseminated
	time.Sleep(2 * time.Second)

	if len(pubsubs[helper[2]].myFilters.filters[1]) != 0 {
		t.Fatal("Failed Unsubscribing!")
	}
}
