package scoutsubs

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	kad "github.com/libp2p/go-libp2p-kad-dht"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
)

var testPrefix = kad.ProtocolPrefix("/test")

type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

func setupDHT(ctx context.Context, t *testing.T, client bool, options ...kad.Option) *kad.IpfsDHT {
	baseOpts := []kad.Option{
		testPrefix,
		kad.NamespacedValidator("v", blankValidator{}),
		kad.DisableAutoRefresh(),
	}

	if client {
		baseOpts = append(baseOpts, kad.Mode(kad.ModeClient))
	} else {
		baseOpts = append(baseOpts, kad.Mode(kad.ModeServer))
	}

	host, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	host.Start()
	t.Cleanup(func() { host.Close() })

	d, err := kad.New(ctx, host, append(baseOpts, options...)...)
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	return d
}

func setupDHTS(t *testing.T, ctx context.Context, n int, options ...kad.Option) []*kad.IpfsDHT {
	addrs := make([]ma.Multiaddr, n)
	dhts := make([]*kad.IpfsDHT, n)
	peers := make([]peer.ID, n)

	sanityAddrsMap := make(map[string]struct{})
	sanityPeersMap := make(map[string]struct{})

	for i := 0; i < n; i++ {
		dhts[i] = setupDHT(ctx, t, false, options...)
		peers[i] = dhts[i].PeerID()
		addrs[i] = dhts[i].Host().Addrs()[0]

		if _, lol := sanityAddrsMap[addrs[i].String()]; lol {
			t.Fatal("While setting up DHTs address got duplicated.")
		} else {
			sanityAddrsMap[addrs[i].String()] = struct{}{}
		}
		if _, lol := sanityPeersMap[peers[i].String()]; lol {
			t.Fatal("While setting up DHTs peerid got duplicated.")
		} else {
			sanityPeersMap[peers[i].String()] = struct{}{}
		}
	}

	return dhts
}

func connectNoSync(t *testing.T, ctx context.Context, a, b *kad.IpfsDHT) {
	t.Helper()

	idB := b.PeerID()
	addrB := b.Host().Peerstore().Addrs(idB)
	if len(addrB) == 0 {
		t.Fatal("peers setup incorrectly: no local address")
	}

	if err := a.Host().Connect(ctx, peer.AddrInfo{ID: idB, Addrs: addrB}); err != nil {
		t.Fatal(err)
	}
}

func wait(t *testing.T, ctx context.Context, a, b *kad.IpfsDHT) {
	t.Helper()

	// loop until connection notification has been received.
	// under high load, this may not happen as immediately as we would like.
	for a.RoutingTable().Find(b.PeerID()) == "" {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond * 5):
		}
	}
}

func connect(t *testing.T, ctx context.Context, a, b *kad.IpfsDHT) {
	t.Helper()
	connectNoSync(t, ctx, a, b)
	wait(t, ctx, a, b)
	wait(t, ctx, b, a)
}

func bootstrap(t *testing.T, ctx context.Context, dhts []*kad.IpfsDHT) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Println("refreshing DHTs routing tables...")

	// tried async. sequential fares much better. compare:
	// 100 async https://gist.github.com/jbenet/56d12f0578d5f34810b2
	// 100 sync https://gist.github.com/jbenet/6c59e7c15426e48aaedd
	// probably because results compound

	start := rand.Intn(len(dhts)) // randomize to decrease bias.
	for i := range dhts {
		dht := dhts[(start+i)%len(dhts)]
		select {
		case err := <-dht.RefreshRoutingTable():
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func printRoutingTables(dhts []*kad.IpfsDHT) {
	// the routing tables should be full now. let's inspect them.
	fmt.Printf("checking routing table of %d\n", len(dhts))
	for _, dht := range dhts {
		fmt.Printf("checking routing table of %s\n", dht.PeerID())
		dht.RoutingTable().Print()
		fmt.Println("")
	}
}

func checkForWellFormedTablesOnce(t *testing.T, dhts []*kad.IpfsDHT, minPeers, avgPeers int) bool {
	t.Helper()
	totalPeers := 0
	for _, dht := range dhts {
		rtlen := dht.RoutingTable().Size()
		totalPeers += rtlen
		if minPeers > 0 && rtlen < minPeers {
			// t.Logf("routing table for %s only has %d peers (should have >%d)", dht.self, rtlen, minPeers)
			return false
		}
	}
	actualAvgPeers := totalPeers / len(dhts)
	t.Logf("avg rt size: %d", actualAvgPeers)
	if avgPeers > 0 && actualAvgPeers < avgPeers {
		t.Logf("avg rt size: %d < %d", actualAvgPeers, avgPeers)
		return false
	}
	return true
}

// bootstrapHelper organizes peers from closest to furthest to a key
func bootstrapHelper(kads []*kad.IpfsDHT, attr string) []int {

	var order []int
	orderedPeers := kads[0].RoutingTable().NearestPeers(kb.ConvertKey(attr), len(kads)-1)
	for _, p := range orderedPeers {
		for i, kad := range kads {
			if kad.PeerID().Pretty() == p.Pretty() {
				order = append(order, i)
			}
		}
	}

	return order
}
