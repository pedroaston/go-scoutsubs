package scoutsubs

import (
	"context"
	"testing"
	"time"
)

func TestXX(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 30
	dhts := setupDHTS(t, ctx, nDHTs)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].Host().Close()
		}
	}()

	t.Logf("connecting %d dhts in a ring", nDHTs)
	for i := 0; i < nDHTs; i++ {
		connect(t, ctx, dhts[i], dhts[(i+1)%len(dhts)])
	}

	<-time.After(100 * time.Millisecond)
	// bootstrap a few times until we get good tables.
	t.Logf("bootstrapping them so they find each other %d", nDHTs)

	for {
		bootstrap(t, ctx, dhts)

		if checkForWellFormedTablesOnce(t, dhts, 7, 10) {
			break
		}

		time.Sleep(time.Microsecond * 50)
	}

	printRoutingTables(dhts)
}
