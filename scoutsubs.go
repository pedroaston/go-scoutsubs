package scoutsubs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	kb "github.com/libp2p/go-libp2p-kbucket"
	key "github.com/libp2p/go-libp2p-kbucket/keyspace"
	pb "github.com/pedroaston/contentpubsub/pb"

	"google.golang.org/grpc"
)

// PubSub supports all the middleware logic
type PubSub struct {
	maxAttributesPerPredicate int
	timeToCheckDelivery       time.Duration
	opResendRate              time.Duration
	rpcTimeout                time.Duration
	faultToleranceFactor      int
	activeRedirect            bool
	activeReliability         bool
	addrOption                bool

	pb.UnimplementedScoutHubServer
	server     *grpc.Server
	serverAddr string

	ipfsDHT *kaddht.IpfsDHT

	currentFilterTable *FilterTable
	nextFilterTable    *FilterTable
	myFilters          *RouteStats

	rvCache []string

	myBackups        []string
	myBackupsFilters map[string]*FilterTable

	myTrackers      map[string]*Tracker
	myETrackers     map[string]*EventLedger
	myEBackTrackers map[string]*EventLedger

	interestingEvents      chan *pb.Event
	heartbeatTicker        *time.Ticker
	refreshTicker          *time.Ticker
	eventTicker            *time.Ticker
	subTicker              *time.Ticker
	eventTickerState       bool
	subTickerState         bool
	terminate              chan string
	unconfirmedEvents      map[string]*PubEventState
	totalUnconfirmedEvents int
	unconfirmedSubs        map[string]*SubState
	totalUnconfirmedSubs   int
	lives                  int

	tablesLock *sync.RWMutex
	upBackLock *sync.RWMutex
	ackOpLock  *sync.Mutex
	ackUpLock  *sync.Mutex

	record   *HistoryRecord
	session  int
	eventSeq int
}

func NewPubSub(dht *kaddht.IpfsDHT, cfg *SetupPubSub) *PubSub {

	filterTable := NewFilterTable(dht, cfg.ProductionReady)
	auxFilterTable := NewFilterTable(dht, cfg.ProductionReady)
	mySubs := NewRouteStats("no need")

	ps := &PubSub{
		maxAttributesPerPredicate: cfg.MaxAttributesPerPredicate,
		timeToCheckDelivery:       cfg.TimeToCheckDelivery,
		faultToleranceFactor:      cfg.FaultToleranceFactor,
		opResendRate:              cfg.OpResendRate,
		activeRedirect:            cfg.RedirectMechanism,
		activeReliability:         cfg.ReliableMechanisms,
		addrOption:                cfg.ProductionReady,
		rpcTimeout:                cfg.RPCTimeout,
		currentFilterTable:        filterTable,
		nextFilterTable:           auxFilterTable,
		myFilters:                 mySubs,
		ipfsDHT:                   dht,
		myBackupsFilters:          make(map[string]*FilterTable),
		myTrackers:                make(map[string]*Tracker),
		myETrackers:               make(map[string]*EventLedger),
		myEBackTrackers:           make(map[string]*EventLedger),
		unconfirmedEvents:         make(map[string]*PubEventState),
		unconfirmedSubs:           make(map[string]*SubState),
		interestingEvents:         make(chan *pb.Event, cfg.ConcurrentProcessingFactor),
		terminate:                 make(chan string),
		heartbeatTicker:           time.NewTicker(cfg.SubRefreshRateMin),
		refreshTicker:             time.NewTicker(2 * cfg.SubRefreshRateMin),
		eventTicker:               time.NewTicker(cfg.OpResendRate),
		subTicker:                 time.NewTicker(cfg.OpResendRate),
		eventTickerState:          false,
		subTickerState:            false,
		tablesLock:                &sync.RWMutex{},
		upBackLock:                &sync.RWMutex{},
		ackOpLock:                 &sync.Mutex{},
		ackUpLock:                 &sync.Mutex{},
		totalUnconfirmedEvents:    0,
		totalUnconfirmedSubs:      0,
		record:                    NewHistoryRecord(),
		session:                   rand.Intn(9999),
		eventSeq:                  0,
		lives:                     0,
	}

	ps.myBackups = ps.getBackups()
	ps.eventTicker.Stop()
	ps.subTicker.Stop()

	dialAddr := addrForPubSubServer(ps.ipfsDHT.Host().Addrs(), ps.addrOption)
	lis, err := net.Listen("tcp", dialAddr)
	if err != nil {
		return nil
	}

	ps.serverAddr = dialAddr
	ps.server = grpc.NewServer()
	pb.RegisterScoutHubServer(ps.server, ps)
	go ps.server.Serve(lis)
	go ps.processLoop()

	return ps
}

// +++++++++++++++++++++++++++++++ ScoutSubs ++++++++++++++++++++++++++++++++

type PubEventState struct {
	event    *pb.Event
	aged     bool
	dialAddr string
}

type SubState struct {
	sub      *pb.Subscription
	aged     bool
	dialAddr string
	started  string
}

type AckUp struct {
	dialAddr string
	eventID  *pb.EventID
	peerID   string
	rvID     string
}

// MySubscribe subscribes to certain event(s) and saves
// it in myFilters for further resubing operations and
// assess if node is interested in the events it receives
func (ps *PubSub) MySubscribe(info string) error {
	fmt.Println("MySubscribe: " + ps.serverAddr)

	p, err := NewPredicate(info, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

	_, pNew := ps.myFilters.SimpleAddSummarizedFilter(p, nil)
	if pNew != nil {
		p = pNew
	}

	for _, attr := range p.attributes {
		isRv, _ := ps.rendezvousSelfCheck(attr.name)
		if isRv {
			return nil
		}
	}

	_, minAttr, err := ps.closerAttrRvToSelf(p)
	if err != nil {
		return errors.New("failed to find the closest attribute Rv")
	}

	_, dialAddr := ps.rendezvousSelfCheck(minAttr)
	if dialAddr == "" {
		return errors.New("no address for closest peer")
	}

	if ps.activeRedirect {
		ps.tablesLock.RLock()
		ps.currentFilterTable.addToRouteTracker(minAttr, "sub")
		ps.currentFilterTable.addToRouteTracker(minAttr, "closes")
		ps.nextFilterTable.addToRouteTracker(minAttr, "sub")
		ps.nextFilterTable.addToRouteTracker(minAttr, "closes")
		ps.tablesLock.RUnlock()
	}

	sub := &pb.Subscription{
		PeerID:    peer.Encode(ps.ipfsDHT.PeerID()),
		Predicate: info,
		RvId:      minAttr,
		Shortcut:  "!",
		SubAddr:   ps.serverAddr,
		IntAddr:   ps.serverAddr,
	}

	if ps.activeReliability {
		ps.tablesLock.RLock()
		ps.ackOpLock.Lock()
		ps.unconfirmedSubs[sub.Predicate] = &SubState{
			sub:      sub,
			aged:     false,
			dialAddr: dialAddr,
			started:  time.Now().Format(time.StampMilli),
		}

		ps.totalUnconfirmedSubs++
		if !ps.subTickerState {
			ps.subTicker.Reset(ps.opResendRate)
			ps.subTickerState = true
			ps.unconfirmedSubs[sub.Predicate].aged = true
		}
		ps.ackOpLock.Unlock()
		ps.tablesLock.RUnlock()
	}

	go ps.forwardSub(dialAddr, sub)

	return nil
}

// Subscribe is a remote function called by a external peer
// to send subscriptions towards the rendezvous node
func (ps *PubSub) Subscribe(ctx context.Context, sub *pb.Subscription) (*pb.Ack, error) {
	fmt.Println("Subscribe >> " + ps.serverAddr)

	p, err := NewPredicate(sub.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: "error creating predicate"}, err
	}

	updateBackups := sub.Backups
	var backAddrs []string
	for _, addr := range sub.Backups {
		backAddrs = append(backAddrs, addr)
	}

	ps.tablesLock.Lock()
	if ps.currentFilterTable.routes[sub.PeerID] == nil {
		ps.currentFilterTable.routes[sub.PeerID] = NewRouteStats(sub.IntAddr)
		ps.nextFilterTable.routes[sub.PeerID] = NewRouteStats(sub.IntAddr)
	} else if ps.nextFilterTable.routes[sub.PeerID] == nil {
		ps.nextFilterTable.routes[sub.PeerID] = NewRouteStats(sub.IntAddr)
	}
	ps.tablesLock.Unlock()

	ps.tablesLock.RLock()
	if ps.activeRedirect {
		if sub.Shortcut == "!" {
			ps.currentFilterTable.turnOffRedirect(sub.PeerID, sub.RvId)
			ps.nextFilterTable.turnOffRedirect(sub.PeerID, sub.RvId)
		} else {
			ps.currentFilterTable.addRedirect(sub.PeerID, sub.RvId, sub.Shortcut)
			ps.nextFilterTable.addRedirect(sub.PeerID, sub.RvId, sub.Shortcut)
		}
	}

	ps.currentFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p, backAddrs)
	alreadyDone, pNew := ps.nextFilterTable.routes[sub.PeerID].SimpleAddSummarizedFilter(p, backAddrs)
	ps.tablesLock.RUnlock()

	if alreadyDone {
		if ps.activeReliability {
			ps.sendAckOp(sub.SubAddr, "Subscribe", sub.Predicate)
		}
		return &pb.Ack{State: true, Info: ""}, nil
	} else if pNew != nil {
		sub.Predicate = pNew.ToString()
	}

	isRv, nextHopAddr := ps.rendezvousSelfCheck(sub.RvId)
	if !isRv && nextHopAddr != "" {

		var backups map[int32]string = make(map[int32]string)
		for i, backup := range ps.myBackups {
			backups[int32(i)] = backup
		}

		subForward := &pb.Subscription{
			PeerID:    peer.Encode(ps.ipfsDHT.PeerID()),
			Predicate: sub.Predicate,
			RvId:      sub.RvId,
			Backups:   backups,
			SubAddr:   sub.SubAddr,
			IntAddr:   ps.serverAddr,
		}

		if ps.activeRedirect {
			ps.tablesLock.RLock()
			ps.currentFilterTable.redirectLock.Lock()

			if len(ps.currentFilterTable.routeTracker[sub.RvId]) >= 2 {
				ps.currentFilterTable.redirectLock.Unlock()
				ps.tablesLock.RUnlock()
				subForward.Shortcut = "!"
			} else if sub.Shortcut != "!" {
				ps.currentFilterTable.redirectLock.Unlock()
				ps.tablesLock.RUnlock()
				subForward.Shortcut = sub.Shortcut
			} else {
				subForward.Shortcut = ps.currentFilterTable.routes[sub.PeerID].addr
				ps.currentFilterTable.redirectLock.Unlock()
				ps.tablesLock.RUnlock()
			}
		}

		go ps.forwardSub(nextHopAddr, subForward)
		ps.updateMyBackups(sub.PeerID, sub.Predicate, updateBackups)
	} else if !isRv {
		return &pb.Ack{State: false, Info: "rendezvous check failed"}, nil
	} else {
		if ps.activeReliability {
			ps.sendAckOp(sub.SubAddr, "Subscribe", sub.Predicate)
		}

		ps.updateRvRegion(sub.PeerID, sub.Predicate, sub.RvId, updateBackups)
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardSub is called once a received subscription
// still needs to be forward towards the rendevous
func (ps *PubSub) forwardSub(dialAddr string, sub *pb.Subscription) {

	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
	defer cancel()

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	ack, err := client.Subscribe(ctx, sub)
	if err != nil || !ack.State {
		alternatives := ps.alternativesToRv(sub.RvId)
		for _, addr := range alternatives {
			if addr == ps.serverAddr {
				return
			}

			ctxBackup, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Subscribe(ctxBackup, sub)
			if err == nil && ack.State {
				break
			}
		}
	}
}

// MyUnsubscribe deletes a specific predicate out of mySubs
// list which will stop the refreshing of thatsub and stop
// delivering to the user those contained events
func (ps *PubSub) MyUnsubscribe(info string) error {
	fmt.Println("MyUnsubscribe: " + ps.serverAddr)

	p, err := NewPredicate(info, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

	ps.myFilters.SimpleSubtractFilter(p)

	return nil
}

// MyPublish function is used when we want to publish an event on the overlay.
// Data is the message we want to publish and info is the representative
// predicate of that event data. The publish operation is made towards all
// attributes rendezvous in order find the way to all subscribers
func (ps *PubSub) MyPublish(data string, info string) error {
	fmt.Println("MyPublish: " + ps.serverAddr)

	p, err := NewPredicate(info, ps.maxAttributesPerPredicate)
	if err != nil {
		return err
	}

	eventID := &pb.EventID{
		PublisherID:   peer.Encode(ps.ipfsDHT.PeerID()),
		SessionNumber: int32(ps.session),
		SeqID:         int32(ps.eventSeq),
	}

	notSent := true
	for _, attr := range p.attributes {

		event := &pb.Event{
			EventID:   eventID,
			Event:     data,
			Predicate: info,
			RvId:      attr.name,
			LastHop:   peer.Encode(ps.ipfsDHT.PeerID()),
			AckAddr:   ps.serverAddr,
			Backup:    false,
			BirthTime: time.Now().Format(time.StampMilli),
			PubAddr:   ps.serverAddr,
		}

		isRv, nextRvHopAddr := ps.rendezvousSelfCheck(attr.name)
		eLog := make(map[string]bool)

		if isRv && notSent {
			notSent = false
			ps.tablesLock.RLock()
			for next, route := range ps.currentFilterTable.routes {
				if route.IsInterested(p) {

					eLog[next] = false
					dialAddr := route.addr
					newE := &pb.Event{
						Event:         event.Event,
						OriginalRoute: next,
						Backup:        event.Backup,
						Predicate:     event.Predicate,
						RvId:          event.RvId,
						AckAddr:       event.AckAddr,
						PubAddr:       event.PubAddr,
						EventID:       event.EventID,
						BirthTime:     event.BirthTime,
						LastHop:       event.LastHop,
					}

					if ps.activeRedirect {

						ps.currentFilterTable.redirectLock.Lock()
						ps.nextFilterTable.redirectLock.Lock()

						if ps.currentFilterTable.redirectTable[next] == nil {
							ps.currentFilterTable.redirectTable[next] = make(map[string]string)
							ps.currentFilterTable.redirectTable[next][event.RvId] = ""
							ps.nextFilterTable.redirectTable[next] = make(map[string]string)
							ps.nextFilterTable.redirectTable[next][event.RvId] = ""

							go ps.forwardEventDown(dialAddr, newE, "", route.backups)
						} else if ps.currentFilterTable.redirectTable[next][event.RvId] == "" {
							go ps.forwardEventDown(dialAddr, newE, "", route.backups)
						} else {
							go ps.forwardEventDown(dialAddr, newE, ps.currentFilterTable.redirectTable[next][event.RvId], route.backups)
						}

						ps.currentFilterTable.redirectLock.Unlock()
						ps.nextFilterTable.redirectLock.Unlock()

					} else {
						go ps.forwardEventDown(dialAddr, newE, "", route.backups)
					}
				}
			}
			ps.tablesLock.RUnlock()

			if ps.activeReliability && len(eLog) > 0 {
				ps.sendLogToTrackers(attr.name, eventID, eLog, event)
			}
		} else {

			if ps.activeReliability {

				eID := fmt.Sprintf("%s%d%d%s", event.EventID.PublisherID, event.EventID.SessionNumber, event.EventID.SeqID, event.RvId)
				ps.tablesLock.RLock()
				ps.ackOpLock.Lock()
				ps.unconfirmedEvents[eID] = &PubEventState{event: event, aged: false, dialAddr: nextRvHopAddr}
				ps.totalUnconfirmedEvents++
				if !ps.eventTickerState {
					ps.eventTicker.Reset(ps.opResendRate)
					ps.eventTickerState = true
					ps.unconfirmedEvents[eID].aged = true
				}
				ps.ackOpLock.Unlock()
				ps.tablesLock.RUnlock()

			} else {
				ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
				defer cancel()

				ps.Notify(ctx, event)
			}

			go ps.forwardEventUp(nextRvHopAddr, event)
		}
	}

	ps.eventSeq++
	return nil
}

// Publish is a remote function called by a external peer to send an Event upstream
func (ps *PubSub) Publish(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Println("Publish >> " + ps.serverAddr)

	p, err := NewPredicate(event.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	isRv, nextRvHopAddr := ps.rendezvousSelfCheck(event.RvId)
	if !isRv && nextRvHopAddr != "" {

		newE := &pb.Event{
			Event:         event.Event,
			OriginalRoute: event.OriginalRoute,
			Backup:        event.Backup,
			Predicate:     event.Predicate,
			RvId:          event.RvId,
			AckAddr:       event.AckAddr,
			PubAddr:       event.PubAddr,
			EventID:       event.EventID,
			BirthTime:     event.BirthTime,
			LastHop:       peer.Encode(ps.ipfsDHT.PeerID()),
		}

		go ps.forwardEventUp(nextRvHopAddr, newE)

		if !ps.activeReliability {
			ps.Notify(ctx, event)
		}

	} else if !isRv {
		return &pb.Ack{State: false, Info: "rendezvous check failed"}, nil
	} else if isRv {
		if ps.iAmRVPublish(p, event, false) != nil {
			return &pb.Ack{State: false, Info: "rendezvous role failed"}, nil
		}
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

func (ps *PubSub) iAmRVPublish(p *Predicate, event *pb.Event, failedRv bool) error {

	eL := make(map[string]bool)
	if failedRv && ps.lives >= 1 {
		for backup := range ps.myBackupsFilters {
			backupID, _ := peer.Decode(backup)
			if kb.Closer(backupID, ps.ipfsDHT.PeerID(), event.RvId) {

				newE := &pb.Event{
					Event:         event.Event,
					OriginalRoute: backup,
					Backup:        true,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					AckAddr:       ps.serverAddr,
					PubAddr:       event.PubAddr,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
				defer cancel()

				ack, err := ps.Notify(ctx, newE)
				if err == nil && ack.State {
					eL[backup] = false
				}

				break
			}
		}
	} else if ps.lives < 1 {
		alternatives := ps.alternativesToRv(event.RvId)
		for _, addr := range alternatives {
			ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.HelpNewRv(ctx, event)
			if err == nil && ack.State {
				eL[ack.Info] = false
				break
			}
		}
	}

	ps.tablesLock.RLock()
	eIDRv := fmt.Sprintf("%s%d%d", event.EventID.PublisherID, event.EventID.SessionNumber, event.EventID.SeqID)
	for _, cached := range ps.rvCache {
		if eIDRv == cached {
			return nil
		}
	}

	ps.rvCache = append(ps.rvCache, eIDRv)
	event.AckAddr = ps.serverAddr

	for next, route := range ps.currentFilterTable.routes {
		if !ps.activeReliability && next == event.LastHop {
			continue
		}

		if route.IsInterested(p) {
			eL[next] = false
			dialAddr := route.addr
			newE := &pb.Event{
				Event:         event.Event,
				OriginalRoute: next,
				Backup:        event.Backup,
				Predicate:     event.Predicate,
				RvId:          event.RvId,
				AckAddr:       event.AckAddr,
				PubAddr:       event.PubAddr,
				EventID:       event.EventID,
				BirthTime:     event.BirthTime,
				LastHop:       event.LastHop,
			}

			if ps.activeRedirect {

				ps.currentFilterTable.redirectLock.Lock()
				ps.nextFilterTable.redirectLock.Lock()

				if ps.currentFilterTable.redirectTable[next] == nil {
					ps.currentFilterTable.redirectTable[next] = make(map[string]string)
					ps.currentFilterTable.redirectTable[next][event.RvId] = ""
					ps.nextFilterTable.redirectTable[next] = make(map[string]string)
					ps.nextFilterTable.redirectTable[next][event.RvId] = ""

					go ps.forwardEventDown(dialAddr, newE, "", route.backups)
				} else if ps.currentFilterTable.redirectTable[next][event.RvId] == "" {
					go ps.forwardEventDown(dialAddr, newE, "", route.backups)
				} else {
					go ps.forwardEventDown(dialAddr, newE, ps.currentFilterTable.redirectTable[next][event.RvId], route.backups)
				}

				ps.currentFilterTable.redirectLock.Unlock()
				ps.nextFilterTable.redirectLock.Unlock()

			} else {
				go ps.forwardEventDown(dialAddr, newE, "", route.backups)
			}
		}
	}
	ps.tablesLock.RUnlock()

	if ps.activeReliability {
		eID := fmt.Sprintf("%s%d%d%s", event.EventID.PublisherID, event.EventID.SessionNumber, event.EventID.SeqID, event.RvId)
		if len(eL) > 0 {
			ps.sendLogToTrackers(event.RvId, event.EventID, eL, event)
		}

		ps.sendAckOp(event.PubAddr, "Publish", eID)
	}

	if ps.myFilters.IsInterested(p) {
		ps.interestingEvents <- event
	}

	return nil
}

func (ps *PubSub) HelpNewRv(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Println("HelpNewRv >> " + ps.serverAddr)

	for backup := range ps.myBackupsFilters {
		backupID, _ := peer.Decode(backup)
		if kb.Closer(backupID, ps.ipfsDHT.PeerID(), event.RvId) {
			if ps.lives < 1 {
				return &pb.Ack{State: false, Info: "new node"}, nil
			}

			newE := &pb.Event{
				Event:         event.Event,
				OriginalRoute: backup,
				Backup:        true,
				Predicate:     event.Predicate,
				RvId:          event.RvId,
				AckAddr:       event.AckAddr,
				PubAddr:       event.PubAddr,
				EventID:       event.EventID,
				BirthTime:     event.BirthTime,
				LastHop:       event.LastHop,
			}

			ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			ack, err := ps.Notify(ctx, newE)
			if err == nil && ack.State {
				return &pb.Ack{State: true, Info: peer.Encode(ps.ipfsDHT.PeerID())}, nil
			}

			break
		}
	}

	return &pb.Ack{State: false, Info: "unexpected result"}, nil
}

// sendAckOp just sends an ack to the operation initiator to confirm completion
func (ps *PubSub) sendAckOp(dialAddr string, Op string, info string) {
	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
	defer cancel()

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	ack := &pb.Ack{
		State: true,
		Op:    Op,
		Info:  info,
	}

	client := pb.NewScoutHubClient(conn)
	client.AckOp(ctx, ack)
}

// sendAckUp sends an ack upstream to confirm event delivery
func (ps *PubSub) sendAckUp(ack *AckUp) {

	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
	defer cancel()

	eAck := &pb.EventAck{
		EventID: ack.eventID,
		PeerID:  ack.peerID,
		RvID:    ack.rvID,
	}

	if ack.dialAddr == ps.serverAddr {
		ps.AckUp(ctx, eAck)
		return
	}

	conn, err := grpc.Dial(ack.dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	client.AckUp(ctx, eAck)
}

// sendLogToTracker
func (ps *PubSub) sendLogToTrackers(attr string, eID *pb.EventID, eLog map[string]bool, e *pb.Event) {

	eIDL := fmt.Sprintf("%s%d%d", e.EventID.PublisherID, e.EventID.SessionNumber, e.EventID.SeqID)
	if ps.myTrackers[attr] != nil && ps.myTrackers[attr].leader {
		ps.myTrackers[attr].newEventToCheck(NewEventLedger(eIDL, eLog, "", e, ""))
	} else if ps.myTrackers[attr] != nil {
		ps.tablesLock.Lock()
		ps.myTrackers[attr].leader = true
		ps.tablesLock.Unlock()
		ps.myTrackers[attr].newEventToCheck(NewEventLedger(eIDL, eLog, "", e, ""))
	} else {
		ps.tablesLock.Lock()
		ps.myTrackers[attr] = NewTracker(true, attr, ps, ps.timeToCheckDelivery)
		ps.tablesLock.Unlock()
		ps.myTrackers[attr].newEventToCheck(NewEventLedger(eIDL, eLog, "", e, ""))
	}

	for _, addr := range ps.alternativesToRv(attr) {

		if addr == ps.serverAddr {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
		defer cancel()

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		rm := &pb.RecruitTrackerMessage{
			RvID:   attr,
			RvAddr: ps.serverAddr,
		}

		eL := &pb.EventLog{
			RecruitMessage: rm,
			RvID:           attr,
			EventID:        eID,
			Event:          e,
			Log:            eLog,
		}

		client := pb.NewScoutHubClient(conn)
		client.LogToTracker(ctx, eL)
	}
}

// sendAckToTrackers
func (ps *PubSub) sendAckToTrackers(ack *pb.EventAck) {

	if ps.myTrackers[ack.RvID] != nil {
		ps.myTrackers[ack.RvID].addEventAck <- ack
	} else {
		ps.tablesLock.Lock()
		ps.myTrackers[ack.RvID] = NewTracker(true, ack.RvID, ps, ps.timeToCheckDelivery)
		ps.tablesLock.Unlock()
		ps.myTrackers[ack.RvID].addEventAck <- ack
	}

	for _, addr := range ps.alternativesToRv(ack.RvID) {

		if addr == ps.serverAddr {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
		defer cancel()

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		rm := &pb.RecruitTrackerMessage{
			RvID:   ack.RvID,
			RvAddr: ps.serverAddr,
		}

		ack.RecruitMessage = rm

		client := pb.NewScoutHubClient(conn)
		client.AckToTracker(ctx, ack)
	}
}

// AckUp processes an event ackknowledge and if it was the last
// missing ack returns its own acknowledge upstream
func (ps *PubSub) AckUp(ctx context.Context, ack *pb.EventAck) (*pb.Ack, error) {
	fmt.Println("AckUp >> " + ps.serverAddr)

	eID := fmt.Sprintf("%s%d%d%s", ack.EventID.PublisherID, ack.EventID.SessionNumber, ack.EventID.SeqID, ack.RvID)
	isRv, _ := ps.rendezvousSelfCheck(ack.RvID)
	if ps.myEBackTrackers[eID] != nil {

		ps.ackUpLock.Lock()
		if ps.myEBackTrackers[eID] == nil || ps.myEBackTrackers[eID].receivedAcks == ps.myEBackTrackers[eID].expectedAcks {
			return &pb.Ack{State: true, Info: "Event Tracker already closed"}, nil
		}

		ps.myEBackTrackers[eID].eventLog[ack.PeerID] = true
		ps.myEBackTrackers[eID].receivedAcks++

		if ps.myEBackTrackers[eID].receivedAcks == ps.myEBackTrackers[eID].expectedAcks {

			newAck := &AckUp{
				dialAddr: ps.myEBackTrackers[eID].addrToAck,
				eventID:  ack.EventID,
				peerID:   ps.myEBackTrackers[eID].originalDestination,
				rvID:     ack.RvID,
			}
			ps.ackUpLock.Unlock()

			go ps.sendAckUp(newAck)
		} else {
			ps.ackUpLock.Unlock()
		}
	} else if isRv {
		ps.sendAckToTrackers(ack)
	} else {

		ps.ackUpLock.Lock()
		if ps.myETrackers[eID] == nil || ps.myETrackers[eID].receivedAcks == ps.myETrackers[eID].expectedAcks {
			return &pb.Ack{State: true, Info: "Event Tracker already closed"}, nil
		}

		ps.myETrackers[eID].eventLog[ack.PeerID] = true
		ps.myETrackers[eID].receivedAcks++

		if ps.myETrackers[eID].receivedAcks == ps.myETrackers[eID].expectedAcks {
			newAck := &AckUp{
				dialAddr: ps.myETrackers[eID].addrToAck,
				eventID:  ack.EventID,
				peerID:   ps.myETrackers[eID].originalDestination,
				rvID:     ack.RvID,
			}
			ps.ackUpLock.Unlock()

			go ps.sendAckUp(newAck)
		} else {
			ps.ackUpLock.Unlock()
		}
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// AckOp receives confirmation of a Operation and stops its resending from happening
func (ps *PubSub) AckOp(ctx context.Context, ack *pb.Ack) (*pb.Ack, error) {
	fmt.Println("AckOp >> " + ps.serverAddr)

	ps.ackOpLock.Lock()
	if ack.Op == "Publish" {
		if ps.unconfirmedEvents[ack.Info] != nil {
			ps.unconfirmedEvents[ack.Info] = nil
			ps.totalUnconfirmedEvents--
		}

		if ps.totalUnconfirmedEvents == 0 {
			ps.eventTicker.Stop()
			ps.eventTickerState = false
		}
	} else if ack.Op == "Subscribe" {
		if ps.unconfirmedSubs[ack.Info] != nil {
			ps.record.SaveTimeToSub(ps.unconfirmedSubs[ack.Info].started)
			ps.unconfirmedSubs[ack.Info] = nil
			ps.totalUnconfirmedSubs--
		}

		if ps.totalUnconfirmedSubs == 0 {
			ps.subTicker.Stop()
			ps.subTickerState = false
		}
	}
	ps.ackOpLock.Unlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// LogToTracker is the remote call a tracker receives from
// the Rv node with a event log for him to start tracking
func (ps *PubSub) LogToTracker(ctx context.Context, log *pb.EventLog) (*pb.Ack, error) {
	fmt.Println("LogTracker >> " + ps.serverAddr)

	if ps.myTrackers[log.RecruitMessage.RvID] == nil {

		ps.myTrackers[log.RecruitMessage.RvID] = NewTracker(false, log.RecruitMessage.RvID,
			ps, ps.timeToCheckDelivery)

		for _, addr := range ps.alternativesToRv(log.RvID) {

			if addr == ps.serverAddr {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				return &pb.Ack{State: false, Info: ""}, err
			}
			defer conn.Close()

			log.RecruitMessage.RvAddr = ps.serverAddr

			client := pb.NewScoutHubClient(conn)
			resp, err := client.TrackerRefresh(ctx, log.RecruitMessage)
			if err == nil && resp.State {
				break
			}
		}
	}

	eID := fmt.Sprintf("%s%d%d", log.EventID.PublisherID, log.EventID.SessionNumber, log.EventID.SeqID)
	ps.myTrackers[log.RvID].newEventToCheck(NewEventLedger(eID, log.Log, "", log.Event, ""))

	return &pb.Ack{State: true, Info: ""}, nil
}

// AckToTracker is the remote call the Rv node uses to communicate
// received event acknowledges to the tracker
func (ps *PubSub) AckToTracker(ctx context.Context, ack *pb.EventAck) (*pb.Ack, error) {
	fmt.Println("AckToTracker >> " + ps.serverAddr)

	if ps.myTrackers[ack.RecruitMessage.RvID] == nil {

		ps.myTrackers[ack.RecruitMessage.RvID] = NewTracker(false, ack.RecruitMessage.RvID,
			ps, ps.timeToCheckDelivery)

		for _, addr := range ps.alternativesToRv(ack.RecruitMessage.RvID) {

			if addr == ps.serverAddr {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				return &pb.Ack{State: false, Info: ""}, err
			}
			defer conn.Close()

			ack.RecruitMessage.RvAddr = ps.serverAddr

			client := pb.NewScoutHubClient(conn)
			resp, err := client.TrackerRefresh(ctx, ack.RecruitMessage)
			if err == nil && resp.State {
				break
			}
		}
	}

	if ps.myTrackers[ack.RvID] != nil {
		ps.myTrackers[ack.RvID].addEventAck <- ack
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// TrackerRefresh is a rpc that is requested by a new tracker to the rv
// neighbourhood in order to refresh himself with their event ledgers
func (ps *PubSub) TrackerRefresh(ctx context.Context, req *pb.RecruitTrackerMessage) (*pb.Ack, error) {
	fmt.Println("TrackerRefresh >> " + ps.serverAddr)

	if ps.myTrackers[req.RvID] != nil && len(ps.myTrackers[req.RvID].eventStats) > 0 {

		conn, err := grpc.Dial(req.RvAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		for _, l := range ps.myTrackers[req.RvID].eventStats {
			eL := &pb.EventLog{
				RecruitMessage: req,
				RvID:           l.event.RvId,
				EventID:        l.event.EventID,
				Event:          l.event,
				Log:            l.eventLog,
			}

			ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			client := pb.NewScoutHubClient(conn)
			client.LogToTracker(ctx, eL)
		}
	} else {
		return &pb.Ack{State: false, Info: "no tracker"}, nil
	}

	return &pb.Ack{State: true, Info: ""}, nil
}

// resendEvent
func (ps *PubSub) resendEvent(eLog *pb.EventLog) {

	ps.tablesLock.RLock()
	defer ps.tablesLock.RUnlock()

	for p := range eLog.Log {

		dialAddr := ps.currentFilterTable.routes[p].addr
		auxBackups := ps.currentFilterTable.routes[p].backups
		newE := &pb.Event{
			Event:         eLog.Event.Event,
			OriginalRoute: p,
			Backup:        eLog.Event.Backup,
			Predicate:     eLog.Event.Predicate,
			RvId:          eLog.Event.RvId,
			AckAddr:       eLog.Event.AckAddr,
			PubAddr:       eLog.Event.PubAddr,
			EventID:       eLog.Event.EventID,
			BirthTime:     eLog.Event.BirthTime,
			LastHop:       eLog.Event.LastHop,
		}

		if ps.activeRedirect {

			ps.currentFilterTable.redirectLock.Lock()
			ps.nextFilterTable.redirectLock.Lock()

			if ps.currentFilterTable.redirectTable[p][eLog.Event.RvId] != "" {
				go ps.forwardEventDown(dialAddr, newE, ps.currentFilterTable.redirectTable[p][eLog.Event.RvId], auxBackups)
			} else {
				go ps.forwardEventDown(dialAddr, newE, "", auxBackups)
			}

			ps.currentFilterTable.redirectLock.Unlock()
			ps.nextFilterTable.redirectLock.Unlock()

		} else {
			go ps.forwardEventDown(dialAddr, newE, "", auxBackups)
		}
	}
}

// forwardEventUp is called upon receiving the request to keep forward a event
// towards a rendezvous by calling another publish operation towards it
func (ps *PubSub) forwardEventUp(dialAddr string, event *pb.Event) {

	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
	defer cancel()

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	event.LastHop = peer.Encode(ps.ipfsDHT.PeerID())
	ack, err := client.Publish(ctx, event)
	if err != nil || !ack.State {
		alternatives := ps.alternativesToRv(event.RvId)
		for _, addr := range alternatives {

			if addr == ps.serverAddr {
				p, _ := NewPredicate(event.Predicate, ps.maxAttributesPerPredicate)
				ps.iAmRVPublish(p, event, true)
				return
			}

			ctxBackup, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Publish(ctxBackup, event)
			if err == nil && ack.State {
				break
			}
		}
	}
}

// Notify is a remote function called by a external peer to send an Event downstream
func (ps *PubSub) Notify(ctx context.Context, event *pb.Event) (*pb.Ack, error) {
	fmt.Println("Notify >> " + ps.serverAddr)
	ps.record.operationHistory["Notify"]++

	p, err := NewPredicate(event.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	if !event.Backup {
		for _, attr := range p.attributes {
			isRv, _ := ps.rendezvousSelfCheck(attr.name)
			if isRv {
				newAck := &AckUp{dialAddr: event.AckAddr, eventID: event.EventID, peerID: event.OriginalRoute, rvID: event.RvId}
				go ps.sendAckUp(newAck)
				return &pb.Ack{State: true, Info: ""}, nil
			}
		}
	}

	originalDestination := event.OriginalRoute

	eID := fmt.Sprintf("%s%d%d%s", event.EventID.PublisherID, event.EventID.SessionNumber, event.EventID.SeqID, event.RvId)
	if ps.activeReliability && ps.myETrackers[eID] != nil && !event.Backup {
		for node, received := range ps.myETrackers[eID].eventLog {
			if !received {

				ps.tablesLock.RLock()
				dialAddr := ps.currentFilterTable.routes[node].addr
				auxBackups := ps.currentFilterTable.routes[node].backups
				ps.tablesLock.RUnlock()
				newE := &pb.Event{
					Event:         event.Event,
					OriginalRoute: node,
					Backup:        event.Backup,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					AckAddr:       event.AckAddr,
					PubAddr:       event.PubAddr,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				if ps.activeRedirect {
					ps.currentFilterTable.redirectLock.Lock()
					ps.nextFilterTable.redirectLock.Lock()

					if ps.currentFilterTable.redirectTable[node][event.RvId] != "" {
						go ps.forwardEventDown(dialAddr, newE, ps.currentFilterTable.redirectTable[node][event.RvId], auxBackups)
					} else {
						go ps.forwardEventDown(dialAddr, newE, "", auxBackups)
					}

					ps.currentFilterTable.redirectLock.Unlock()
					ps.nextFilterTable.redirectLock.Unlock()
				} else {
					go ps.forwardEventDown(dialAddr, newE, "", auxBackups)
				}
			}
		}

		return &pb.Ack{State: true, Info: ""}, nil
	}

	ackAddr := event.AckAddr
	event.AckAddr = ps.serverAddr
	eL := make(map[string]bool)

	ps.tablesLock.RLock()
	if !event.Backup {

		if ps.myFilters.IsInterested(p) {
			ps.interestingEvents <- event
		}

		for next, route := range ps.currentFilterTable.routes {
			if route.IsInterested(p) {

				newE := &pb.Event{
					Event:         event.Event,
					Backup:        event.Backup,
					OriginalRoute: next,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					AckAddr:       event.AckAddr,
					PubAddr:       event.PubAddr,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				eL[next] = false
				dialAddr := route.addr

				if ps.activeRedirect {
					ps.currentFilterTable.redirectLock.Lock()
					ps.nextFilterTable.redirectLock.Lock()

					if ps.currentFilterTable.redirectTable[next] == nil {
						ps.currentFilterTable.redirectTable[next] = make(map[string]string)
						ps.currentFilterTable.redirectTable[next][event.RvId] = ""
						ps.nextFilterTable.redirectTable[next] = make(map[string]string)
						ps.nextFilterTable.redirectTable[next][event.RvId] = ""

						go ps.forwardEventDown(dialAddr, newE, "", route.backups)
					} else if ps.currentFilterTable.redirectTable[next][event.RvId] == "" {
						go ps.forwardEventDown(dialAddr, newE, "", route.backups)
					} else {
						go ps.forwardEventDown(dialAddr, newE, ps.currentFilterTable.redirectTable[next][event.RvId], route.backups)
					}

					ps.currentFilterTable.redirectLock.Unlock()
					ps.nextFilterTable.redirectLock.Unlock()

				} else {
					go ps.forwardEventDown(dialAddr, newE, "", route.backups)
				}
			}
		}

		if ps.activeReliability {
			if len(eL) > 0 && ps.myETrackers[eID] == nil {
				ps.myETrackers[eID] = NewEventLedger(eID, eL, ackAddr, event, originalDestination)
			} else {
				newAck := &AckUp{dialAddr: ackAddr, eventID: event.EventID, peerID: originalDestination, rvID: event.RvId}
				go ps.sendAckUp(newAck)
			}
		}
	} else {

		ps.upBackLock.RLock()
		if _, ok := ps.myBackupsFilters[event.OriginalRoute]; !ok {
			ps.upBackLock.RUnlock()
			ps.tablesLock.RUnlock()
			return &pb.Ack{State: false, Info: "cannot backup"}, nil
		}

		for next, route := range ps.myBackupsFilters[event.OriginalRoute].routes {
			if route.IsInterested(p) {

				eL[next] = false
				dialAddr := route.addr
				newE := &pb.Event{
					Event:         event.Event,
					OriginalRoute: next,
					Backup:        false,
					Predicate:     event.Predicate,
					RvId:          event.RvId,
					AckAddr:       event.AckAddr,
					PubAddr:       event.PubAddr,
					EventID:       event.EventID,
					BirthTime:     event.BirthTime,
					LastHop:       event.LastHop,
				}

				go ps.forwardEventDown(dialAddr, newE, "", route.backups)
			}
		}
		ps.upBackLock.RUnlock()

		if ps.activeReliability {
			if len(eL) > 0 && ps.myEBackTrackers[eID] == nil {
				ps.myEBackTrackers[eID] = NewEventLedger(eID, eL, ackAddr, event, originalDestination)
			} else {
				newAck := &AckUp{dialAddr: ackAddr, eventID: event.EventID, peerID: originalDestination, rvID: event.RvId}
				go ps.sendAckUp(newAck)
			}
		}
	}
	ps.tablesLock.RUnlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// forwardEventDown is called upon receiving the request to keep forward a event downwards
// until it finds all subscribers by calling a notify operation towards them
func (ps *PubSub) forwardEventDown(dialAddr string, event *pb.Event, redirect string, backups []string) {

	ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
	defer cancel()

	if dialAddr == ps.serverAddr {
		ps.Notify(ctx, event)
		return
	}

	if redirect != "" && ps.tryRedirect(ctx, redirect, event) {
		return
	}

	conn, err := grpc.Dial(dialAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	event.LastHop = peer.Encode(ps.ipfsDHT.PeerID())
	ack, err := client.Notify(ctx, event)
	if err != nil || !ack.State {

		event.Backup = true
		for _, backup := range backups {

			if backup == ps.serverAddr {
				ps.Notify(ctx, event)
				return
			}

			ctxBackup, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
			defer cancel()

			conn, err := grpc.Dial(backup, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("fail to dial: %v", err)
			}
			defer conn.Close()

			client := pb.NewScoutHubClient(conn)
			ack, err := client.Notify(ctxBackup, event)
			if err == nil && ack.State {
				break
			}
		}
	}
}

// tryRedirect tries to use the redirect option to evade unnecessary downstream hops
func (ps *PubSub) tryRedirect(ctx context.Context, redirect string, event *pb.Event) bool {

	conn, err := grpc.Dial(redirect, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	ack, err := client.Notify(ctx, event)
	if err == nil && ack.State {
		return true
	}

	return false
}

// UpdateBackup sends a new filter of the filter table to the backup
func (ps *PubSub) UpdateBackup(ctx context.Context, update *pb.Update) (*pb.Ack, error) {
	fmt.Println("UpdateBackup >> " + ps.serverAddr)

	p, err := NewPredicate(update.Predicate, ps.maxAttributesPerPredicate)
	if err != nil {
		return &pb.Ack{State: false, Info: err.Error()}, err
	}

	var backAddrs []string
	for _, addr := range update.Backups {
		backAddrs = append(backAddrs, addr)
	}

	ps.upBackLock.Lock()
	if _, ok := ps.myBackupsFilters[update.Sender]; !ok {
		ps.myBackupsFilters[update.Sender] = &FilterTable{routes: make(map[string]*RouteStats)}
	}

	if _, ok := ps.myBackupsFilters[update.Sender].routes[update.Route]; !ok {
		ps.myBackupsFilters[update.Sender].routes[update.Route] = NewRouteStats(update.RouteAddr)
	}
	ps.upBackLock.Unlock()

	ps.upBackLock.RLock()
	ps.myBackupsFilters[update.Sender].routes[update.Route].SimpleAddSummarizedFilter(p, backAddrs)
	ps.upBackLock.RUnlock()

	return &pb.Ack{State: true, Info: ""}, nil
}

// updateMyBackups basically sends updates rpcs to its backups
// to update their versions of his filter table
func (ps *PubSub) updateMyBackups(route string, info string, backups map[int32]string) error {

	ps.tablesLock.RLock()
	routeAddr := ps.currentFilterTable.routes[route].addr
	ps.tablesLock.RUnlock()

	for _, addrB := range ps.myBackups {
		ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
		defer cancel()

		conn, err := grpc.Dial(addrB, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		client := pb.NewScoutHubClient(conn)
		update := &pb.Update{
			Sender:    peer.Encode(ps.ipfsDHT.PeerID()),
			Route:     route,
			RouteAddr: routeAddr,
			Predicate: info,
			Backups:   backups,
		}

		ack, err := client.UpdateBackup(ctx, update)
		if err != nil || !ack.State {
			ps.eraseOldFetchNewBackup(addrB)
			return errors.New("failed update")
		}
	}

	return nil
}

// updateRvRegion basically sends updates rpcs
// to the closest nodes to the attribute
func (ps *PubSub) updateRvRegion(route string, info string, rvID string, backups map[int32]string) error {

	ps.tablesLock.RLock()
	routeAddr := ps.currentFilterTable.routes[route].addr
	ps.tablesLock.RUnlock()

	for _, alt := range ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertKey(rvID), ps.faultToleranceFactor) {

		backupAddrs := ps.ipfsDHT.FindLocal(context.Background(), alt).Addrs
		if backupAddrs == nil {
			continue
		}

		altAddr := addrForPubSubServer(backupAddrs, ps.addrOption)

		ctx, cancel := context.WithTimeout(context.Background(), ps.rpcTimeout)
		defer cancel()

		conn, err := grpc.Dial(altAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()

		client := pb.NewScoutHubClient(conn)
		update := &pb.Update{
			Sender:    peer.Encode(ps.ipfsDHT.PeerID()),
			Route:     route,
			RouteAddr: routeAddr,
			Predicate: info,
			Backups:   backups,
		}

		client.UpdateBackup(ctx, update)
	}

	return nil
}

// getBackups selects f backup peers for the node,
// which are the ones closer to him by ID
func (ps *PubSub) getBackups() []string {

	var backups []string
	var dialAddr string
	for _, backup := range ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertPeerID(ps.ipfsDHT.PeerID()), ps.faultToleranceFactor) {
		backupAddrs := ps.ipfsDHT.FindLocal(context.Background(), backup).Addrs
		if backupAddrs == nil {
			continue
		}

		dialAddr = addrForPubSubServer(backupAddrs, ps.addrOption)
		backups = append(backups, dialAddr)
	}

	return backups
}

// eraseOldFetchNewBackup rases a old backup and
// recruits and updates another to replace it
func (ps *PubSub) eraseOldFetchNewBackup(oldAddr string) {

	var refIndex int
	for i, backup := range ps.myBackups {
		if backup == oldAddr {
			refIndex = i
		}
	}

	candidate := ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertPeerID(ps.ipfsDHT.PeerID()), ps.faultToleranceFactor+1)
	if len(candidate) != ps.faultToleranceFactor+1 {
		return
	}

	backupAddrs := ps.ipfsDHT.FindLocal(context.Background(), candidate[ps.faultToleranceFactor]).Addrs
	if backupAddrs == nil {
		return
	}
	newAddr := addrForPubSubServer(backupAddrs, ps.addrOption)
	ps.myBackups[refIndex] = newAddr

	updates, err := ps.filtersForBackupRefresh()
	if err != nil {
		return
	}

	ps.refreshOneBackup(newAddr, updates)
}

// BackupRefresh refreshes the filter table the backup keeps of the peer
func (ps *PubSub) BackupRefresh(stream pb.ScoutHub_BackupRefreshServer) error {
	fmt.Println("BackupRefresh >> " + ps.serverAddr)

	var i = 0
	ps.upBackLock.Lock()
	defer ps.upBackLock.Unlock()

	for {
		update, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.Ack{State: true, Info: ""})
		}
		if err != nil {
			return err
		}
		if i == 0 {
			ps.myBackupsFilters[update.Sender] = nil
		}

		p, err := NewPredicate(update.Predicate, ps.maxAttributesPerPredicate)
		if err != nil {
			return err
		}

		if ps.myBackupsFilters[update.Sender] == nil {
			ps.myBackupsFilters[update.Sender] = &FilterTable{routes: make(map[string]*RouteStats)}
		}

		if _, ok := ps.myBackupsFilters[update.Sender].routes[update.Route]; !ok {
			ps.myBackupsFilters[update.Sender].routes[update.Route] = NewRouteStats(update.RouteAddr)
		}

		ps.myBackupsFilters[update.Sender].routes[update.Route].SimpleAddSummarizedFilter(p, nil)
		i = 1
	}
}

// refreashBackups sends a BackupRefresh
// to all backup nodes
func (ps *PubSub) refreshAllBackups() error {

	updates, err := ps.filtersForBackupRefresh()
	if err != nil {
		return err
	}

	for _, backup := range ps.myBackups {
		err := ps.refreshOneBackup(backup, updates)
		if err != nil {
			return err
		}
	}

	return nil
}

// filtersForBackupRefresh coverts an entire filterTable into a
// sequence of updates for easier delivery via gRPC
func (ps *PubSub) filtersForBackupRefresh() ([]*pb.Update, error) {

	var updates []*pb.Update
	ps.tablesLock.RLock()
	for route, routeS := range ps.currentFilterTable.routes {

		routeAddr := routeS.addr
		for _, filters := range routeS.filters {
			for _, filter := range filters {
				u := &pb.Update{
					Sender:    peer.Encode(ps.ipfsDHT.PeerID()),
					Route:     route,
					RouteAddr: routeAddr,
					Predicate: filter.ToString()}
				updates = append(updates, u)
			}
		}
	}
	ps.tablesLock.RUnlock()

	return updates, nil
}

// refreshOneBackup refreshes one of the nodes backup
func (ps *PubSub) refreshOneBackup(backup string, updates []*pb.Update) error {

	ctx, cancel := context.WithTimeout(context.Background(), 2*ps.rpcTimeout)
	defer cancel()

	conn, err := grpc.Dial(backup, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewScoutHubClient(conn)
	stream, err := client.BackupRefresh(ctx)
	if err != nil {
		return err
	}

	for _, up := range updates {
		if err := stream.Send(up); err != nil {
			return err
		}
	}

	ack, err := stream.CloseAndRecv()
	if err != nil || !ack.State {
		return err
	}

	return nil
}

// rendezvousSelfCheck evaluates if the peer is the rendezvous node
// and if not it returns the peerID of the next subscribing hop
func (ps *PubSub) rendezvousSelfCheck(rvID string) (bool, string) {

	selfID := ps.ipfsDHT.PeerID()
	closestIDs := ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertKey(rvID), ps.faultToleranceFactor+1)

	for _, closestID := range closestIDs {
		addr := addrForPubSubServer(ps.ipfsDHT.FindLocal(context.Background(), closestID).Addrs, ps.addrOption)
		if kb.Closer(selfID, closestID, rvID) {
			return true, ""
		} else if addr != "" {
			return false, addr
		}
	}

	return true, ""
}

// alternativesToRv checks for alternative
// ways to reach the rendevous node
func (ps *PubSub) alternativesToRv(rvID string) []string {

	var validAlt []string
	selfID := ps.ipfsDHT.PeerID()
	closestIDs := ps.ipfsDHT.RoutingTable().NearestPeers(kb.ConvertKey(rvID), ps.faultToleranceFactor)

	for _, ID := range closestIDs {
		if !kb.Closer(selfID, ID, rvID) {
			attrAddrs := ps.ipfsDHT.FindLocal(context.Background(), ID).Addrs
			if attrAddrs != nil {
				validAlt = append(validAlt, addrForPubSubServer(attrAddrs, ps.addrOption))
			}
		} else {
			validAlt = append(validAlt, ps.serverAddr)
			break
		}
	}

	return validAlt
}

// closerAttrRvToSelf returns the closest ID node from all the Rv nodes
// of each of a subscription predicate attributes, so that the subscriber
// will send the subscription on the minimal number of hops
func (ps *PubSub) closerAttrRvToSelf(p *Predicate) (peer.ID, string, error) {

	marshalSelf, err := ps.ipfsDHT.Host().ID().MarshalBinary()
	if err != nil {
		return "", "", err
	}

	selfKey := key.XORKeySpace.Key(marshalSelf)
	var minAttr string
	var minID peer.ID
	var minDist *big.Int = nil

	for _, attr := range p.attributes {
		candidateID := peer.ID(kb.ConvertKey(attr.name))
		aux, err := candidateID.MarshalBinary()
		if err != nil {
			return "", "", err
		}

		candidateDist := key.XORKeySpace.Distance(selfKey, key.XORKeySpace.Key(aux))
		if minDist == nil || candidateDist.Cmp(minDist) == -1 {
			minAttr = attr.name
			minID = candidateID
			minDist = candidateDist
		}
	}

	return minID, minAttr, nil
}

// TerminateService closes the PubSub service
func (ps *PubSub) TerminateService() {
	fmt.Println("Terminate: " + ps.serverAddr)
	ps.terminate <- "end"
	ps.server.Stop()
}

// processLopp processes async operations and proceeds
// to execute cyclical functions of refreshing
func (ps *PubSub) processLoop() {
	for {
		select {
		case pid := <-ps.interestingEvents:
			ps.record.SaveReceivedEvent(pid.EventID.PublisherID, pid.BirthTime, pid.Event)
			fmt.Printf("Received Event at: %s\n", ps.serverAddr)
			fmt.Println(">> " + pid.Event)
		case <-ps.eventTicker.C:
			for _, e := range ps.unconfirmedEvents {
				if e != nil && e.aged {
					ps.forwardEventUp(e.dialAddr, e.event)
				} else if e != nil {
					e.aged = true
				}
			}
		case <-ps.subTicker.C:
			for _, sub := range ps.unconfirmedSubs {
				if sub != nil && sub.aged {
					ps.forwardSub(sub.dialAddr, sub.sub)
				} else if sub != nil {
					sub.aged = true
				}
			}
		case <-ps.heartbeatTicker.C:
			for _, filters := range ps.myFilters.filters {
				for _, filter := range filters {
					ps.MySubscribe(filter.ToString())
				}
			}
		case <-ps.refreshTicker.C:
			ps.tablesLock.Lock()
			ps.currentFilterTable = ps.nextFilterTable
			ps.nextFilterTable = NewFilterTable(ps.ipfsDHT, ps.addrOption)
			ps.tablesLock.Unlock()
			ps.refreshAllBackups()
			ps.lives++
		case <-ps.terminate:
			return
		}
	}
}

// ++++++++++++++++++++++ Metrics Fetching plus Testing Functions ++++++++++++++++++++++

// ReturnEventStats returns the time
// it took to receive each event
func (ps *PubSub) ReturnEventStats() []int {

	return ps.record.EventStats()
}

// ReturnSubsStats returns the time it took to receive
// confirmation of subscription completion
func (ps *PubSub) ReturnSubStats() []int {

	return ps.record.SubStats()
}

// ReturnCorrectnessStats returns the number of events missing and duplicated
func (ps *PubSub) ReturnCorrectnessStats(expected []string) (int, int) {

	return ps.record.CorrectnessStats(expected)
}

// ReturnOpStats returns the number of times a operation was executed
func (ps *PubSub) ReturnOpStats(opName string) int {

	return ps.record.operationHistory[opName]
}

// SetHasOldPeer only goal is to set peer as old in testing scenario
func (ps *PubSub) SetHasOldPeer() {

	ps.lives = 100
}
