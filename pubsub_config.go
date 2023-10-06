package scoutsubs

import "time"

type SetupPubSub struct {

	// Time resend operation if not acknowledge
	OpResendRate time.Duration

	// Number of backups, localized tolerable faults
	FaultToleranceFactor int

	// How many parallel operations of each type can be supported
	ConcurrentProcessingFactor int

	// Maximum allowed number of attributes per predicate
	MaxAttributesPerPredicate int

	// Frequency in which a subscriber needs to resub
	SubRefreshRateMin time.Duration

	// Time the publisher waits for rv ack until it resends event
	TimeToCheckDelivery time.Duration

	// True to activate redirect mechanism
	RedirectMechanism bool

	// True to activate the tracking mechanism and operation acknowledgement
	ReliableMechanisms bool

	// Should be true if we are running in production or emulated testbed
	ProductionReady bool

	// timeout of each rpc
	RPCTimeout time.Duration
}

func DefaultConfig(production bool) *SetupPubSub {

	cfg := &SetupPubSub{
		OpResendRate:               10 * time.Second,
		FaultToleranceFactor:       2,
		ConcurrentProcessingFactor: 50,
		MaxAttributesPerPredicate:  5,
		SubRefreshRateMin:          15 * time.Minute,
		TimeToCheckDelivery:        30 * time.Second,
		RPCTimeout:                 10 * time.Second,
		RedirectMechanism:          true,
		ReliableMechanisms:         true,
		ProductionReady:            production,
	}

	return cfg
}
