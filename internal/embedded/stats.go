package embedded

import (
	"strings"

	"mmw-agent/internal/collector"

	"github.com/xtls/xray-core/features/stats"
)

// CollectStats reads traffic counters from the embedded stats.Manager
// and returns them in the same format as the HTTP metrics collector.
// Counters are reset after reading (Set(0)).
func (e *EmbeddedXray) CollectStats() *collector.XrayStats {
	e.mu.RLock()
	sm := e.statsManager
	e.mu.RUnlock()

	if sm == nil {
		return nil
	}

	result := &collector.XrayStats{
		Inbound:  make(map[string]collector.TrafficData),
		Outbound: make(map[string]collector.TrafficData),
		User:     make(map[string]collector.TrafficData),
	}

	// Counter names follow the pattern:
	//   inbound>>>tag>>>traffic>>>uplink
	//   inbound>>>tag>>>traffic>>>downlink
	//   outbound>>>tag>>>traffic>>>uplink
	//   outbound>>>tag>>>traffic>>>downlink
	//   user>>>email>>>traffic>>>uplink
	//   user>>>email>>>traffic>>>downlink
	//
	// We iterate known patterns by checking both uplink and downlink for each entity.
	// Since stats.Manager doesn't expose a list of all counters,
	// we use the inbound/outbound managers to know which tags exist.

	e.mu.RLock()
	instance := e.instance
	e.mu.RUnlock()
	if instance == nil {
		return result
	}

	collectCounterPair(sm, result.Inbound, "inbound")
	collectCounterPair(sm, result.Outbound, "outbound")
	collectCounterPair(sm, result.User, "user")

	return result
}

// SnapshotUserTraffic returns per-user cumulative traffic using Value() (non-destructive).
func (e *EmbeddedXray) SnapshotUserTraffic() map[string]int64 {
	e.mu.RLock()
	sm := e.statsManager
	e.mu.RUnlock()
	if sm == nil {
		return nil
	}

	type counterLister interface {
		VisitCounters(func(string, stats.Counter) bool)
	}
	lister, ok := sm.(counterLister)
	if !ok {
		return nil
	}

	result := make(map[string]int64)
	lister.VisitCounters(func(name string, c stats.Counter) bool {
		if !strings.HasPrefix(name, "user>>>") {
			return true
		}
		parts := strings.Split(name, ">>>")
		if len(parts) != 4 || parts[2] != "traffic" {
			return true
		}
		email := parts[1]
		result[email] += c.Value()
		return true
	})
	return result
}

func collectCounterPair(sm stats.Manager, dest map[string]collector.TrafficData, category string) {
	// We need to scan all registered counters. Unfortunately stats.Manager
	// doesn't have a ListCounters method. We'll use a different approach:
	// iterate through the known counter interface.
	// For now, we rely on the concrete stats implementation.
	type counterLister interface {
		VisitCounters(func(string, stats.Counter) bool)
	}
	if lister, ok := sm.(counterLister); ok {
		lister.VisitCounters(func(name string, c stats.Counter) bool {
			// Parse "category>>>name>>>traffic>>>direction"
			if !strings.HasPrefix(name, category+">>>") {
				return true
			}
			parts := strings.Split(name, ">>>")
			if len(parts) != 4 || parts[2] != "traffic" {
				return true
			}
			tag := parts[1]
			direction := parts[3]
			value := c.Set(0) // read and reset

			td := dest[tag]
			switch direction {
			case "uplink":
				td.Uplink += value
			case "downlink":
				td.Downlink += value
			}
			dest[tag] = td
			return true
		})
	}
}
