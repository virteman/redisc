package redisc

import (
	"sort"
	"strings"
)

// Slot returns the hash slot for the key.
func Slot(key string) int {
	if start := strings.Index(key, "{"); start >= 0 {
		if end := strings.Index(key[start+1:], "}"); end > 0 { // if end == 0, then it's {}, so we ignore it
			end += start + 1
			key = key[start+1 : end]
		}
	}
	return int(crc16(key) % HashSlots)
}

// SplitBySlot takes a list of keys and returns a list of list of keys,
// grouped by identical cluster slot. For example:
//
//	bySlot := SplitBySlot("k1", "k2", "k3")
//	for _, keys := range bySlot {
//	  // keys is a list of keys that belong to the same slot
//	}
func SplitBySlot(keys []string) (groups [][]string) {

	slots := requireInts()
	defer func() {
		releaseInts(slots)
	}()

	groups = requireSliceStrings()

Next:
	for _, k := range keys {
		slot := Slot(k)

		for i, s := range slots {
			if s == slot {
				groups[i] = append(groups[i], k)
				continue Next
			}
		}

		slots = append(slots, slot)
		groups = append(groups, append(requireStrings(), k))
	}

	sort.Slice(groups, func(i, j int) bool {
		if slots[i] < slots[j] {
			slots[i], slots[j] = slots[j], slots[i]
			return true
		}
		return false
	})

	return groups
}

func SplitByNode(c *Cluster, keys []string) [][]string {
	groups := SplitBySlot(keys)
	defer releaseSliceStrings(groups)

	return SplitByNodeWithSlot(c, groups)
}

func SplitByNodeWithSlot(c *Cluster, groups [][]string) [][]string {
	nodes := requireSliceStrings()
	m := make(map[string][]string)

	for _, group := range groups {
		s := Slot(group[0])

		c.mu.RLock()
		mapping := c.mapping[s]
		c.mu.RUnlock()

		if len(mapping) > 0 {
			m[mapping[0]] = append(m[mapping[0]], group...)
		} else {
			nodes = append(nodes, group)
		}
	}

	for _, v := range m {
		nodes = append(nodes, v)
	}

	return nodes
}
