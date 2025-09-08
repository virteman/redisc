package redisc

import "sync"

var (
	poolStrings = sync.Pool{
		New: func() any {
			return make([]string, 0, 8)
		},
	}
	poolSliceStrings = sync.Pool{
		New: func() any {
			return make([][]string, 0, 8)
		},
	}
	poolInts = sync.Pool{
		New: func() any {
			return make([]int, 0, 8)
		},
	}
	poolBatches = sync.Pool{New: func() any { return make([]batch, 0, 8) }}
)

func requireBatches() []batch {
	return poolBatches.Get().([]batch)
}

func releaseBatches(i []batch) {
	for k, v := range i {
		if cap(v.cmds) <= 16 {
			v.cmds = v.cmds[:0]
			poolInts.Put(v.cmds)
		}
		i[k].conn = nil
		i[k].cmds = nil
	}

	if cap(i) <= 16 {
		i = i[:0]
		poolBatches.Put(i)
	}
}

func requireInts() []int {
	return poolInts.Get().([]int)
}

func releaseInts(i []int) {
	if cap(i) <= 16 {
		i = i[:0]
		poolInts.Put(i)
	}
}

func requireStrings() []string {
	return poolStrings.Get().([]string)
}

func releaseStrings(i []string) {
	if cap(i) <= 16 {
		i = i[:0]
		poolStrings.Put(i)
	}
}

func requireSliceStrings() [][]string {
	return poolSliceStrings.Get().([][]string)
}

func releaseSliceStrings(i [][]string) {

	for k, v := range i {
		releaseStrings(v)
		i[k] = nil
	}

	if cap(i) <= 16 {
		i = i[:0]
		poolSliceStrings.Put(i)
	}
}
