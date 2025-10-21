package lock

import (
	"fmt"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name    string
	id      string
	timeOut time.Duration
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:      ck,
		name:    l,
		id:      fmt.Sprintf("%d", rand.Int()),
		timeOut: 10 * time.Second,
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Attempt to acquire the lock by setting the lock key in the kv store
	start := time.Now()
	for time.Since(start) < lk.timeOut {
		value, version, err := lk.ck.Get(lk.name)

		if err == rpc.OK && value == lk.id {
			return
		}

		if err == rpc.ErrNoKey || (err == rpc.OK && value == "unlock") {
			var ok rpc.Err
			if err == rpc.ErrNoKey {
				ok = lk.ck.Put(lk.name, lk.id, 0)
			} else {
				ok = lk.ck.Put(lk.name, lk.id, version)
			}

			if ok == rpc.OK || ok == rpc.ErrMaybe {
				verify, _, _ := lk.ck.Get(lk.name)
				if verify == lk.id {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	start := time.Now()
	for time.Since(start) < lk.timeOut {
		value, version, err := lk.ck.Get(lk.name)
		if err == rpc.OK && value == lk.id {
			ok := lk.ck.Put(lk.name, "unlock", version)
			if ok == rpc.OK || ok == rpc.ErrMaybe || ok == rpc.ErrNoKey {
				return
			}
		} else if err == rpc.ErrNoKey || (err == rpc.OK && value != lk.id) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
