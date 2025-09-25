package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	id       string
	lockKey  string
	isLocked bool
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.id = kvtest.RandValue(8)
	lk.lockKey = l
	lk.isLocked = false
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// 1. 读取当前锁状态
		value, version, err := lk.ck.Get(lk.lockKey)

		// 2. 判断是否可以获取锁
		canAcquire := err == rpc.ErrNoKey || value == "free" || value == lk.id

		if canAcquire {
			// 3. 尝试原子获取锁
			putErr := lk.ck.Put(lk.lockKey, lk.id, version)
			if putErr == rpc.OK {
				lk.isLocked = true
				return
			}
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func (lk *Lock) Release() {
	if !lk.isLocked {
		return
	}

	for {
		// 1. 读取当前锁状态
		value, version, err := lk.ck.Get(lk.lockKey)
		if err != rpc.OK && err != rpc.ErrNoKey {
			continue // 网络错误重试
		}

		// 2. 检查锁是否被自己持有
		if value != lk.id {
			lk.isLocked = false // 锁已经不是自己的了
			return
		}

		// 3. 原子释放锁
		err = lk.ck.Put(lk.lockKey, "free", version)
		if err == rpc.OK {
			lk.isLocked = false
			return
		}
	}
}
