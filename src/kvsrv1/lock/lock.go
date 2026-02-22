package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	lockKey string
	lockVal string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:      ck,
		lockKey: l,
		lockVal: kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.lockKey)
		if err == rpc.OK && val == lk.lockVal {
			return
		}

		if err == rpc.ErrNoKey || val == "" {
			if err := lk.ck.Put(lk.lockKey, lk.lockVal, ver); err == rpc.OK {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey || val != lk.lockVal {
			return
		}

		if lk.ck.Put(lk.lockKey, "", ver) == rpc.OK {
			return
		}
	}
}
