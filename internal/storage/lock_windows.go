//go:build windows

package storage

import (
	"os"
	"syscall"
	"unsafe"
)

var lockFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")

func (s *Store) Lock() (func(), error) {
	f, e := os.OpenFile(s.Dir+string(os.PathSeparator)+"engine.lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	var ov syscall.Overlapped
	r, _, e := lockFileEx.Call(f.Fd(), 3, 0, 1, 0, uintptr(unsafe.Pointer(&ov)))
	if r == 0 {
		f.Close()
		return nil, e
	}
	return func() { f.Close() }, nil
}
