// +build linux

package protobuf

import (
	"os"
	"syscall"
)

type mappedAllocator struct{}

func NewAllocatorMapped() Allocator {
	return &mappedAllocator{}
}

func (this *mappedAllocator) New(size int) []byte {
	buffer, errno := syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS)
	err := os.NewSyscallError("mmap", errno)
	if err != nil {
		panic(err)
	}
	return buffer
}

func (this *mappedAllocator) Dispose(buffer []byte) {
	err := os.NewSyscallError("munmap", syscall.Munmap(buffer))
	if err != nil {
		panic(err)
	}
}
