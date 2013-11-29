package protobuf

import (
	"fmt"
)

type Allocator interface {
	New(size int) (buffer []byte)
	Dispose(buffer []byte)
}

type BufferPool interface {
	Allocator
	Close()
}

type heapAllocator struct{}

func NewAllocatorHeap() Allocator {
	return &heapAllocator{}
}

func (this *heapAllocator) New(size int) (buffer []byte) {
	return make([]byte, size)
}

func (this *heapAllocator) Dispose(buffer []byte) {
	// let the garbage collector deal with it...
}

type fixedPool struct {
	allocator Allocator
	size      int
	free      [][]byte
}

func NewBufferPoolFixed(allocator Allocator, size, capacity int) BufferPool {
	return &fixedPool{allocator, size, make([][]byte, 0, capacity)}
}

func (this *fixedPool) New(size int) (buffer []byte) {
	if size != this.size {
		panic(fmt.Errorf("invalid size (%v)", size))
	}
	if len(this.free) > 0 {
		buffer = this.free[len(this.free)-1]
		this.free = this.free[:len(this.free)-1]
	} else {
		buffer = this.allocator.New(size)
	}
	return
}

func (this *fixedPool) Dispose(buffer []byte) {
	if len(this.free) == cap(this.free) {
		this.allocator.Dispose(buffer)
		return
	}
	this.free = append(this.free, buffer)
}

func (this *fixedPool) Close() {
	count := len(this.free)
	for k := 0; k < count; k++ {
		buffer := this.free[len(this.free)-1]
		this.free = this.free[:len(this.free)-1]
		this.allocator.Dispose(buffer)
	}
	if len(this.free) > 0 {
		panic("internal error")
	}
}

type singlePool struct {
	allocator Allocator
	cached    []byte
}

func NewBufferPoolSingle(allocator Allocator) BufferPool {
	return &singlePool{allocator, nil}
}

func (this *singlePool) New(size int) (buffer []byte) {
	if cap(this.cached) >= size {
		buffer = this.cached[:size]
		this.cached = nil
		return
	}
	if this.cached != nil {
		this.allocator.Dispose(this.cached)
		this.cached = nil
	}
	buffer = this.allocator.New(size)
	return
}

func (this *singlePool) Dispose(buffer []byte) {
	if this.cached == nil {
		this.cached = buffer[:cap(buffer)]
		return
	}
	if cap(this.cached) >= cap(buffer) {
		this.allocator.Dispose(buffer)
		return
	}
	this.allocator.Dispose(this.cached)
	this.cached = buffer
}

func (this *singlePool) Close() {
	if this.cached != nil {
		this.allocator.Dispose(this.cached)
		this.cached = nil
	}
}
