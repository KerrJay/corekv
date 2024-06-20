package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

type Arena struct {
	n   uint32 //offset
	buf []byte
}

const MaxNodeSize = int(unsafe.Sizeof(Element{}))

const offsetSize = int(unsafe.Sizeof(uint32(0)))
const nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

func newArena(n int64) *Arena {
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	//implement me here！！！
	offset := atomic.AddUint32(&s.n, sz)

	buflen := len(s.buf)
	if buflen-int(offset) < MaxNodeSize {
		groupBy := uint32(buflen)
		if groupBy < 1<<30 {
			groupBy = 1 << 30
		}

		if sz > groupBy {
			groupBy = sz
		}

		newBuf := make([]byte, uint32(buflen)+groupBy)
		AssertTrue(buflen == copy(newBuf, s.buf))
		s.buf = newBuf
	}

	return offset - sz
}

// 在arena里开辟一块空间，用以存放sl中的节点
// 返回值为在arena中的offset
func (s *Arena) putNode(height int) uint32 {
	//implement me here！！！
	// 这里的 node 要保存 value 、key 和 next 指针值
	// 所以要计算清楚需要申请多大的内存空间
	unusedSize := offsetSize * (defaultMaxLevel - height)
	offset := MaxNodeSize - unusedSize
	allocate := s.allocate(uint32(offset))

	//TODO: 内存对齐
	return allocate
}

func (s *Arena) putVal(v ValueStruct) uint32 {
	//implement me here！！！
	//将 Value 值存储到 arena 当中
	// 并且将指针返回，返回的指针值应被存储在 Node 节点中
	size := v.EncodedSize()
	valueOffset := s.allocate(size)
	v.EncodeValue(s.buf[valueOffset : valueOffset+size])

	return valueOffset
}

func (s *Arena) putKey(key []byte) uint32 {
	//implement me here！！！
	//将  Key 值存储到 arena 当中
	// 并且将指针返回，返回的指针值应被存储在 Node 节点中
	len := len(key)
	keyOffset := s.allocate(uint32(len))
	buf := s.buf[keyOffset : keyOffset+uint32(len)]
	copy(buf, key)
	return keyOffset

}

func (s *Arena) getElement(offset uint32) *Element {
	if offset == 0 {
		return nil
	}
	return (*Element)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (v ValueStruct) {
	valueBuf := s.buf[offset : offset+size]
	v.DecodeValue(valueBuf)
	return
}

// 用element在内存中的地址 - arena首字节的内存地址，得到在arena中的偏移量
func (s *Arena) getElementOffset(nd *Element) uint32 {
	//implement me here！！！
	u := uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0]))
	return uint32(u)
}

func (e *Element) getNextOffset(h int) uint32 {
	//implement me here！！！
	// 这个方法用来计算节点在h 层数下的 next 节点
	return atomic.LoadUint32(&e.levels[h])
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
