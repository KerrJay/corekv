package utils

import (
	"bytes"
	"sync"
	"unsafe"
)

const (
	defaultMaxLevel = 20
)

type SkipList struct {
	maxLevel   int          //sl的最大高度
	lock       sync.RWMutex //读写锁，用来实现并发安全的sl
	currHeight int32        //sl当前的最大高度
	headOffset uint32       //头结点在arena当中的偏移量
	arena      *Arena
}

func NewSkipList(arenaSize int64) *SkipList {
	arena := &Arena{
		n:   1,
		buf: make([]byte, arenaSize),
	}
	head := newElement(arena, nil, ValueStruct{}, defaultMaxLevel)
	headOffset := arena.getElementOffset(head)

	return &SkipList{
		maxLevel:   defaultMaxLevel,
		currHeight: 1,
		headOffset: headOffset,
		arena:      arena,
	}
}

func newElement(arena *Arena, key []byte, v ValueStruct, height int) *Element {
	// 注意：先写node，再写key，在写value，然后将key/value的地址写入node的属性中
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	valoffset := arena.putVal(v)
	valueSize := v.EncodedSize()
	value := encodeValue(valoffset, valueSize)

	//NOTE: 通过内存地址获取到该地址上的对象
	ele := (*Element)(unsafe.Pointer(&arena.buf[nodeOffset]))
	ele.keyOffset = keyOffset
	ele.keySize = uint16(len(key))
	ele.value = value
	ele.height = uint16(height)
	ele.score = calcScore(key)

	return ele
}

// 用来对value值进行编解码
// value = valueSize | valueOffset
func encodeValue(valOffset uint32, valSize uint32) uint64 {

	return uint64(valOffset)<<32 | uint64(valSize)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valSize = uint32(value)
	valOffset = uint32(value >> 32)
	return
}

type Element struct {
	score float64 //加快查找，只在内存中生效，因此不需要持久化
	value uint64  //将value的off和size组装成一个uint64，实现原子化的操作

	keyOffset uint32
	keySize   uint16

	height uint16

	levels [defaultMaxLevel]uint32 //这里先按照最大高度声明，往arena中放置的时候，会计算实际高度和内存消耗
}

func (e *Element) key(arena *Arena) []byte {
	return arena.buf[e.keyOffset : e.keyOffset+uint32(e.keySize)]
}

// 返回的是内存占用大小
func (list *SkipList) Size() int64 {
	return list.arena.Size()
}

func (list *SkipList) Add(data *Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()

	header := list.arena.getElement(list.headOffset)

}

func (list *SkipList) Search(key []byte) (e *Entry) {

}

func (list *SkipList) Close() error {
	return nil
}

func calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.key(list.arena))
	}

	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	maxLevel := list.maxLevel

	var i = 1
	for i = 1; i < maxLevel; i++ {
		if RandN(1000)%2 == 0 {
			return i
		}
	}

	return i
}

// 拿到某个节点，在某个高度上的next节点
// 如果该节点已经是该层最后一个节点（该节点的level[height]将是0），会返回nil
// 注意:根据内存地址计算
func (list *SkipList) getNext(e *Element, height int) *Element {
	return list.arena.getElement(e.getNextOffset(height))
}

type SkipListIter struct {
	list *SkipList
	elem *Element //iterator当前持有的节点
	lock sync.RWMutex
}

func (list *SkipList) NewSkipListIterator() Iterator {
	return &SkipListIter{
		list: list,
	}
}

func (iter *SkipListIter) Next() {
	AssertTrue(iter.Valid())
	iter.elem = iter.list.getNext(iter.elem, 0) //只在最底层遍历就行了
}

func (iter *SkipListIter) Valid() bool {
	return iter.elem != nil
}
func (iter *SkipListIter) Rewind() {
	head := iter.list.arena.getElement(iter.list.headOffset)
	iter.elem = iter.list.getNext(head, 0)
}

func (iter *SkipListIter) Item() Item {
	vo, vs := decodeValue(iter.elem.value)
	return &Entry{
		Key:       iter.list.arena.getKey(iter.elem.keyOffset, iter.elem.keySize),
		Value:     iter.list.arena.getVal(vo, vs).Value,
		ExpiresAt: iter.list.arena.getVal(vo, vs).ExpiresAt,
	}
}
func (iter *SkipListIter) Close() error {
	return nil
}

func (iter *SkipListIter) Seek(key []byte) {
}
