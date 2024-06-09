package utils

import (
	"bytes"
	"github.com/hardcore-os/corekv/utils/codec"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	//implement me here!!!
	header := &Element{
		levels: make([]*Element, defaultMaxLevel),
	}
	return &SkipList{
		header:   header,
		maxLevel: defaultMaxLevel - 1,
		// todo: concurrent
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type Element struct {
	// levels[i]存储这个节点第i个level的下个节点
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	//implement me here!!!
	//maxLevel := list.maxLevel
	// NOTE: level用实际头节点包含的level来算
	maxLevel := len(list.header.levels) - 1
	header := list.header
	prev := header
	score := list.calcScore(data.Key)
	preList := make([]*Element, maxLevel+1)

	for i := maxLevel; i >= 0; i-- {

		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			comp := list.compare(score, data.Key, next)
			if comp <= 0 {
				if comp == 0 {
					next.entry.Value = data.Value
				}
				break
			}

			prev = next
		}
		// 记录每一层中需要插入的位点的之前的节点，因为需要插入的值肯定位于prev和next之间
		preList[i] = prev
	}

	// 计算出随机层高
	randLevel := list.randLevel()
	// 创建需要插入的节点
	element := newElement(score, data, randLevel)
	// 每一层分别插入该节点
	for i := 0; i < randLevel; i++ {
		element.levels[i] = preList[i].levels[i]
		preList[i].levels[i] = element
	}

	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	//implement me here!!!
	list.lock.RLock()
	defer list.lock.RUnlock()

	header := list.header
	maxLevel := list.maxLevel
	score := list.calcScore(key)
	prev := header
	for i := maxLevel; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			compare := list.compare(score, key, next)
			if compare <= 0 {
				if compare == 0 {
					return next.entry
				}
				break
			}

			prev = next
		}
	}

	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
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
	//implement me here!!!
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score > next.score {
		return 1
	} else {
		return -1
	}
}

func (list *SkipList) randLevel() int {
	//implement me here!!!
	for i := 1; i < list.maxLevel; i++ {
		//todo: concurrent
		if rand.Intn(2) == 0 {
			return i
		}
	}
	return list.maxLevel
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return 0
}
