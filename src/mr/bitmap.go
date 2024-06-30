package mr

// 一个简单的 bitmap 实现
// 直接用数组的每一位来表示一位，而不是一个字节的一位
// todo: 优化 bitmap 实现，压缩空间
type bitmap struct {
	bits  []bool //  用 []byte 来存储具体的位
	count uint   //  bitmap 中为 1 的位数
	size  uint   //  bitmap 的 size，总位数即 size*8，从0 - size*8-1
}

// 创建一个 位数为 size 的 bitmap
func NewBitMap(size uint) *bitmap {
	return &bitmap{
		size: size,
		bits: make([]bool, size),
	}
}

// 把 pos 位置置 1，重复对同一个pos使用 / pos 越界无效
func (b *bitmap) set(pos uint) {
	if pos >= b.size {
		return
	}
	if !b.bits[pos] {
		b.count++
		b.bits[pos] = true
	}
}

// 判断 pos 位是否为 1
func (b *bitmap) get(pos uint) bool {
	if pos >= b.size {
		return false
	}
	return b.bits[pos]
}

// bitmap 所有位置 0
func (b *bitmap) clear() {
	for i := 0; i < int(b.size); i++ {
		b.bits[i] = false
	}
	b.count = 0
}

// 是否所有位都为1
func (b *bitmap) isAllSet() bool {
	return b.count == b.size
}

// 查找 bitmap 中，在 pos 及 pos 之后，第一个为0的位置
// 如果到了 bitmap 末尾，再从 0 开始找
// 若全为1，则返回 -1
func (b *bitmap) findFirstZeroAfter(pos int) int {
	if b.isAllSet() {
		return -1
	}
	//往后找
	for i := pos; i < int(b.size); i++ {
		if !b.bits[i] {
			return i
		}
	}
	//再从头开始找
	for i := 0; i < pos; i++ {
		if !b.bits[i] {
			return i
		}
	}

	//显然是 isAllSet/ 上面的for 有问题才会到这里
	panic("[findFirstZero] has some problems")
	return -1
}
