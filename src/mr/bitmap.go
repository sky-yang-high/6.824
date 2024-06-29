package mr

// 一个简单的 bitmap 实现
// todo: 优化，更精细的位数控制，而不是必须 8的整数倍，
// 即把 size 变为 length，后面的函数可能也需要修改
type bitmap struct {
	bits  []byte //  用 []byte 来存储具体的位
	count uint   //  bitmap 中为 1 的位数
	size  uint   //  bitmap 的 size，总位数即 size*8，从0 - size*8-1
}

// 创建一个 位数为 size*8 的 bitmap
func NewBitMap(size uint) *bitmap {
	return &bitmap{
		size: size,
		bits: make([]byte, size), //a additional byte
	}
}

// 把 pos 位置置 1，重复对同一个pos使用 / pos 越界无效
func (b *bitmap) set(pos uint) {
	if pos >= b.size*8 {
		return
	}

	if b.bits[pos/8]&(1<<pos%8) == 0 {
		b.count++
	}
	b.bits[pos/8] |= 1 << (pos % 8)
}

// 判断 pos 位是否为 1
func (b *bitmap) get(pos uint) bool {
	if pos >= b.size*8 {
		return false
	}
	return b.bits[pos/8]&(1<<(pos%8)) != 0
}

// bitmap 所有位置 0
func (b *bitmap) clear() {
	for i := 0; i < int(b.size); i++ {
		b.bits[i] = 0
	}
	b.count = 0
}

// 是否所有位都为1
func (b *bitmap) isAllSet() bool {
	return b.count == b.size*8
}

// 查找 bitmap 中，在 pos 及 pos 之后，第一个为0的位置
// 若全为1，则返回 -1
func (b *bitmap) findFirstZeroAfter(pos int) int {
	if b.isAllSet() {
		return -1
	}
	// 字节位置，位位置
	Bp, bp := (pos/8)%int(b.size), pos%8
	for i := 0; i < int(b.size); i++ {
		p, j := (Bp+i)%int(b.size), 0
		if i == 0 {
			j = bp
		}
		if b.bits[p] == 0xff {
			continue
		}
		for ; j < 8; j++ {
			if b.bits[p]&(1<<j) == 0 {
				return (p*8 + j)
			}
		}
	}
	// 到了这里，说明是和 pos 同一字节的前面的位
	for j := 0; j < bp; j++ {
		if b.bits[Bp]&(1<<j) == 0 {
			return (Bp*8 + j)
		}
	}
	//显然是有问题才会到这里
	panic("the function has some promble")
}
