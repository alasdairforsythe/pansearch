package pansearch

import (
 "github.com/AlasdairF/BinSearch/Limit16"
 "github.com/AlasdairF/BinSearch/Limit24"
 "github.com/AlasdairF/BinSearch/Limit32"
 "github.com/AlasdairF/BinSearch/Limit40"
 "github.com/AlasdairF/BinSearch/Limit48"
 "github.com/AlasdairF/BinSearch/Limit56"
 "github.com/AlasdairF/BinSearch/Limit64"
 "github.com/AlasdairF/BinSearch/LimitVal8"
 "github.com/AlasdairF/BinSearch/LimitVal16"
 "github.com/AlasdairF/BinSearch/LimitVal24"
 "github.com/AlasdairF/BinSearch/LimitVal32"
 "github.com/AlasdairF/BinSearch/LimitVal40"
 "github.com/AlasdairF/BinSearch/LimitVal48"
 "github.com/AlasdairF/BinSearch/LimitVal56"
 "github.com/AlasdairF/BinSearch/LimitVal64"
 "github.com/AlasdairF/Sort/IntUint64"
 "github.com/AlasdairF/Custom"
 "errors"
 "sync"
)

const (
	FNV_OFFSET = 14695981039346656037
	FNV_PRIME = 1099511628211
	FIBONACCI = 11400714819323198485
	FALSE	  = 4294967295 // the negative value for map1 & map2
)

type filterFunc func([]byte) bool
type double [2]uint64

type Fast struct {
	limit8 [8][]uint64 // where len(word) <= 8
	limit16 [8][][2]uint64
	limit24 [8][][3]uint64
	limit32 [8][][4]uint64
	limit40 [8][][5]uint64
   // The order vars are used only when using AddSorted & Build. Build clears them. They are used for remembering the order that the keys were added in.
	order8 [8][]int
	order16 [8][]int
	order24 [8][]int
	order32 [8][]int
	order40 [8][]int
	count [41]uint32 // Used to convert limit maps to an index value
	total int
   // Bloom filters, hashmaps & lookup tables
	bloom8Top4 [262144]uint64
	bloom8Top2 [262144]uint64
	bloom16 [262144]uint64
	bloom16Top6 [262144]uint64
	bloom16Top4 [262144]uint64
	bloom16Top2 [262144]uint64
	bloom24 [262144]uint64
	bloom32 [262144]uint64
	bloom40 [262144]uint64
	bloomBigSkip [262144]uint64
	map4 [2]map[uint32]uint32
	map8 [4]map[uint64]uint32
	map16 [8]map[double]uint32
	map1 [256]uint32
	map2 [65536]uint32

   // Used for iterating through all of it
	onlimit int
	on8 int
	oncursor int
   }
   
   func (t *Fast) Len() int {
	   return t.total
   }
  
/*
func (t *Fast) find0(key []byte, a uint64, l int) (int, int, bool) {
	var at, min, max int
	var compare uint64
	var cur []uint64
	length := len(key)
	for {
		cur = t.limit8[l]
		max = len(cur) - 1
		min = 0
		for min <= max {
			at = min + ((max - min) / 2)
			if compare = cur[at]; a < compare {
				max = at - 1
				continue
			}
			if a > compare {
				min = at + 1
				continue
			}
			return at + t.count[l], length, true // found
		}
		if length == 1 {
			return 0, length, false
		}
		length--
		a, l = bytes2uint64(key[0:length])
	}
}
*/

func (t *Fast) find0(key []byte, a uint64, l int) (uint32, int, bool) {
	var index uint32
	var exists bool
	switch len(key) {
		case 0:
			return 0, 0, false

		case 1:
			index = t.map1[key[0]]
			if index != FALSE {
				return index, 1, true
			}
			return 0, 0, false

		case 2:
			index = t.map2[a]
			if index != FALSE {
				return index, 2, true
			}
			index = t.map1[key[0]]
			if index != FALSE {
				return index, 1, true
			}
			return 0, 0, false

		case 3:
			if index, exists = t.map4[0][uint32(a)]; exists {
				return index, 3, true
			}
			a >>= 8
			index = t.map2[a]
			if index != FALSE {
				return index, 2, true
			}
			index = t.map1[key[0]]
			if index != FALSE {
				return index, 1, true
			}
			return 0, 0, false

		case 4:
			if index, exists = t.map4[1][uint32(a)]; exists {
				return index, 4, true
			}
			a >>= 8
			if index, exists = t.map4[0][uint32(a)]; exists {
				return index, 3, true
			}
			a >>= 8
			index = t.map2[a]
			if index != FALSE {
				return index, 2, true
			}
			index = t.map1[key[0]]
			if index != FALSE {
				return index, 1, true
			}
			return 0, 0, false

		case 5:
			var bit uint64 = (FIBONACCI * (a)) >> 40
			if t.bloom8Top4[bit>>6]&(1<<(bit&63)) != 0 {
				if index, exists = t.map8[0][a]; exists {
					return index, 5, true
				}
			}
			a >>= 8
			if index, exists = t.map4[1][uint32(a)]; exists {
				return index, 4, true
			}
			a >>= 8
			if index, exists = t.map4[0][uint32(a)]; exists {
				return index, 3, true
			}
			a >>= 8
			index = t.map2[a]
			if index != FALSE {
				return index, 2, true
			}
			index = t.map1[key[0]]
			if index != FALSE {
				return index, 1, true
			}
			return 0, 0, false

		case 6:
			var bit uint64 = (FIBONACCI * (a >> 8)) >> 40
			if t.bloom8Top4[bit>>6]&(1<<(bit&63)) != 0 {
				if index, exists = t.map8[1][a]; exists {
					return index, 6, true
				}
				a >>= 8
				if index, exists = t.map8[0][a]; exists {
					return index, 5, true
				}
				a >>= 8
			} else {
				a >>= 16
			}
			if index, exists = t.map4[1][uint32(a)]; exists {
				return index, 4, true
			}
			a >>= 8
			if index, exists = t.map4[0][uint32(a)]; exists {
				return index, 3, true
			}
			a >>= 8
			index = t.map2[a]
			if index != FALSE {
				return index, 2, true
			}
			index = t.map1[key[0]]
			if index != FALSE {
				return index, 1, true
			}
			return 0, 0, false

		case 7:
			var bit uint64 = (FIBONACCI * (a >> 16)) >> 40
			if t.bloom8Top4[bit>>6]&(1<<(bit&63)) != 0 {
				bit = (FIBONACCI * (a)) >> 40
				if t.bloom8Top2[bit>>6]&(1<<(bit&63)) != 0 {
					if index, exists = t.map8[2][a]; exists {
						return index, 7, true
					}
				}
				a >>= 8
				if index, exists = t.map8[1][a]; exists {
					return index, 6, true
				}
				a >>= 8
				if index, exists = t.map8[0][a]; exists {
					return index, 5, true
				}
				a >>= 8
			} else {
				a >>= 24
			}
			if index, exists = t.map4[1][uint32(a)]; exists {
				return index, 4, true
			}
			a >>= 8
			if index, exists = t.map4[0][uint32(a)]; exists {
				return index, 3, true
			}
			a >>= 8
			index = t.map2[a]
			if index != FALSE {
				return index, 2, true
			}
			index = t.map1[key[0]]
			if index != FALSE {
				return index, 1, true
			}
			return 0, 0, false

		case 8:
			var bit uint64 = (FIBONACCI * (a >> 24)) >> 40
			if t.bloom8Top4[bit>>6]&(1<<(bit&63)) != 0 {
				bit = (FIBONACCI * (a >> 8)) >> 40
				if t.bloom8Top2[bit>>6]&(1<<(bit&63)) != 0 {
					if index, exists = t.map8[3][a]; exists {
						return index, 8, true
					}
					a >>= 8
					if index, exists = t.map8[2][a]; exists {
						return index, 7, true
					}
					a >>= 8
				} else {
					a >>= 16
				}
				if index, exists = t.map8[1][a]; exists {
					return index, 6, true
				}
				a >>= 8
				if index, exists = t.map8[0][a]; exists {
					return index, 5, true
				}
				a >>= 8
			} else {
				a >>= 32
			}
			if index, exists = t.map4[1][uint32(a)]; exists {
				return index, 4, true
			}
			a >>= 8
			if index, exists = t.map4[0][uint32(a)]; exists {
				return index, 3, true
			}
			a >>= 8
			index = t.map2[a]
			if index != FALSE {
				return index, 2, true
			}
			index = t.map1[key[0]]
			if index != FALSE {
				return index, 1, true
			}
			return 0, 0, false
	}
	return 0, 0, false
}

func (t *Fast) find1(key []byte, ab double, l int) (uint32, int, bool) {
	var index uint32
	var exists bool

	if l > 1 {
		var bit uint64 = ((((FNV_OFFSET ^ ((FIBONACCI * (ab[1] >> ((uint64(l) - 2) * 8))))) * FNV_PRIME) ^ ab[0]) * FNV_PRIME) >> 40
		if t.bloom16Top6[bit>>6]&(1<<(bit&63)) == 0 {
			ab[1] >>= 8 * uint64(l - 1)
			l = 1
		} else if l > 3 {
			bit = ((((FNV_OFFSET ^ ((FIBONACCI * (ab[1] >> ((uint64(l) - 4) * 8))))) * FNV_PRIME) ^ ab[0]) * FNV_PRIME) >> 40
			if t.bloom16Top4[bit>>6]&(1<<(bit&63)) == 0 {
				ab[1] >>= 8 * uint64(l - 3)
				l = 3
			} else if l > 5 {
				bit = ((((FNV_OFFSET ^ ((FIBONACCI * (ab[1] >> ((uint64(l) - 6) * 8))))) * FNV_PRIME) ^ ab[0]) * FNV_PRIME) >> 40
				if t.bloom16Top2[bit>>6]&(1<<(bit&63)) == 0 {
					ab[1] >>= 8 * uint64(l - 5)
					l = 5
				}
			}
		}
	}
	for {
		if index, exists = t.map16[l][ab]; exists {
			return index, 9 + l, true
		}
		if l == 0 {
			return 0, 8, false
		}
		l--
		ab[1] >>= 8
	}
}

/*
func (t *Fast) find1(key []byte, a, b uint64, l int) (int, int, bool) {
	var at, min, max int
	var compare uint64
	var cur [][2]uint64
	length := len(key)
	for {
		cur = t.limit16[l]
		max = len(cur) - 1
		min = 0
		for min <= max {
			at = min + ((max - min) / 2)
			if compare = cur[at][0]; a < compare {
				max = at - 1
				continue
			}
			if a > compare {
				min = at + 1
				continue
			}
			if compare = cur[at][1]; b < compare {
				max = at - 1
				continue
			}
			if b > compare {
				min = at + 1
				continue
			}
			return at + t.count[l + 8], length, true // found
		}
		if length == 9 {
			return 0, length, false
		}
		length--
		b, l = bytes2uint64(key[8:length])
	}
}
*/

func (t *Fast) find2(key []byte, a, b, c uint64, l int) (uint32, int, bool) {
	var at, min, max int
	var compare uint64
	var cur [][3]uint64
	length := len(key)
	for {
		cur = t.limit24[l]
		max = len(cur) - 1
		min = 0
		for min <= max {
			at = min + ((max - min) / 2)
			if compare = cur[at][0]; a < compare {
				max = at - 1
				continue
			}
			if a > compare {
				min = at + 1
				continue
			}
			if compare = cur[at][1]; b < compare {
				max = at - 1
				continue
			}
			if b > compare {
				min = at + 1
				continue
			}
			if compare = cur[at][2]; c < compare {
				max = at - 1
				continue
			}
			if c > compare {
				min = at + 1
				continue
			}
			return uint32(at) + t.count[l + 16], length, true // found
		}
		if length == 17 {
			return 0, length, false
		}
		length--
		c, l = bytes2uint64(key[16:length])
	}
}

func (t *Fast) find3(key []byte, a, b, c, d uint64, l int) (uint32, int, bool) {
	var at, min, max int
	var compare uint64
	var cur [][4]uint64
	length := len(key)
	for {
		cur = t.limit32[l]
		max = len(cur) - 1
		min = 0
		for min <= max {
			at = min + ((max - min) / 2)
			if compare = cur[at][0]; a < compare {
				max = at - 1
				continue
			}
			if a > compare {
				min = at + 1
				continue
			}
			if compare = cur[at][1]; b < compare {
				max = at - 1
				continue
			}
			if b > compare {
				min = at + 1
				continue
			}
			if compare = cur[at][2]; c < compare {
				max = at - 1
				continue
			}
			if c > compare {
				min = at + 1
				continue
			}
			if compare = cur[at][3]; d < compare {
				max = at - 1
				continue
			}
			if d > compare {
				min = at + 1
				continue
			}
			return uint32(at) + t.count[l + 24], length, true // found
		}
		if length == 25 {
			return 0, length, false
		}
		length--
		d, l = bytes2uint64(key[24:length])
	}
}

func (t *Fast) find4(key []byte, a, b, c, d, e uint64, l int) (uint32, int, bool) {
	var at, min, max int
	var compare uint64
	var cur [][5]uint64
	length := len(key)
	for {
		cur = t.limit40[l]
		max = len(cur) - 1
		min = 0
		for min <= max {
			at = min + ((max - min) / 2)
			if compare = cur[at][0]; a < compare {
				max = at - 1
				continue
			}
			if a > compare {
				min = at + 1
				continue
			}
			if compare = cur[at][1]; b < compare {
				max = at - 1
				continue
			}
			if b > compare {
				min = at + 1
				continue
			}
			if compare = cur[at][2]; c < compare {
				max = at - 1
				continue
			}
			if c > compare {
				min = at + 1
				continue
			}
			if compare = cur[at][3]; d < compare {
				max = at - 1
				continue
			}
			if d > compare {
				min = at + 1
				continue
			}
			if compare = cur[at][4]; e < compare {
				max = at - 1
				continue
			}
			if e > compare {
				min = at + 1
				continue
			}
			return uint32(at) + t.count[l + 32], length, true // found
		}
		if length == 33 {
			return 0, length, false
		}
		length--
		e, l = bytes2uint64(key[32:length])
	}
}

func (t *Fast) LongestSubstring(key []byte) (uint32, int, bool) {
	
	var a, b, c, d, e uint64
	var l, length int
	var index uint32
	var exists bool

	switch ((len(key) - 1) / 8) {
		case 0:
			if len(key) != 0 {
				a, l = bytes2uint64(key)
				return t.find0(key, a, l)
			}
			return 0, 0, false

		case 1:
			a = (uint64(key[0]) << 56) | (uint64(key[1]) << 48) | (uint64(key[2]) << 40) | (uint64(key[3]) << 32) | (uint64(key[4]) << 24) | (uint64(key[5]) << 16) | (uint64(key[6]) << 8) | uint64(key[7])
			var bit uint64 = ((FNV_OFFSET ^ a) * FNV_PRIME) >> 40
			if t.bloom16[bit>>6]&(1<<(bit&63)) != 0 {
				b, l = bytes2uint64(key[8:])
				if index, length, exists = t.find1(key, double{a, b}, l); exists {
					return index, length, true
				}
			}
			return t.find0(key[0:8], a, 7)

		case 2:
			a = (uint64(key[0]) << 56) | (uint64(key[1]) << 48) | (uint64(key[2]) << 40) | (uint64(key[3]) << 32) | (uint64(key[4]) << 24) | (uint64(key[5]) << 16) | (uint64(key[6]) << 8) | uint64(key[7])
			var hash uint64 = (FNV_OFFSET ^ a) * FNV_PRIME
			var bit uint64 = hash >> 40
			if t.bloomBigSkip[bit>>6]&(1<<(bit&63)) == 0 {
				return t.find0(key[0:8], a, 7)
			}
			b = (uint64(key[8]) << 56) | (uint64(key[9]) << 48) | (uint64(key[10]) << 40) | (uint64(key[11]) << 32) | (uint64(key[12]) << 24) | (uint64(key[13]) << 16) | (uint64(key[14]) << 8) | uint64(key[15])
			bit2 := ((hash ^ b) * FNV_PRIME) >> 40
			if t.bloom24[bit2>>6]&(1<<(bit2&63)) != 0 {
				c, l = bytes2uint64(key[16:])
				if index, length, exists = t.find2(key, a, b, c, l); exists {
					return index, length, true
				}
			}
			if t.bloom16[bit>>6]&(1<<(bit&63)) != 0 {
				if index, length, exists = t.find1(key[0:16], double{a, b}, 7); exists {
					return index, length, true
				}
			}
			return t.find0(key[0:8], a, 7)

		case 3:
			a = (uint64(key[0]) << 56) | (uint64(key[1]) << 48) | (uint64(key[2]) << 40) | (uint64(key[3]) << 32) | (uint64(key[4]) << 24) | (uint64(key[5]) << 16) | (uint64(key[6]) << 8) | uint64(key[7])
			var hash uint64 = (FNV_OFFSET ^ a) * FNV_PRIME
			var bit uint64 = hash >> 40
			if t.bloomBigSkip[bit>>6]&(1<<(bit&63)) == 0 {
				return t.find0(key[0:8], a, 7)
			}
			c = (uint64(key[16]) << 56) | (uint64(key[17]) << 48) | (uint64(key[18]) << 40) | (uint64(key[19]) << 32) | (uint64(key[20]) << 24) | (uint64(key[21]) << 16) | (uint64(key[22]) << 8) | uint64(key[23])
			b = (uint64(key[8]) << 56) | (uint64(key[9]) << 48) | (uint64(key[10]) << 40) | (uint64(key[11]) << 32) | (uint64(key[12]) << 24) | (uint64(key[13]) << 16) | (uint64(key[14]) << 8) | uint64(key[15])
			var bit2 uint64 = ((hash ^ b) * FNV_PRIME) >> 40
			var mask uint64 = 1<<(bit2&63)
			if t.bloom32[bit2>>6]&mask != 0 {
				d, l = bytes2uint64(key[24:])
				if index, length, exists = t.find3(key, a, b, c, d, l); exists {
					return index, length, true
				}
			}
			if t.bloom24[bit2>>6]&mask != 0 {
				if index, length, exists = t.find2(key[0:24], a, b, c, 7); exists {
					return index, length, true
				}
			}
			if t.bloom16[bit>>6]&(1<<(bit&63)) != 0 {
				if index, length, exists = t.find1(key[0:16], double{a, b}, 7); exists {
					return index, length, true
				}
			}
			return t.find0(key[0:8], a, 7)

		case 4:
			a = (uint64(key[0]) << 56) | (uint64(key[1]) << 48) | (uint64(key[2]) << 40) | (uint64(key[3]) << 32) | (uint64(key[4]) << 24) | (uint64(key[5]) << 16) | (uint64(key[6]) << 8) | uint64(key[7])
			var hash uint64 = ((FNV_OFFSET ^ a) * FNV_PRIME)
			var bit uint64 = hash >> 40
			if t.bloomBigSkip[bit>>6]&(1<<(bit&63)) == 0 {
				return t.find0(key[0:8], a, 7)
			}
			d = (uint64(key[24]) << 56) | (uint64(key[25]) << 48) | (uint64(key[26]) << 40) | (uint64(key[27]) << 32) | (uint64(key[28]) << 24) | (uint64(key[29]) << 16) | (uint64(key[30]) << 8) | uint64(key[31])
			c = (uint64(key[16]) << 56) | (uint64(key[17]) << 48) | (uint64(key[18]) << 40) | (uint64(key[19]) << 32) | (uint64(key[20]) << 24) | (uint64(key[21]) << 16) | (uint64(key[22]) << 8) | uint64(key[23])
			b = (uint64(key[8]) << 56) | (uint64(key[9]) << 48) | (uint64(key[10]) << 40) | (uint64(key[11]) << 32) | (uint64(key[12]) << 24) | (uint64(key[13]) << 16) | (uint64(key[14]) << 8) | uint64(key[15])
			var bit2 uint64 = ((hash ^ b) * FNV_PRIME) >> 40
			var idx uint64 = bit2>>6
			var mask uint64 = 1<<(bit2&63)
			if t.bloom40[idx]&mask != 0 {
				e, l = bytes2uint64(key[32:])
				if index, length, exists = t.find4(key, a, b, c, d, e, l); exists {
					return index, length, true
				}
			}
			if t.bloom32[idx]&mask != 0 {
				if index, length, exists = t.find3(key[0:32], a, b, c, d, 7); exists {
					return index, length, true
				}
			}
			if t.bloom24[idx]&mask != 0 {
				if index, length, exists = t.find2(key[0:24], a, b, c, 7); exists {
					return index, length, true
				}
			}
			if t.bloom16[bit>>6]&(1<<(bit&63)) != 0 {
				if index, length, exists = t.find1(key[0:16], double{a, b}, 7); exists {
					return index, length, true
				}
			}
			return t.find0(key[0:8], a, 7)

		default:
			return 0, 0, false
	}
}
   
   // Find returns the index based on the key.
   func (t *Fast) Find(key []byte) (uint32, bool) {

	   switch (len(key) - 1) / 8 {
		   case 0: // 0 - 8 bytes
		   		switch len(key) {
					case 0:
						return 0, false
					case 1:
						index := t.map1[key[0]]
						if index != FALSE {
							return index, true
						}
						return 0, false
					case 2:
						index := t.map2[(uint64(key[0]) << 8) | uint64(key[1])]
						if index != FALSE {
							return index, true
						}
						return 0, false
					case 3:
						index, exists := t.map4[0][(uint32(key[0]) << 16) | (uint32(key[1]) << 8) | uint32(key[2])]
						return index, exists
					case 4:
						index, exists := t.map4[1][(uint32(key[0]) << 24) | (uint32(key[1]) << 16) | (uint32(key[2]) << 8) | uint32(key[3])]
						return index, exists
					default:
						a, l := bytes2uint64(key)
						index, exists := t.map8[l-4][a]
						return index, exists
				}
			   
		   case 1: // 9 - 16 bytes
		   	   a, _ := bytes2uint64(key)
			   bit := ((FNV_OFFSET ^ a) * FNV_PRIME) >> 40
			   if t.bloom16[bit>>6]&(1<<(bit&63)) == 0 {
				  return 0, false
			   }
			   b, l := bytes2uint64(key[8:])
			   index, exists := t.map16[l][double{a, b}]
			   return index, exists
			   
		   case 2: // 17 - 24 bytes
		       a, _ := bytes2uint64(key)
		   	   b, _ := bytes2uint64(key[8:])
			   bit := (((((FNV_OFFSET ^ a) * FNV_PRIME)) ^ b) * FNV_PRIME) >> 40
			   if t.bloom24[bit>>6]&(1<<(bit&63)) == 0 {
					return 0, false
			   }
			   c, l := bytes2uint64(key[16:])
			   var at, min int
	   		   var compare uint64
			   cur := t.limit24[l]
			   max := len(cur) - 1
			   for min <= max {
				   at = min + ((max - min) / 2)
				   if compare = cur[at][0]; a < compare {
					   max = at - 1
					   continue
				   }
				   if a > compare {
					   min = at + 1
					   continue
				   }
				   if compare = cur[at][1]; b < compare {
					   max = at - 1
					   continue
				   }
				   if b > compare {
					   min = at + 1
					   continue
				   }
				   if compare = cur[at][2]; c < compare {
					   max = at - 1
					   continue
				   }
				   if c > compare {
					   min = at + 1
					   continue
				   }
				   return uint32(at) + t.count[l + 16], true // found
			   }
			   return uint32(min) + t.count[l + 16], false // doesn't exist
			   
		   case 3: // 25 - 32 bytes
		       a, _ := bytes2uint64(key)
		       b, _ := bytes2uint64(key[8:])
			   bit := (((((FNV_OFFSET ^ a) * FNV_PRIME)) ^ b) * FNV_PRIME) >> 40
			   if t.bloom32[bit>>6]&(1<<(bit&63)) == 0 {
				  return 0, false
			   }
			   c, _ := bytes2uint64(key[16:])
			   d, l := bytes2uint64(key[24:])
			   var at, min int
	   		   var compare uint64
			   cur := t.limit32[l]
			   max := len(cur) - 1
			   for min <= max {
				   at = min + ((max - min) / 2)
				   if compare = cur[at][0]; a < compare {
					   max = at - 1
					   continue
				   }
				   if a > compare {
					   min = at + 1
					   continue
				   }
				   if compare = cur[at][1]; b < compare {
					   max = at - 1
					   continue
				   }
				   if b > compare {
					   min = at + 1
					   continue
				   }
				   if compare = cur[at][2]; c < compare {
					   max = at - 1
					   continue
				   }
				   if c > compare {
					   min = at + 1
					   continue
				   }
				   if compare = cur[at][3]; d < compare {
					   max = at - 1
					   continue
				   }
				   if d > compare {
					   min = at + 1
					   continue
				   }
				   return uint32(at) + t.count[l + 24], true // found
			   }
			   return uint32(min) + t.count[l + 24], false // doesn't exist
			   
		   case 4: // 33 - 40 bytes
			   a, _ := bytes2uint64(key)
			   b, _ := bytes2uint64(key[8:])
			   bit := (((((FNV_OFFSET ^ a) * FNV_PRIME)) ^ b) * FNV_PRIME) >> 40
			   if t.bloom40[bit>>6]&(1<<(bit&63)) == 0 {
				 return 0, false
			   }
			   c, _ := bytes2uint64(key[16:])
			   d, _ := bytes2uint64(key[24:])
			   e, l := bytes2uint64(key[32:])
			   var at, min int
	           var compare uint64
			   cur := t.limit40[l]
			   max := len(cur) - 1
			   for min <= max {
				   at = min + ((max - min) / 2)
				   if compare = cur[at][0]; a < compare {
					   max = at - 1
					   continue
				   }
				   if a > compare {
					   min = at + 1
					   continue
				   }
				   if compare = cur[at][1]; b < compare {
					   max = at - 1
					   continue
				   }
				   if b > compare {
					   min = at + 1
					   continue
				   }
				   if compare = cur[at][2]; c < compare {
					   max = at - 1
					   continue
				   }
				   if c > compare {
					   min = at + 1
					   continue
				   }
				   if compare = cur[at][3]; d < compare {
					   max = at - 1
					   continue
				   }
				   if d > compare {
					   min = at + 1
					   continue
				   }
				   if compare = cur[at][4]; e < compare {
					   max = at - 1
					   continue
				   }
				   if e > compare {
					   min = at + 1
					   continue
				   }
				   return uint32(at) + t.count[l + 32], true // found
			   }
			   return uint32(min) + t.count[l + 32], false // doesn't exist

		   default: // > 40 bytes
			   return uint32(t.total), false
	   }
   }

   // AddUnsorted adds this key to the end of the index for later building with Build.
   func (t *Fast) Add(key []byte) error {
	   switch (len(key) - 1) / 8 {
		   case 0:
			   a, i := bytes2uint64(key)
			   if i > 3 {
					var hash uint64 = a >> ((uint64(i) - 4) * 8)
					hash = (FIBONACCI * hash) >> 40
					t.bloom8Top4[hash>>6] |= 1 << (hash&63) // set the bit
					if i > 5 {
						hash = a >> ((uint64(i) - 6) * 8)
						hash = (FIBONACCI * hash) >> 40
						t.bloom8Top2[hash>>6] |= 1 << (hash&63) // set the bit
					}
			   }
			   t.limit8[i] = append(t.limit8[i], a)
			   t.order8[i] = append(t.order8[i], t.total)
			   t.count[i + 1]++
			   t.total++
			   return nil
		   case 1:
			   a, _ := bytes2uint64(key)
			   b, i := bytes2uint64(key[8:])
			   var hash uint64 = ((FNV_OFFSET ^ a) * FNV_PRIME) >> 40
			   t.bloom16[hash>>6] |= 1 << (hash&63) // set the bit
			   t.bloomBigSkip[hash>>6] |= 1 << (hash&63) // set the bit
			   if i > 1 {
					hash = b >> ((uint64(i) - 2) * 8)
					hash = ((((FNV_OFFSET ^ ((FIBONACCI * hash))) * FNV_PRIME) ^ a) * FNV_PRIME) >> 40
					t.bloom16Top6[hash>>6] |= 1 << (hash&63) // set the bit
					if i > 3 {
						hash = b >> ((uint64(i) - 4) * 8)
						hash = ((((FNV_OFFSET ^ ((FIBONACCI * hash))) * FNV_PRIME) ^ a) * FNV_PRIME) >> 40
						t.bloom16Top4[hash>>6] |= 1 << (hash&63) // set the bit
						if i > 5 {
							hash = b >> ((uint64(i) - 6) * 8)
							hash = ((((FNV_OFFSET ^ ((FIBONACCI * hash))) * FNV_PRIME) ^ a) * FNV_PRIME) >> 40
							t.bloom16Top2[hash>>6] |= 1 << (hash&63) // set the bit
						}
					}
			   }
			   t.limit16[i] = append(t.limit16[i], [2]uint64{a, b})
			   t.order16[i] = append(t.order16[i], t.total)
			   t.count[i + 9]++
			   t.total++
			   return nil
		   case 2:
			   a, _ := bytes2uint64(key)
			   b, _ := bytes2uint64(key[8:])
			   c, i := bytes2uint64(key[16:])
			   var hash uint64 = (((((FNV_OFFSET ^ a) * FNV_PRIME)) ^ b) * FNV_PRIME) >> 40
			   t.bloom24[hash>>6] |= 1 << (hash&63) // set the bit
			   hash = ((FNV_OFFSET ^ a) * FNV_PRIME) >> 40
			   t.bloomBigSkip[hash>>6] |= 1 << (hash&63) // set the bit
			   t.limit24[i] = append(t.limit24[i], [3]uint64{a, b, c})
			   t.order24[i] = append(t.order24[i], t.total)
			   t.count[i + 17]++
			   t.total++
			   return nil
		   case 3:
			   a, _ := bytes2uint64(key)
			   b, _ := bytes2uint64(key[8:])
			   c, _ := bytes2uint64(key[16:])
			   d, i := bytes2uint64(key[24:])
			   var hash uint64 = (((((FNV_OFFSET ^ a) * FNV_PRIME)) ^ b) * FNV_PRIME) >> 40
			   t.bloom32[hash>>6] |= 1 << (hash&63) // set the bit
			   hash = ((FNV_OFFSET ^ a) * FNV_PRIME) >> 40
			   t.bloomBigSkip[hash>>6] |= 1 << (hash&63) // set the bit
			   t.limit32[i] = append(t.limit32[i], [4]uint64{a, b, c, d})
			   t.order32[i] = append(t.order32[i], t.total)
			   t.count[i + 25]++
			   t.total++
			   return nil
		   case 4:
			   a, _ := bytes2uint64(key)
			   b, _ := bytes2uint64(key[8:])
			   c, _ := bytes2uint64(key[16:])
			   d, _ := bytes2uint64(key[24:])
			   e, i := bytes2uint64(key[32:])
			   var hash uint64 = (((((FNV_OFFSET ^ a) * FNV_PRIME)) ^ b) * FNV_PRIME) >> 40
			   t.bloom40[hash>>6] |= 1 << (hash&63) // set the bit
			   hash = ((FNV_OFFSET ^ a) * FNV_PRIME) >> 40
			   t.bloomBigSkip[hash>>6] |= 1 << (hash&63) // set the bit
			   t.limit40[i] = append(t.limit40[i], [5]uint64{a, b, c, d, e})
			   t.order40[i] = append(t.order40[i], t.total)
			   t.count[i + 33]++
			   t.total++
			   return nil
		   default:
			   return errors.New(`Maximum key length is 40 bytes`)
	   }
   }

   // Build sorts the keys and returns an array telling you how to sort the values, you must do this yourself.
   func (t *Fast) Build() error {

	   var l, run int
	   var on uint32
	   //imap := make([]int, t.total)

	    t.map4[0] = make(map[uint32]uint32)
		t.map4[1] = make(map[uint32]uint32)
		t.map8[0] = make(map[uint64]uint32)
		t.map8[1] = make(map[uint64]uint32)
		t.map8[2] = make(map[uint64]uint32)
		t.map8[3] = make(map[uint64]uint32)
		for run=0; run<8; run++ {
			t.map16[run] = make(map[double]uint32)
		}
		for run=0; run<256; run++ {
			t.map1[run] = FALSE
		}
		for run=0; run<65536; run ++ {
			t.map2[run] = FALSE
		} 
	   
	   {
	   var temp []sortIntUint64.KeyVal
	   for run=0; run<8; run++ {
		   if l = len(t.limit8[run]); l > 0 {
			   m := t.order8[run]
			   if l != len(m) {
				   return errors.New(`Build can only be run once`)
			   }
			   if cap(temp) < l {
				   temp = make([]sortIntUint64.KeyVal, l)
			   } else {
				   temp = temp[0:l]
			   }
			   for z, k := range t.limit8[run] {
				   temp[z] = sortIntUint64.KeyVal{m[z], k}
			   }
			   t.order8[run] = nil
			   sortIntUint64.Asc(temp)
			   newkey := t.limit8[run]

			   switch run {
			   	case 0:
					for i, obj := range temp {
						t.map1[obj.V] = on
						//imap[on] = obj.K
						on++
						newkey[i] = obj.V
					}
				case 1:
					for i, obj := range temp {
						t.map2[obj.V] = on
						//imap[on] = obj.K
						on++
						newkey[i] = obj.V
					}
				case 2:
					for i, obj := range temp {
						// obj.V is the key, on is the index
						t.map4[0][uint32(obj.V)] = on
						//imap[on] = obj.K
						on++
						newkey[i] = obj.V
					}
				case 3:
					for i, obj := range temp {
						// obj.V is the key, on is the index
						t.map4[1][uint32(obj.V)] = on
						//imap[on] = obj.K
						on++
						newkey[i] = obj.V
					}
				default:
					for i, obj := range temp {
						// obj.V is the key, on is the index
						t.map8[run-4][obj.V] = on
						//imap[on] = obj.K
						on++
						newkey[i] = obj.V
					}
			   }
		   }
	   }
	   }
	   
	   {
	   var temp sortLimit16.Slice
	   for run=0; run<8; run++ {
		   if l = len(t.limit16[run]); l > 0 {
			   m := t.order16[run]
			   if l != len(m) {
				   return errors.New(`Build can only be run once`)
			   }
			   if cap(temp) < l {
				   temp = make(sortLimit16.Slice, l)
			   } else {
				   temp = temp[0:l]
			   }
			   for z, k := range t.limit16[run] {
				   temp[z] = sortLimit16.KeyVal{m[z], k}
			   }
			   t.order16[run] = nil
			   sortLimit16.Asc(temp)
			   newkey := t.limit16[run]
			   for i, obj := range temp {
				   t.map16[run][obj.V] = on
				   //imap[on] = obj.K
				   on++
				   newkey[i] = obj.V
			   }
		   }
	   }
	   }
	   
	   {
	   var temp sortLimit24.Slice
	   for run=0; run<8; run++ {
		   if l = len(t.limit24[run]); l > 0 {
			   m := t.order24[run]
			   if l != len(m) {
				   return errors.New(`Build can only be run once`)
			   }
			   if cap(temp) < l {
				   temp = make(sortLimit24.Slice, l)
			   } else {
				   temp = temp[0:l]
			   }
			   for z, k := range t.limit24[run] {
				   temp[z] = sortLimit24.KeyVal{m[z], k}
			   }
			   t.order24[run] = nil
			   sortLimit24.Asc(temp)
			   newkey := t.limit24[run]
			   for i, obj := range temp {
				   //imap[on] = obj.K
				   //on++
				   newkey[i] = obj.V
			   }
		   }
	   }
	   }
	   
	   {
	   var temp sortLimit32.Slice
	   for run=0; run<8; run++ {
		   if l = len(t.limit32[run]); l > 0 {
			   m := t.order32[run]
			   if l != len(m) {
				   return errors.New(`Build can only be run once`)
			   }
			   if cap(temp) < l {
				   temp = make(sortLimit32.Slice, l)
			   } else {
				   temp = temp[0:l]
			   }
			   for z, k := range t.limit32[run] {
				   temp[z] = sortLimit32.KeyVal{m[z], k}
			   }
			   t.order32[run] = nil
			   sortLimit32.Asc(temp)
			   newkey := t.limit32[run]
			   for i, obj := range temp {
				   //imap[on] = obj.K
				   //on++
				   newkey[i] = obj.V
			   }
		   }
	   }
	   }
	   
	   {
	   var temp sortLimit40.Slice
	   for run=0; run<8; run++ {
		   if l = len(t.limit40[run]); l > 0 {
			   m := t.order40[run]
			   if l != len(m) {
				   return errors.New(`Build can only be run once`)
			   }
			   if cap(temp) < l {
				   temp = make(sortLimit40.Slice, l)
			   } else {
				   temp = temp[0:l]
			   }
			   for z, k := range t.limit40[run] {
				   temp[z] = sortLimit40.KeyVal{m[z], k}
			   }
			   t.order40[run] = nil
			   sortLimit40.Asc(temp)
			   newkey := t.limit40[run]
			   for i, obj := range temp {
				   //imap[on] = obj.K
				   //on++
				   newkey[i] = obj.V
			   }
		   }
	   }
	   }
	   
	   // Correct all the counts
	   for run=2; run<41; run++ {
		   t.count[run] += t.count[run-1]
	   }
	   
	   return nil
   }
   
   func (t *Fast) Optimize() {
   
	   var l, run int
	   
	   for run=0; run<8; run++ {
		   if l = len(t.limit8[run]); l > 0 {
			   newkey := make([]uint64, l)
			   copy(newkey, t.limit8[run])
			   t.limit8[run] = newkey
		   }
	   }
	   
	   for run=0; run<8; run++ {
		   if l = len(t.limit16[run]); l > 0 {
			   newkey := make([][2]uint64, l)
			   copy(newkey, t.limit16[run])
			   t.limit16[run] = newkey
		   }
	   }
	   
	   for run=0; run<8; run++ {
		   if l = len(t.limit24[run]); l > 0 {
			   newkey := make([][3]uint64, l)
			   copy(newkey, t.limit24[run])
			   t.limit24[run] = newkey
		   }
	   }
	   
	   for run=0; run<8; run++ {
		   if l = len(t.limit32[run]); l > 0 {
			   newkey := make([][4]uint64, l)
			   copy(newkey, t.limit32[run])
			   t.limit32[run] = newkey
		   }
	   }
	   
	   for run=0; run<8; run++ {
		   if l = len(t.limit40[run]); l > 0 {
			   newkey := make([][5]uint64, l)
			   copy(newkey, t.limit40[run])
			   t.limit40[run] = newkey
		   }
	   }
   }
   
   // Reset() must be called before Next(). Returns whether there are any entries.
   func (t *Fast) Reset() bool {
	   t.onlimit = 0
	   t.on8 = 0
	   t.oncursor = 0
	   if len(t.limit8[0]) == 0 {
		   if t.total == 0 {
			   return false
		   } else {
			   t.forward(0)
		   }
	   }
	   return true
   }
   
   func (t *Fast) forward(l int) bool {
	   t.oncursor++
	   for t.oncursor >= l {
		   t.oncursor = 0
		   if t.on8++; t.on8 == 8 {
			   t.on8 = 0
			   if t.onlimit++; t.onlimit == 5 {
				   t.Reset()
				   return true
			   }
		   }
		   switch t.onlimit {
			   case 0: l = len(t.limit8[t.on8])
			   case 1: l = len(t.limit16[t.on8])
			   case 2: l = len(t.limit24[t.on8])
			   case 3: l = len(t.limit32[t.on8])
			   case 4: l = len(t.limit40[t.on8])
		   }
	   }
	   return false
   }
   
   func (t *Fast) Next() ([]byte, bool) {
	   on8 := t.on8
	   switch t.onlimit {
		   case 0:
			   v := t.limit8[on8][t.oncursor]
			   eof := t.forward(len(t.limit8[on8]))
			   return reverse8(v, on8 + 1), eof
		   case 1:
			   v := t.limit16[on8][t.oncursor]
			   eof := t.forward(len(t.limit16[on8]))
			   return reverse16(v, on8 + 1), eof
		   case 2:
			   v := t.limit24[on8][t.oncursor]
			   eof := t.forward(len(t.limit24[on8]))
			   return reverse24(v, on8 + 1), eof
		   case 3:
			   v := t.limit32[on8][t.oncursor]
			   eof := t.forward(len(t.limit32[on8]))
			   return reverse32(v, on8 + 1), eof
		   default:
			   v := t.limit40[on8][t.oncursor]
			   eof := t.forward(len(t.limit40[on8]))
			   return reverse40(v, on8 + 1), eof
	   }
   }
   
   func (t *Fast) LongestLength() int { // the length of the longest key
	   var run int
	   for run=7; run>=0; run-- {
		   if len(t.limit40[run]) > 0 {
			   return 33 + run
		   }
	   }
	   for run=7; run>=0; run-- {
		   if len(t.limit32[run]) > 0 {
			   return 25 + run
		   }
	   }
	   for run=7; run>=0; run-- {
		   if len(t.limit24[run]) > 0 {
			   return 17 + run
		   }
	   }
	   for run=7; run>=0; run-- {
		   if len(t.limit16[run]) > 0 {
			   return 9 + run
		   }
	   }
	   for run=7; run>=0; run-- {
		   if len(t.limit8[run]) > 0 {
			   return 1 + run
		   }
	   }
	   return 0
   }
   
   func (t *Fast) Keys() [][]byte {
   
	   var on, run int
	   keys := make([][]byte, t.total)
	   
	   for run=0; run<8; run++ {
		   for _, v := range t.limit8[run] {
			   keys[on] = reverse8(v, run + 1)
			   on++
		   }
	   }
	   for run=0; run<8; run++ {
		   for _, v := range t.limit16[run] {
			   keys[on] = reverse16(v, run + 1)
			   on++
		   }
	   }
	   for run=0; run<8; run++ {
		   for _, v := range t.limit24[run] {
			   keys[on] = reverse24(v, run + 1)
			   on++
		   }
	   }
	   for run=0; run<8; run++ {
		   for _, v := range t.limit32[run] {
			   keys[on] = reverse32(v, run + 1)
			   on++
		   }
	   }
	   for run=0; run<8; run++ {
		   for _, v := range t.limit40[run] {
			   keys[on] = reverse40(v, run + 1)
			   on++
		   }
	   }
	   
	   return keys
   }
   
   func (t *Fast) Write(w custom.Interface) {
	   var i, run int
   
	   // Write total
	   w.WriteUint64Variable(uint64(t.total))
	   
	   // Write count
	   for i=0; i<64; i++ {
		   w.WriteUint64Variable(uint64(t.count[i]))
	   }
	   
	   // Write t.limit8
	   for run=0; run<8; run++ {
		   tmp := t.limit8[run]
		   w.WriteUint64Variable(uint64(len(tmp)))
		   for _, v := range tmp {
			   w.WriteUint64(v)
		   }
	   }
	   // Write t.limit16
	   for run=0; run<8; run++ {
		   tmp := t.limit16[run]
		   w.WriteUint64Variable(uint64(len(tmp)))
		   for _, v := range tmp {
			   w.WriteUint64(v[0])
			   w.WriteUint64(v[1])
		   }
	   }
	   // Write t.limit24
	   for run=0; run<8; run++ {
		   tmp := t.limit24[run]
		   w.WriteUint64Variable(uint64(len(tmp)))
		   for _, v := range tmp {
			   w.WriteUint64(v[0])
			   w.WriteUint64(v[1])
			   w.WriteUint64(v[2])
		   }
	   }
	   // Write t.limit32
	   for run=0; run<8; run++ {
		   tmp := t.limit32[run]
		   w.WriteUint64Variable(uint64(len(tmp)))
		   for _, v := range tmp {
			   w.WriteUint64(v[0])
			   w.WriteUint64(v[1])
			   w.WriteUint64(v[2])
			   w.WriteUint64(v[3])
		   }
	   }
	   // Write t.limit40
	   for run=0; run<8; run++ {
		   tmp := t.limit40[run]
		   w.WriteUint64Variable(uint64(len(tmp)))
		   for _, v := range tmp {
			   w.WriteUint64(v[0])
			   w.WriteUint64(v[1])
			   w.WriteUint64(v[2])
			   w.WriteUint64(v[3])
			   w.WriteUint64(v[4])
		   }
	   }
   }
   
   func (t *Fast) Read(r *custom.Reader) {
	   var run int
	   var i, l, a, b, c, d, e uint64
   
	   // Write total
	   t.total = int(r.ReadUint64Variable())
	   
	   // Read count
	   for i=0; i<64; i++ {
		   t.count[i] = uint32(r.ReadUint64Variable())
	   }
	   
	   // Read t.limit8
	   for run=0; run<8; run++ {
		   l = r.ReadUint64Variable()
		   tmp := make([]uint64, l)
		   for i=0; i<l; i++ {
			   tmp[i] = r.ReadUint64()
		   }
		   t.limit8[run] = tmp
	   }
	   // Read t.limit16
	   for run=0; run<8; run++ {
		   l = r.ReadUint64Variable()
		   tmp := make([][2]uint64, l)
		   for i=0; i<l; i++ {
			   a = r.ReadUint64()
			   b = r.ReadUint64()
			   tmp[i] = [2]uint64{a, b}
		   }
		   t.limit16[run] = tmp
	   }
	   // Read t.limit24
	   for run=0; run<8; run++ {
		   l = r.ReadUint64Variable()
		   tmp := make([][3]uint64, l)
		   for i=0; i<l; i++ {
			   a = r.ReadUint64()
			   b = r.ReadUint64()
			   c = r.ReadUint64()
			   tmp[i] = [3]uint64{a, b, c}
		   }
		   t.limit24[run] = tmp
	   }
	   // Read t.limit32
	   for run=0; run<8; run++ {
		   l = r.ReadUint64Variable()
		   tmp := make([][4]uint64, l)
		   for i=0; i<l; i++ {
			   a = r.ReadUint64()
			   b = r.ReadUint64()
			   c = r.ReadUint64()
			   d = r.ReadUint64()
			   tmp[i] = [4]uint64{a, b, c, d}
		   }
		   t.limit32[run] = tmp
	   }
	   // Read t.limit40
	   for run=0; run<8; run++ {
		   l = r.ReadUint64Variable()
		   tmp := make([][5]uint64, l)
		   for i=0; i<l; i++ {
			   a = r.ReadUint64()
			   b = r.ReadUint64()
			   c = r.ReadUint64()
			   d = r.ReadUint64()
			   e = r.ReadUint64()
			   tmp[i] = [5]uint64{a, b, c, d, e}
		   }
		   t.limit40[run] = tmp
	   }
   }

// ---------- Light ----------
// Key bytes has around 5KB of memory overhead for the structures, beyond this it stores all keys as efficiently as possible.

// Add this to any struct to make it binary searchable.
type Light struct {
 limit8 [8][]uint64 // where len(word) <= 8
 limit16 [8][][2]uint64
 limit24 [8][][3]uint64
 limit32 [8][][4]uint64
 limit40 [8][][5]uint64
 limit48 [8][][6]uint64
 limit56 [8][][7]uint64
 limit64 [8][][8]uint64
// The order vars are used only when using AddSorted & Build. Build clears them. They are used for remembering the order that the keys were added in so the remap can be returned to the user by Build.
 order8 [8][]int
 order16 [8][]int
 order24 [8][]int
 order32 [8][]int
 order40 [8][]int
 order48 [8][]int
 order56 [8][]int
 order64 [8][]int
 count [64]int // Used to convert limit maps to the 1D array value indicating where the value exists
 total int
// Used for iterating through all of it
 onlimit int
 on8 int
 oncursor int
}

func bytes2uint64(word []byte) (uint64, int) {
	switch len(word) {
		case 0:
			return 0, 0 // an empty slice is sorted with the single characters
		case 1:
			return uint64(word[0]), 0
		case 2:
			return (uint64(word[0]) << 8) | uint64(word[1]), 1
		case 3:
			return (uint64(word[0]) << 16) | (uint64(word[1]) << 8) | uint64(word[2]), 2
		case 4:
			return (uint64(word[0]) << 24) | (uint64(word[1]) << 16) | (uint64(word[2]) << 8) | uint64(word[3]), 3
		case 5:
			return (uint64(word[0]) << 32) | (uint64(word[1]) << 24) | (uint64(word[2]) << 16) | (uint64(word[3]) << 8) | uint64(word[4]), 4
		case 6:
			return (uint64(word[0]) << 40) | (uint64(word[1]) << 32) | (uint64(word[2]) << 24) | (uint64(word[3]) << 16) | (uint64(word[4]) << 8) | uint64(word[5]), 5
		case 7:
			return (uint64(word[0]) << 48) | (uint64(word[1]) << 40) | (uint64(word[2]) << 32) | (uint64(word[3]) << 24) | (uint64(word[4]) << 16) | (uint64(word[5]) << 8) | uint64(word[6]), 6
		default:
			return (uint64(word[0]) << 56) | (uint64(word[1]) << 48) | (uint64(word[2]) << 40) | (uint64(word[3]) << 32) | (uint64(word[4]) << 24) | (uint64(word[5]) << 16) | (uint64(word[6]) << 8) | uint64(word[7]), 7
	}
}

func uint642bytes(word []byte, v uint64) {
	word[7] = byte(v & 255)
	word[6] = byte((v >> 8) & 255)
	word[5] = byte((v >> 16) & 255)
	word[4] = byte((v >> 24) & 255)
	word[3] = byte((v >> 32) & 255)
	word[2] = byte((v >> 40) & 255)
	word[1] = byte((v >> 48) & 255)
	word[0] = byte((v >> 56) & 255)
}


func uint642bytesend(word []byte, v uint64, i int) {
	switch i {
		case 8:
			word[7] = byte(v & 255)
			word[6] = byte((v >> 8) & 255)
			word[5] = byte((v >> 16) & 255)
			word[4] = byte((v >> 24) & 255)
			word[3] = byte((v >> 32) & 255)
			word[2] = byte((v >> 40) & 255)
			word[1] = byte((v >> 48) & 255)
			word[0] = byte((v >> 56) & 255)
		case 7:
			word[6] = byte(v & 255)
			word[5] = byte((v >> 8) & 255)
			word[4] = byte((v >> 16) & 255)
			word[3] = byte((v >> 24) & 255)
			word[2] = byte((v >> 32) & 255)
			word[1] = byte((v >> 40) & 255)
			word[0] = byte((v >> 48) & 255)
		case 6:
			word[5] = byte(v & 255)
			word[4] = byte((v >> 8) & 255)
			word[3] = byte((v >> 16) & 255)
			word[2] = byte((v >> 24) & 255)
			word[1] = byte((v >> 32) & 255)
			word[0] = byte((v >> 40) & 255)
		case 5:
			word[4] = byte(v & 255)
			word[3] = byte((v >> 8) & 255)
			word[2] = byte((v >> 16) & 255)
			word[1] = byte((v >> 24) & 255)
			word[0] = byte((v >> 32) & 255)
		case 4:
			word[3] = byte(v & 255)
			word[2] = byte((v >> 8) & 255)
			word[1] = byte((v >> 16) & 255)
			word[0] = byte((v >> 24) & 255)
		case 3:
			word[2] = byte(v & 255)
			word[1] = byte((v >> 8) & 255)
			word[0] = byte((v >> 16) & 255)
		case 2:
			word[1] = byte(v & 255)
			word[0] = byte((v >> 8) & 255)
		case 1:
			word[0] = byte(v & 255)
	}
	return
}

func reverse8(v uint64, i int) []byte {
	word := make([]byte, 8)
	uint642bytesend(word, v, i)
	return word[0:i]
}

func reverse8b(v [2]uint64, i int) []byte {
	word := make([]byte, 8)
	uint642bytesend(word, v[0], i)
	return word[0:i]
}

func reverse16(v [2]uint64, i int) []byte {
	word := make([]byte, 16)
	uint642bytes(word, v[0])
	uint642bytesend(word[8:], v[1], i)
	return word[0 : 8 + i]
}

func reverse16b(v [3]uint64, i int) []byte {
	word := make([]byte, 16)
	uint642bytes(word, v[0])
	uint642bytesend(word[8:], v[1], i)
	return word[0 : 8 + i]
}

func reverse24(v [3]uint64, i int) []byte {
	word := make([]byte, 24)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytesend(word[16:], v[2], i)
	return word[0 : 16 + i]
}

func reverse24b(v [4]uint64, i int) []byte {
	word := make([]byte, 24)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytesend(word[16:], v[2], i)
	return word[0 : 16 + i]
}

func reverse32(v [4]uint64, i int) []byte {
	word := make([]byte, 32)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytes(word[16:], v[2])
	uint642bytesend(word[24:], v[3], i)
	return word[0 : 24 + i]
}

func reverse32b(v [5]uint64, i int) []byte {
	word := make([]byte, 32)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytes(word[16:], v[2])
	uint642bytesend(word[24:], v[3], i)
	return word[0 : 24 + i]
}

func reverse40(v [5]uint64, i int) []byte {
	word := make([]byte, 40)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytes(word[16:], v[2])
	uint642bytes(word[24:], v[3])
	uint642bytesend(word[32:], v[4], i)
	return word[0 : 32 + i]
}

func reverse40b(v [6]uint64, i int) []byte {
	word := make([]byte, 40)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytes(word[16:], v[2])
	uint642bytes(word[24:], v[3])
	uint642bytesend(word[32:], v[4], i)
	return word[0 : 32 + i]
}

func reverse48(v [6]uint64, i int) []byte {
	word := make([]byte, 48)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytes(word[16:], v[2])
	uint642bytes(word[24:], v[3])
	uint642bytes(word[32:], v[4])
	uint642bytesend(word[40:], v[5], i)
	return word[0 : 40 + i]
}

func reverse48b(v [7]uint64, i int) []byte {
	word := make([]byte, 48)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytes(word[16:], v[2])
	uint642bytes(word[24:], v[3])
	uint642bytes(word[32:], v[4])
	uint642bytesend(word[40:], v[5], i)
	return word[0 : 40 + i]
}

func reverse56(v [7]uint64, i int) []byte {
	word := make([]byte, 56)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytes(word[16:], v[2])
	uint642bytes(word[24:], v[3])
	uint642bytes(word[32:], v[4])
	uint642bytes(word[40:], v[5])
	uint642bytesend(word[48:], v[6], i)
	return word[0 : 48 + i]
}

func reverse56b(v [8]uint64, i int) []byte {
	word := make([]byte, 56)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytes(word[16:], v[2])
	uint642bytes(word[24:], v[3])
	uint642bytes(word[32:], v[4])
	uint642bytes(word[40:], v[5])
	uint642bytesend(word[48:], v[6], i)
	return word[0 : 48 + i]
}

func reverse64(v [8]uint64, i int) []byte {
	word := make([]byte, 64)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytes(word[16:], v[2])
	uint642bytes(word[24:], v[3])
	uint642bytes(word[32:], v[4])
	uint642bytes(word[40:], v[5])
	uint642bytes(word[48:], v[6])
	uint642bytesend(word[56:], v[7], i)
	return word[0 : 56 + i]
}

func reverse64b(v [9]uint64, i int) []byte {
	word := make([]byte, 64)
	uint642bytes(word, v[0])
	uint642bytes(word[8:], v[1])
	uint642bytes(word[16:], v[2])
	uint642bytes(word[24:], v[3])
	uint642bytes(word[32:], v[4])
	uint642bytes(word[40:], v[5])
	uint642bytes(word[48:], v[6])
	uint642bytesend(word[56:], v[7], i)
	return word[0 : 56 + i]
}

func (t *Light) Len() int {
	return t.total
}

// Find returns the index based on the key.
func (t *Light) Find(key []byte) (int, bool) {
	
	var at, min int
	var compare uint64
	switch (len(key) - 1) / 8 {
	
		case 0: // 0 - 8 bytes
			a, l := bytes2uint64(key)
			cur := t.limit8[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				return at + t.count[l], true // found
			}
			return min + t.count[l], false // doesn't exist
			
		case 1: // 9 - 16 bytes
			a, _ := bytes2uint64(key)
			b, l := bytes2uint64(key[8:])
			cur := t.limit16[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 8], true // found
			}
			return min + t.count[l + 8], false // doesn't exist
			
		case 2: // 17 - 24 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, l := bytes2uint64(key[16:])
			cur := t.limit24[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 16], true // found
			}
			return min + t.count[l + 16], false // doesn't exist
			
		case 3: // 25 - 32 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, l := bytes2uint64(key[24:])
			cur := t.limit32[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 24], true // found
			}
			return min + t.count[l + 24], false // doesn't exist
			
		case 4: // 33 - 40 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, l := bytes2uint64(key[32:])
			cur := t.limit40[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 32], true // found
			}
			return min + t.count[l + 32], false // doesn't exist
			
		case 5: // 41 - 48 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, l := bytes2uint64(key[40:])
			cur := t.limit48[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 40], true // found
			}
			return min + t.count[l + 40], false // doesn't exist
			
		case 6: // 49 - 56 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, l := bytes2uint64(key[48:])
			cur := t.limit56[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][6]; g < compare {
					max = at - 1
					continue
				}
				if g > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 48], true // found
			}
			return min + t.count[l + 48], false // doesn't exist
			
		case 7: // 57 - 64 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, _ := bytes2uint64(key[48:])
			h, l := bytes2uint64(key[56:])
			cur := t.limit64[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][6]; g < compare {
					max = at - 1
					continue
				}
				if g > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][7]; h < compare {
					max = at - 1
					continue
				}
				if h > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 56], true // found
			}
			return min + t.count[l + 56], false // doesn't exist
		
		default: // > 64 bytes
			return t.total, false
	}
}

// Add is equivalent to Find and then AddAt
func (t *Light) Add(key []byte) (int, bool) {
	
	var at, min int
	var compare uint64
	switch (len(key) - 1) / 8 {
	
		case 0: // 0 - 8 bytes
			a, l := bytes2uint64(key)
			cur := t.limit8[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				return at + t.count[l], true // found
			}
			// Doesn't exist so add it >
			at = min
			min += t.count[l]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:at])
				copy(tmp[at+1:], cur[at:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[at+1:], cur[at:])
			}
			cur[at] = a
			t.limit8[l] = cur
			for l++; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return min, false
			
		case 1: // 9 - 16 bytes
			a, _ := bytes2uint64(key)
			b, l := bytes2uint64(key[8:])
			cur := t.limit16[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 8], true // found
			}
			// Doesn't exist so add it >
			at = min
			min += t.count[l + 8]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][2]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:at])
				copy(tmp[at+1:], cur[at:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[at+1:], cur[at:])
			}
			cur[at] = [2]uint64{a, b}
			t.limit16[l] = cur
			for l+=9; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return min, false
			
		case 2: // 17 - 24 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, l := bytes2uint64(key[16:])
			cur := t.limit24[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 16], true // found
			}
			// Doesn't exist so add it >
			at = min
			min += t.count[l + 16]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][3]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:at])
				copy(tmp[at+1:], cur[at:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[at+1:], cur[at:])
			}
			cur[at] = [3]uint64{a, b, c}
			t.limit24[l] = cur
			for l+=17; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return min, false
			
		case 3: // 25 - 32 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, l := bytes2uint64(key[24:])
			cur := t.limit32[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 24], true // found
			}
			// Doesn't exist so add it >
			at = min
			min += t.count[l + 24]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][4]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:at])
				copy(tmp[at+1:], cur[at:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[at+1:], cur[at:])
			}
			cur[at] = [4]uint64{a, b, c, d}
			t.limit32[l] = cur
			for l+=25; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return min, false
			
		case 4: // 33 - 40 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, l := bytes2uint64(key[32:])
			cur := t.limit40[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 32], true // found
			}
			// Doesn't exist so add it >
			at = min
			min += t.count[l + 32]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][5]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:at])
				copy(tmp[at+1:], cur[at:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[at+1:], cur[at:])
			}
			cur[at] = [5]uint64{a, b, c, d, e}
			t.limit40[l] = cur
			for l+=33; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return min, false
			
		case 5: // 41 - 48 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, l := bytes2uint64(key[40:])
			cur := t.limit48[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 40], true // found
			}
			// Doesn't exist so add it >
			at = min
			min += t.count[l + 40]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][6]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:at])
				copy(tmp[at+1:], cur[at:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[at+1:], cur[at:])
			}
			cur[at] = [6]uint64{a, b, c, d, e, f}
			t.limit48[l] = cur
			for l+=41; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return min, false
			
		case 6: // 49 - 56 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, l := bytes2uint64(key[48:])
			cur := t.limit56[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][6]; g < compare {
					max = at - 1
					continue
				}
				if g > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 48], true // found
			}
			// Doesn't exist so add it >
			at = min
			min += t.count[l + 48]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][7]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:at])
				copy(tmp[at+1:], cur[at:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[at+1:], cur[at:])
			}
			cur[at] = [7]uint64{a, b, c, d, e, f, g}
			t.limit56[l] = cur
			for l+=49; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return min, false
			
		case 7: // 57 - 64 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, _ := bytes2uint64(key[48:])
			h, l := bytes2uint64(key[56:])
			cur := t.limit64[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][6]; g < compare {
					max = at - 1
					continue
				}
				if g > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][7]; h < compare {
					max = at - 1
					continue
				}
				if h > compare {
					min = at + 1
					continue
				}
				return at + t.count[l + 56], true // found
			}
			// Doesn't exist so add it >
			at = min
			min += t.count[l + 56]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][8]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:at])
				copy(tmp[at+1:], cur[at:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[at+1:], cur[at:])
			}
			cur[at] = [8]uint64{a, b, c, d, e, f, g, h}
			t.limit64[l] = cur
			for l+=57; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return min, false
		
		default: // > 64 bytes
			return t.total, false
	}
}

// AddUnsorted adds this key to the end of the index for later building with Build.
func (t *Light) AddUnsorted(key []byte) error {
	switch (len(key) - 1) / 8 {
		case 0:
			a, i := bytes2uint64(key)
			t.limit8[i] = append(t.limit8[i], a)
			t.order8[i] = append(t.order8[i], t.total)
			t.count[i + 1]++
			t.total++
			return nil
		case 1:
			a, _ := bytes2uint64(key)
			b, i := bytes2uint64(key[8:])
			t.limit16[i] = append(t.limit16[i], [2]uint64{a, b})
			t.order16[i] = append(t.order16[i], t.total)
			t.count[i + 9]++
			t.total++
			return nil
		case 2:
			
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, i := bytes2uint64(key[16:])
			t.limit24[i] = append(t.limit24[i], [3]uint64{a, b, c})
			t.order24[i] = append(t.order24[i], t.total)
			t.count[i + 17]++
			t.total++
			return nil
		case 3:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, i := bytes2uint64(key[24:])
			t.limit32[i] = append(t.limit32[i], [4]uint64{a, b, c, d})
			t.order32[i] = append(t.order32[i], t.total)
			t.count[i + 25]++
			t.total++
			return nil
		case 4:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, i := bytes2uint64(key[32:])
			t.limit40[i] = append(t.limit40[i], [5]uint64{a, b, c, d, e})
			t.order40[i] = append(t.order40[i], t.total)
			t.count[i + 33]++
			t.total++
			return nil
		case 5:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, i := bytes2uint64(key[40:])
			t.limit48[i] = append(t.limit48[i], [6]uint64{a, b, c, d, e, f})
			t.order48[i] = append(t.order48[i], t.total)
			t.count[i + 41]++
			t.total++
			return nil
		case 6:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, i := bytes2uint64(key[48:])
			t.limit56[i] = append(t.limit56[i], [7]uint64{a, b, c, d, e, f, g})
			t.order56[i] = append(t.order56[i], t.total)
			t.count[i + 49]++
			t.total++
			return nil
		case 7:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, _ := bytes2uint64(key[48:])
			h, i := bytes2uint64(key[56:])
			t.limit64[i] = append(t.limit64[i], [8]uint64{a, b, c, d, e, f, g, h})
			t.order64[i] = append(t.order64[i], t.total)
			if i < 7 {
				t.count[i + 57]++
			}
			t.total++
			return nil
		default:
			return errors.New(`Maximum key length is 64 bytes`)
	}
}

// AddAt adds this key to the index in this exact position, so it does not require later rebuilding.
func (t *Light) AddAt(key []byte, i int) error {

	switch (len(key) - 1) / 8 {
		case 0:
			a, l := bytes2uint64(key)
			i -= t.count[l]
			cur := t.limit8[l]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:i])
				copy(tmp[i+1:], cur[i:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[i+1:], cur[i:])
			}
			cur[i] = a
			t.limit8[l] = cur
			for l++; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return nil
			
		case 1:
			a, _ := bytes2uint64(key)
			b, l := bytes2uint64(key[8:])
			i -= t.count[l + 8]
			cur := t.limit16[l]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][2]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:i])
				copy(tmp[i+1:], cur[i:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[i+1:], cur[i:])
			}
			cur[i] = [2]uint64{a, b}
			t.limit16[l] = cur
			for l+=9; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return nil
			
		case 2:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, l := bytes2uint64(key[16:])
			i -= t.count[l + 16]
			cur := t.limit24[l]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][3]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:i])
				copy(tmp[i+1:], cur[i:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[i+1:], cur[i:])
			}
			cur[i] = [3]uint64{a, b, c}
			t.limit24[l] = cur
			for l+=17; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return nil
			
		case 3:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, l := bytes2uint64(key[24:])
			i -= t.count[l + 24]
			cur := t.limit32[l]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][4]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:i])
				copy(tmp[i+1:], cur[i:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[i+1:], cur[i:])
			}
			cur[i] = [4]uint64{a, b, c, d}
			t.limit32[l] = cur
			for l+=25; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return nil
			
		case 4:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, l := bytes2uint64(key[32:])
			i -= t.count[l + 32]
			cur := t.limit40[l]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][5]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:i])
				copy(tmp[i+1:], cur[i:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[i+1:], cur[i:])
			}
			cur[i] = [5]uint64{a, b, c, d, e}
			t.limit40[l] = cur
			for l+=33; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return nil
			
		case 5:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, l := bytes2uint64(key[40:])
			i -= t.count[l + 40]
			cur := t.limit48[l]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][6]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:i])
				copy(tmp[i+1:], cur[i:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[i+1:], cur[i:])
			}
			cur[i] = [6]uint64{a, b, c, d, e, f}
			t.limit48[l] = cur
			for l+=41; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return nil
			
		case 6:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, l := bytes2uint64(key[48:])
			i -= t.count[l + 48]
			cur := t.limit56[l]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][7]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:i])
				copy(tmp[i+1:], cur[i:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[i+1:], cur[i:])
			}
			cur[i] = [7]uint64{a, b, c, d, e, f, g}
			t.limit56[l] = cur
			for l+=49; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return nil
			
		case 7:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, _ := bytes2uint64(key[48:])
			h, l := bytes2uint64(key[56:])
			i -= t.count[l + 56]
			cur := t.limit64[l]
			lc := len(cur)
			if lc == cap(cur) {
				tmp := make([][8]uint64, lc + 1, (lc * 2) + 1)
				copy(tmp, cur[0:i])
				copy(tmp[i+1:], cur[i:])
				cur = tmp
			} else {
				cur = cur[0:lc+1]
				copy(cur[i+1:], cur[i:])
			}
			cur[i] = [8]uint64{a, b, c, d, e, f, g, h}
			t.limit64[l] = cur
			for l+=57; l<64; l++ {
				t.count[l]++
			}
			t.total++
			return nil
			
		default:
			return errors.New(`Maximum key length is 64 bytes`)
	}
}

// Build sorts the keys and returns an array telling you how to sort the values, you must do this yourself.
func (t *Light) Build() ([]int, error) {

	var l, on, run int
	imap := make([]int, t.total)
	
	{
	var temp []sortIntUint64.KeyVal
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {
			m := t.order8[run]
			if l != len(m) {
				return nil, errors.New(`Build can only be run once`)
			}
			if cap(temp) < l {
				temp = make([]sortIntUint64.KeyVal, l)
			} else {
				temp = temp[0:l]
			}
			for z, k := range t.limit8[run] {
				temp[z] = sortIntUint64.KeyVal{m[z], k}
			}
			t.order8[run] = nil
			sortIntUint64.Asc(temp)
			newkey := t.limit8[run]
			for i, obj := range temp {
				imap[on] = obj.K
				on++
				newkey[i] = obj.V
			}
		}
	}
	}
	
	{
	var temp sortLimit16.Slice
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {
			m := t.order16[run]
			if l != len(m) {
				return nil, errors.New(`Build can only be run once`)
			}
			if cap(temp) < l {
				temp = make(sortLimit16.Slice, l)
			} else {
				temp = temp[0:l]
			}
			for z, k := range t.limit16[run] {
				temp[z] = sortLimit16.KeyVal{m[z], k}
			}
			t.order16[run] = nil
			sortLimit16.Asc(temp)
			newkey := t.limit16[run]
			for i, obj := range temp {
				imap[on] = obj.K
				on++
				newkey[i] = obj.V
			}
		}
	}
	}
	
	{
	var temp sortLimit24.Slice
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {
			m := t.order24[run]
			if l != len(m) {
				return nil, errors.New(`Build can only be run once`)
			}
			if cap(temp) < l {
				temp = make(sortLimit24.Slice, l)
			} else {
				temp = temp[0:l]
			}
			for z, k := range t.limit24[run] {
				temp[z] = sortLimit24.KeyVal{m[z], k}
			}
			t.order24[run] = nil
			sortLimit24.Asc(temp)
			newkey := t.limit24[run]
			for i, obj := range temp {
				imap[on] = obj.K
				on++
				newkey[i] = obj.V
			}
		}
	}
	}
	
	{
	var temp sortLimit32.Slice
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {
			m := t.order32[run]
			if l != len(m) {
				return nil, errors.New(`Build can only be run once`)
			}
			if cap(temp) < l {
				temp = make(sortLimit32.Slice, l)
			} else {
				temp = temp[0:l]
			}
			for z, k := range t.limit32[run] {
				temp[z] = sortLimit32.KeyVal{m[z], k}
			}
			t.order32[run] = nil
			sortLimit32.Asc(temp)
			newkey := t.limit32[run]
			for i, obj := range temp {
				imap[on] = obj.K
				on++
				newkey[i] = obj.V
			}
		}
	}
	}
	
	{
	var temp sortLimit40.Slice
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {
			m := t.order40[run]
			if l != len(m) {
				return nil, errors.New(`Build can only be run once`)
			}
			if cap(temp) < l {
				temp = make(sortLimit40.Slice, l)
			} else {
				temp = temp[0:l]
			}
			for z, k := range t.limit40[run] {
				temp[z] = sortLimit40.KeyVal{m[z], k}
			}
			t.order40[run] = nil
			sortLimit40.Asc(temp)
			newkey := t.limit40[run]
			for i, obj := range temp {
				imap[on] = obj.K
				on++
				newkey[i] = obj.V
			}
		}
	}
	}
	
	{
	var temp sortLimit48.Slice
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {
			m := t.order48[run]
			if l != len(m) {
				return nil, errors.New(`Build can only be run once`)
			}
			if cap(temp) < l {
				temp = make(sortLimit48.Slice, l)
			} else {
				temp = temp[0:l]
			}
			for z, k := range t.limit48[run] {
				temp[z] = sortLimit48.KeyVal{m[z], k}
			}
			t.order48[run] = nil
			sortLimit48.Asc(temp)
			newkey := t.limit48[run]
			for i, obj := range temp {
				imap[on] = obj.K
				on++
				newkey[i] = obj.V
			}
		}
	}
	}
	
	{
	var temp sortLimit56.Slice
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {
			m := t.order56[run]
			if l != len(m) {
				return nil, errors.New(`Build can only be run once`)
			}
			if cap(temp) < l {
				temp = make(sortLimit56.Slice, l)
			} else {
				temp = temp[0:l]
			}
			for z, k := range t.limit56[run] {
				temp[z] = sortLimit56.KeyVal{m[z], k}
			}
			t.order56[run] = nil
			sortLimit56.Asc(temp)
			newkey := t.limit56[run]
			for i, obj := range temp {
				imap[on] = obj.K
				on++
				newkey[i] = obj.V
			}
		}
	}
	}
	
	{
	var temp sortLimit64.Slice
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {
			m := t.order64[run]
			if l != len(m) {
				return nil, errors.New(`Build can only be run once`)
			}
			if cap(temp) < l {
				temp = make(sortLimit64.Slice, l)
			} else {
				temp = temp[0:l]
			}
			for z, k := range t.limit64[run] {
				temp[z] = sortLimit64.KeyVal{m[z], k}
			}
			t.order64[run] = nil
			sortLimit64.Asc(temp)
			newkey := t.limit64[run]
			for i, obj := range temp {
				imap[on] = obj.K
				on++
				newkey[i] = obj.V
			}
		}
	}
	}
	
	// Correct all the counts
	for run=2; run<64; run++ {
		t.count[run] += t.count[run-1]
	}
	
	return imap, nil
}

func (t *Light) Optimize() {

	var l, run int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {
			newkey := make([]uint64, l)
			copy(newkey, t.limit8[run])
			t.limit8[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {
			newkey := make([][2]uint64, l)
			copy(newkey, t.limit16[run])
			t.limit16[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {
			newkey := make([][3]uint64, l)
			copy(newkey, t.limit24[run])
			t.limit24[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {
			newkey := make([][4]uint64, l)
			copy(newkey, t.limit32[run])
			t.limit32[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {
			newkey := make([][5]uint64, l)
			copy(newkey, t.limit40[run])
			t.limit40[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {
			newkey := make([][6]uint64, l)
			copy(newkey, t.limit48[run])
			t.limit48[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {
			newkey := make([][7]uint64, l)
			copy(newkey, t.limit56[run])
			t.limit56[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {
			newkey := make([][8]uint64, l)
			copy(newkey, t.limit64[run])
			t.limit64[run] = newkey
		}
	}
}

// Reset() must be called before Next(). Returns whether there are any entries.
func (t *Light) Reset() bool {
	t.onlimit = 0
	t.on8 = 0
	t.oncursor = 0
	if len(t.limit8[0]) == 0 {
		if t.total == 0 {
			return false
		} else {
			t.forward(0)
		}
	}
	return true
}

func (t *Light) forward(l int) bool {
	t.oncursor++
	for t.oncursor >= l {
		t.oncursor = 0
		if t.on8++; t.on8 == 8 {
			t.on8 = 0
			if t.onlimit++; t.onlimit == 8 {
				t.Reset()
				return true
			}
		}
		switch t.onlimit {
			case 0: l = len(t.limit8[t.on8])
			case 1: l = len(t.limit16[t.on8])
			case 2: l = len(t.limit24[t.on8])
			case 3: l = len(t.limit32[t.on8])
			case 4: l = len(t.limit40[t.on8])
			case 5: l = len(t.limit48[t.on8])
			case 6: l = len(t.limit56[t.on8])
			case 7: l = len(t.limit64[t.on8])
		}
	}
	return false
}

func (t *Light) Next() ([]byte, bool) {
	on8 := t.on8
	switch t.onlimit {
		case 0:
			v := t.limit8[on8][t.oncursor]
			eof := t.forward(len(t.limit8[on8]))
			return reverse8(v, on8 + 1), eof
		case 1:
			v := t.limit16[on8][t.oncursor]
			eof := t.forward(len(t.limit16[on8]))
			return reverse16(v, on8 + 1), eof
		case 2:
			v := t.limit24[on8][t.oncursor]
			eof := t.forward(len(t.limit24[on8]))
			return reverse24(v, on8 + 1), eof
		case 3:
			v := t.limit32[on8][t.oncursor]
			eof := t.forward(len(t.limit32[on8]))
			return reverse32(v, on8 + 1), eof
		case 4:
			v := t.limit40[on8][t.oncursor]
			eof := t.forward(len(t.limit40[on8]))
			return reverse40(v, on8 + 1), eof
		case 5:
			v := t.limit48[on8][t.oncursor]
			eof := t.forward(len(t.limit48[on8]))
			return reverse48(v, on8 + 1), eof
		case 6:
			v := t.limit56[on8][t.oncursor]
			eof := t.forward(len(t.limit56[on8]))
			return reverse56(v, on8 + 1), eof
		default:
			v := t.limit64[on8][t.oncursor]
			eof := t.forward(len(t.limit64[on8]))
			return reverse64(v, on8 + 1), eof
	}
}

func (t *Light) LongestLength() int { // the length of the longest key
	var run int
	for run=7; run>=0; run-- {
		if len(t.limit64[run]) > 0 {
			return 57 + run
		}
	}
	for run=7; run>=0; run-- {
		if len(t.limit56[run]) > 0 {
			return 49 + run
		}
	}
	for run=7; run>=0; run-- {
		if len(t.limit48[run]) > 0 {
			return 41 + run
		}
	}
	for run=7; run>=0; run-- {
		if len(t.limit40[run]) > 0 {
			return 33 + run
		}
	}
	for run=7; run>=0; run-- {
		if len(t.limit32[run]) > 0 {
			return 25 + run
		}
	}
	for run=7; run>=0; run-- {
		if len(t.limit24[run]) > 0 {
			return 17 + run
		}
	}
	for run=7; run>=0; run-- {
		if len(t.limit16[run]) > 0 {
			return 9 + run
		}
	}
	for run=7; run>=0; run-- {
		if len(t.limit8[run]) > 0 {
			return 1 + run
		}
	}
	return 0
}

func (t *Light) Keys() [][]byte {

	var on, run int
	keys := make([][]byte, t.total)
	
	for run=0; run<8; run++ {
		for _, v := range t.limit8[run] {
			keys[on] = reverse8(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit16[run] {
			keys[on] = reverse16(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit24[run] {
			keys[on] = reverse24(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit32[run] {
			keys[on] = reverse32(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit40[run] {
			keys[on] = reverse40(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit48[run] {
			keys[on] = reverse48(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit56[run] {
			keys[on] = reverse56(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit64[run] {
			keys[on] = reverse64(v, run + 1)
			on++
		}
	}
	
	return keys
}

func (t *Light) Write(w custom.Interface) {
	var i, run int

	// Write total
	w.WriteUint64Variable(uint64(t.total))
	
	// Write count
	for i=0; i<64; i++ {
		w.WriteUint64Variable(uint64(t.count[i]))
	}
	
	// Write t.limit8
	for run=0; run<8; run++ {
		tmp := t.limit8[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v)
		}
	}
	// Write t.limit16
	for run=0; run<8; run++ {
		tmp := t.limit16[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
		}
	}
	// Write t.limit24
	for run=0; run<8; run++ {
		tmp := t.limit24[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
		}
	}
	// Write t.limit32
	for run=0; run<8; run++ {
		tmp := t.limit32[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
		}
	}
	// Write t.limit40
	for run=0; run<8; run++ {
		tmp := t.limit40[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
			w.WriteUint64(v[4])
		}
	}
	// Write t.limit48
	for run=0; run<8; run++ {
		tmp := t.limit48[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
			w.WriteUint64(v[4])
			w.WriteUint64(v[5])
		}
	}
	// Write t.limit56
	for run=0; run<8; run++ {
		tmp := t.limit56[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
			w.WriteUint64(v[4])
			w.WriteUint64(v[5])
			w.WriteUint64(v[6])
		}
	}
	// Write t.limit64
	for run=0; run<8; run++ {
		tmp := t.limit64[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
			w.WriteUint64(v[4])
			w.WriteUint64(v[5])
			w.WriteUint64(v[6])
			w.WriteUint64(v[7])
		}
	}
	
}

func (t *Light) Read(r *custom.Reader) {
	var run int
	var i, l, a, b, c, d, e, f, g, h uint64

	// Write total
	t.total = int(r.ReadUint64Variable())
	
	// Read count
	for i=0; i<64; i++ {
		t.count[i] = int(r.ReadUint64Variable())
	}
	
	// Read t.limit8
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([]uint64, l)
		for i=0; i<l; i++ {
			tmp[i] = r.ReadUint64()
		}
		t.limit8[run] = tmp
	}
	// Read t.limit16
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][2]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			tmp[i] = [2]uint64{a, b}
		}
		t.limit16[run] = tmp
	}
	// Read t.limit24
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][3]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			tmp[i] = [3]uint64{a, b, c}
		}
		t.limit24[run] = tmp
	}
	// Read t.limit32
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][4]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			tmp[i] = [4]uint64{a, b, c, d}
		}
		t.limit32[run] = tmp
	}
	// Read t.limit40
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][5]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			e = r.ReadUint64()
			tmp[i] = [5]uint64{a, b, c, d, e}
		}
		t.limit40[run] = tmp
	}
	// Read t.limit48
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][6]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			e = r.ReadUint64()
			f = r.ReadUint64()
			tmp[i] = [6]uint64{a, b, c, d, e, f}
		}
		t.limit48[run] = tmp
	}
	// Read t.limit56
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][7]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			e = r.ReadUint64()
			f = r.ReadUint64()
			g = r.ReadUint64()
			tmp[i] = [7]uint64{a, b, c, d, e, f, g}
		}
		t.limit56[run] = tmp
	}
	// Read t.limit64
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][8]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			e = r.ReadUint64()
			f = r.ReadUint64()
			g = r.ReadUint64()
			h = r.ReadUint64()
			tmp[i] = [8]uint64{a, b, c, d, e, f, g, h}
		}
		t.limit64[run] = tmp
	}
}

// ---------- Counter ----------
// Counter bytes has around 2KB of memory overhead for the structures, beyond this it stores all keys as efficiently as possible.

// Add this to any struct to make it binary searchable.
type Counter struct {
 limit8 [8][][2]uint64 // where len(word) <= 8
 limit16 [8][][3]uint64
 limit24 [8][][4]uint64
 limit32 [8][][5]uint64
 limit40 [8][][6]uint64
 limit48 [8][][7]uint64
 limit56 [8][][8]uint64
 limit64 [8][][9]uint64
 total int
// Used for iterating through all of it
 onlimit int
 on8 int
 oncursor int
}

func (t *Counter) Convert() *Light {
	obj := new(Light)
	obj.total = t.total
	var run int
	for run=0; run<8; run++ {
		tmp := t.limit8[run]
		cpy := make([]uint64, len(tmp))
		for i, v := range tmp {
			cpy[i] = v[0]
		}
		obj.limit8[run] = cpy
		obj.count[run + 1] = len(cpy)
	}
	for run=0; run<8; run++ {
		tmp := t.limit16[run]
		cpy := make([][2]uint64, len(tmp))
		for i, v := range tmp {
			cpy[i][0] = v[0]
			cpy[i][1] = v[1]
		}
		obj.limit16[run] = cpy
		obj.count[run + 9] = len(cpy)
	}
	for run=0; run<8; run++ {
		tmp := t.limit24[run]
		cpy := make([][3]uint64, len(tmp))
		for i, v := range tmp {
			cpy[i][0] = v[0]
			cpy[i][1] = v[1]
			cpy[i][2] = v[2]
		}
		obj.limit24[run] = cpy
		obj.count[run + 17] = len(cpy)
	}
	for run=0; run<8; run++ {
		tmp := t.limit32[run]
		cpy := make([][4]uint64, len(tmp))
		for i, v := range tmp {
			cpy[i][0] = v[0]
			cpy[i][1] = v[1]
			cpy[i][2] = v[2]
			cpy[i][3] = v[3]
		}
		obj.limit32[run] = cpy
		obj.count[run + 25] = len(cpy)
	}
	for run=0; run<8; run++ {
		tmp := t.limit40[run]
		cpy := make([][5]uint64, len(tmp))
		for i, v := range tmp {
			cpy[i][0] = v[0]
			cpy[i][1] = v[1]
			cpy[i][2] = v[2]
			cpy[i][3] = v[3]
			cpy[i][4] = v[4]
		}
		obj.limit40[run] = cpy
		obj.count[run + 33] = len(cpy)
	}
	for run=0; run<8; run++ {
		tmp := t.limit48[run]
		cpy := make([][6]uint64, len(tmp))
		for i, v := range tmp {
			cpy[i][0] = v[0]
			cpy[i][1] = v[1]
			cpy[i][2] = v[2]
			cpy[i][3] = v[3]
			cpy[i][4] = v[4]
			cpy[i][5] = v[5]
		}
		obj.limit48[run] = cpy
		obj.count[run + 41] = len(cpy)
	}
	for run=0; run<8; run++ {
		tmp := t.limit56[run]
		cpy := make([][7]uint64, len(tmp))
		for i, v := range tmp {
			cpy[i][0] = v[0]
			cpy[i][1] = v[1]
			cpy[i][2] = v[2]
			cpy[i][3] = v[3]
			cpy[i][4] = v[4]
			cpy[i][5] = v[5]
			cpy[i][6] = v[6]
		}
		obj.limit56[run] = cpy
		obj.count[run + 49] = len(cpy)
	}
	for run=0; run<8; run++ {
		tmp := t.limit64[run]
		cpy := make([][8]uint64, len(tmp))
		for i, v := range tmp {
			cpy[i][0] = v[0]
			cpy[i][1] = v[1]
			cpy[i][2] = v[2]
			cpy[i][3] = v[3]
			cpy[i][4] = v[4]
			cpy[i][5] = v[5]
			cpy[i][6] = v[6]
			cpy[i][7] = v[7]
		}
		obj.limit64[run] = cpy
		if run < 7 {
			obj.count[run + 57] = len(cpy)
		}
	}
	// Correct all the counts
	for run=2; run<64; run++ {
		obj.count[run] += obj.count[run-1]
	}
	return obj
}

func (t *Counter) Len() int {
	return t.total
}

// Find returns the index based on the key.
func (t *Counter) Find(key []byte) (int, bool) {
	
	var at, min int
	var compare uint64
	switch (len(key) - 1) / 8 {
	
		case 0: // 0 - 8 bytes
			a, l := bytes2uint64(key)
			cur := t.limit8[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				return int(cur[at][1]), true // found
			}
			return 0, false // doesn't exist
			
		case 1: // 9 - 16 bytes
			a, _ := bytes2uint64(key)
			b, l := bytes2uint64(key[8:])
			cur := t.limit16[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				return int(cur[at][2]), true // found
			}
			return 0, false // doesn't exist
			
		case 2: // 17 - 24 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, l := bytes2uint64(key[16:])
			cur := t.limit24[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				return int(cur[at][3]), true // found
			}
			return 0, false // doesn't exist
			
		case 3: // 25 - 32 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, l := bytes2uint64(key[24:])
			cur := t.limit32[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				return int(cur[at][4]), true // found
			}
			return 0, false // doesn't exist
			
		case 4: // 33 - 40 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, l := bytes2uint64(key[32:])
			cur := t.limit40[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				return int(cur[at][5]), true // found
			}
			return 0, false // doesn't exist
			
		case 5: // 41 - 48 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, l := bytes2uint64(key[40:])
			cur := t.limit48[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				return int(cur[at][6]), true // found
			}
			return 0, false // doesn't exist
			
		case 6: // 49 - 56 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, l := bytes2uint64(key[48:])
			cur := t.limit56[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][6]; g < compare {
					max = at - 1
					continue
				}
				if g > compare {
					min = at + 1
					continue
				}
				return int(cur[at][7]), true // found
			}
			return 0, false // doesn't exist
			
		case 7: // 57 - 64 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, _ := bytes2uint64(key[48:])
			h, l := bytes2uint64(key[56:])
			cur := t.limit64[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][6]; g < compare {
					max = at - 1
					continue
				}
				if g > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][7]; h < compare {
					max = at - 1
					continue
				}
				if h > compare {
					min = at + 1
					continue
				}
				return int(cur[at][8]), true // found
			}
			return 0, false // doesn't exist
		
		default: // > 64 bytes
			return t.total, false
	}
}

// Modifies the value of the key by running it through the provided function.
func (t *Counter) Update(key []byte, fn func(int) int) bool {
	
	var at, min int
	var compare uint64
	switch (len(key) - 1) / 8 {
	
		case 0: // 0 - 8 bytes
			a, l := bytes2uint64(key)
			cur := t.limit8[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				cur[at][1] = uint64(fn(int(cur[at][1])))
				return true // found
			}
			return false // doesn't exist
			
		case 1: // 9 - 16 bytes
			a, _ := bytes2uint64(key)
			b, l := bytes2uint64(key[8:])
			cur := t.limit16[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				cur[at][2] = uint64(fn(int(cur[at][2])))
				return true // found
			}
			return false // doesn't exist
			
		case 2: // 17 - 24 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, l := bytes2uint64(key[16:])
			cur := t.limit24[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				cur[at][3] = uint64(fn(int(cur[at][3])))
				return true // found
			}
			return false // doesn't exist
			
		case 3: // 25 - 32 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, l := bytes2uint64(key[24:])
			cur := t.limit32[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				cur[at][4] = uint64(fn(int(cur[at][4])))
				return true // found
			}
			return false // doesn't exist
			
		case 4: // 33 - 40 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, l := bytes2uint64(key[32:])
			cur := t.limit40[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				cur[at][5] = uint64(fn(int(cur[at][5])))
				return true // found
			}
			return false // doesn't exist
			
		case 5: // 41 - 48 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, l := bytes2uint64(key[40:])
			cur := t.limit48[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				cur[at][6] = uint64(fn(int(cur[at][6])))
				return true // found
			}
			return false // doesn't exist
			
		case 6: // 49 - 56 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, l := bytes2uint64(key[48:])
			cur := t.limit56[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][6]; g < compare {
					max = at - 1
					continue
				}
				if g > compare {
					min = at + 1
					continue
				}
				cur[at][7] = uint64(fn(int(cur[at][7])))
				return true // found
			}
			return false // doesn't exist
			
		case 7: // 57 - 64 bytes
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, _ := bytes2uint64(key[48:])
			h, l := bytes2uint64(key[56:])
			cur := t.limit64[l]
			max := len(cur) - 1
			for min <= max {
				at = min + ((max - min) / 2)
				if compare = cur[at][0]; a < compare {
					max = at - 1
					continue
				}
				if a > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][1]; b < compare {
					max = at - 1
					continue
				}
				if b > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][2]; c < compare {
					max = at - 1
					continue
				}
				if c > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][3]; d < compare {
					max = at - 1
					continue
				}
				if d > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][4]; e < compare {
					max = at - 1
					continue
				}
				if e > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][5]; f < compare {
					max = at - 1
					continue
				}
				if f > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][6]; g < compare {
					max = at - 1
					continue
				}
				if g > compare {
					min = at + 1
					continue
				}
				if compare = cur[at][7]; h < compare {
					max = at - 1
					continue
				}
				if h > compare {
					min = at + 1
					continue
				}
				cur[at][8] = uint64(fn(int(cur[at][8])))
				return true // found
			}
			return false // doesn't exist
		
		default: // > 64 bytes
			return false
	}
}

// Modifies all values by running each through the provided function.
func (t *Counter) UpdateAll(fn func(int) int) {
	var run, l, i int
	for run=0; run<8; run++ {
		tmp := t.limit8[run]
		l = len(tmp)
		for i=0; i<l; i++ {
			tmp[i][1] = uint64(fn(int(tmp[i][1])))
		}
	}
	for run=0; run<8; run++ {
		tmp := t.limit16[run]
		l = len(tmp)
		for i=0; i<l; i++ {
			tmp[i][2] = uint64(fn(int(tmp[i][2])))
		}
	}
	for run=0; run<8; run++ {
		tmp := t.limit24[run]
		l = len(tmp)
		for i=0; i<l; i++ {
			tmp[i][3] = uint64(fn(int(tmp[i][3])))
		}
	}
	for run=0; run<8; run++ {
		tmp := t.limit32[run]
		l = len(tmp)
		for i=0; i<l; i++ {
			tmp[i][4] = uint64(fn(int(tmp[i][4])))
		}
	}
	for run=0; run<8; run++ {
		tmp := t.limit40[run]
		l = len(tmp)
		for i=0; i<l; i++ {
			tmp[i][5] = uint64(fn(int(tmp[i][5])))
		}
	}
	for run=0; run<8; run++ {
		tmp := t.limit48[run]
		l = len(tmp)
		for i=0; i<l; i++ {
			tmp[i][6] = uint64(fn(int(tmp[i][6])))
		}
	}
	for run=0; run<8; run++ {
		tmp := t.limit56[run]
		l = len(tmp)
		for i=0; i<l; i++ {
			tmp[i][7] = uint64(fn(int(tmp[i][7])))
		}
	}
	for run=0; run<8; run++ {
		tmp := t.limit64[run]
		l = len(tmp)
		for i=0; i<l; i++ {
			tmp[i][8] = uint64(fn(int(tmp[i][8])))
		}
	}
}

// Add adds this key to the end of the index for later building with Build.
func (t *Counter) Add(key []byte, theval int) error {
	switch (len(key) - 1) / 8 {
		case 0:
			a, i := bytes2uint64(key)
			t.limit8[i] = append(t.limit8[i], [2]uint64{a, uint64(theval)})
			t.total++
			return nil
		case 1:
			a, _ := bytes2uint64(key)
			b, i := bytes2uint64(key[8:])
			t.limit16[i] = append(t.limit16[i], [3]uint64{a, b, uint64(theval)})
			t.total++
			return nil
		case 2:
			
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, i := bytes2uint64(key[16:])
			t.limit24[i] = append(t.limit24[i], [4]uint64{a, b, c, uint64(theval)})
			t.total++
			return nil
		case 3:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, i := bytes2uint64(key[24:])
			t.limit32[i] = append(t.limit32[i], [5]uint64{a, b, c, d, uint64(theval)})
			t.total++
			return nil
		case 4:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, i := bytes2uint64(key[32:])
			t.limit40[i] = append(t.limit40[i], [6]uint64{a, b, c, d, e, uint64(theval)})
			t.total++
			return nil
		case 5:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, i := bytes2uint64(key[40:])
			t.limit48[i] = append(t.limit48[i], [7]uint64{a, b, c, d, e, f, uint64(theval)})
			t.total++
			return nil
		case 6:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, i := bytes2uint64(key[48:])
			t.limit56[i] = append(t.limit56[i], [8]uint64{a, b, c, d, e, f, g, uint64(theval)})
			t.total++
			return nil
		case 7:
			a, _ := bytes2uint64(key)
			b, _ := bytes2uint64(key[8:])
			c, _ := bytes2uint64(key[16:])
			d, _ := bytes2uint64(key[24:])
			e, _ := bytes2uint64(key[32:])
			f, _ := bytes2uint64(key[40:])
			g, _ := bytes2uint64(key[48:])
			h, i := bytes2uint64(key[56:])
			t.limit64[i] = append(t.limit64[i], [9]uint64{a, b, c, d, e, f, g, h, uint64(theval)})
			t.total++
			return nil
		default:
			return errors.New(`Maximum key length is 64 bytes`)
	}
}

func (t *Counter) Build() {

	var l, run, n, total, on int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {
			var temp sortLimitVal8.Slice = t.limit8[run]
			sortLimitVal8.Asc(temp)
			this := temp[0]
			n = int(temp[0][1])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] {
					n += int(k[1])
				} else {
					this[1] = uint64(n)
					temp[on] = this
					on++
					this = k
					n = int(k[1])
				}
			}
			this[1] = uint64(n)
			temp[on] = this
			on++
			t.limit8[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {
			var temp sortLimitVal16.Slice = t.limit16[run]
			sortLimitVal16.Asc(temp)
			this := temp[0]
			n = int(temp[0][2])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] {
					n += int(k[2])
				} else {
					this[2] = uint64(n)
					temp[on] = this
					on++
					this = k
					n = int(k[2])
				}
			}
			this[2] = uint64(n)
			temp[on] = this
			on++
			t.limit16[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {
			var temp sortLimitVal24.Slice = t.limit24[run]
			sortLimitVal24.Asc(temp)
			this := temp[0]
			n = int(temp[0][3])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] {
					n += int(k[3])
				} else {
					this[3] = uint64(n)
					temp[on] = this
					on++
					this = k
					n = int(k[3])
				}
			}
			this[3] = uint64(n)
			temp[on] = this
			on++
			t.limit24[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {
			var temp sortLimitVal32.Slice = t.limit32[run]
			sortLimitVal32.Asc(temp)
			this := temp[0]
			n = int(temp[0][4])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] {
					n += int(k[4])
				} else {
					this[4] = uint64(n)
					temp[on] = this
					on++
					this = k
					n = int(k[4])
				}
			}
			this[4] = uint64(n)
			temp[on] = this
			on++
			t.limit32[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {
			var temp sortLimitVal40.Slice = t.limit40[run]
			sortLimitVal40.Asc(temp)
			this := temp[0]
			n = int(temp[0][5])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] {
					n += int(k[5])
				} else {
					this[5] = uint64(n)
					temp[on] = this
					on++
					this = k
					n = int(k[5])
				}
			}
			this[5] = uint64(n)
			temp[on] = this
			on++
			t.limit40[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {
			var temp sortLimitVal48.Slice = t.limit48[run]
			sortLimitVal48.Asc(temp)
			this := temp[0]
			n = int(temp[0][6])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] {
					n += int(k[6])
				} else {
					this[6] = uint64(n)
					temp[on] = this
					on++
					this = k
					n = int(k[6])
				}
			}
			this[6] = uint64(n)
			temp[on] = this
			on++
			t.limit48[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {
			var temp sortLimitVal56.Slice = t.limit56[run]
			sortLimitVal56.Asc(temp)
			this := temp[0]
			n = int(temp[0][7])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] {
					n += int(k[7])
				} else {
					this[7] = uint64(n)
					temp[on] = this
					on++
					this = k
					n = int(k[7])
				}
			}
			this[7] = uint64(n)
			temp[on] = this
			on++
			t.limit56[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {
			var temp sortLimitVal64.Slice = t.limit64[run]
			sortLimitVal64.Asc(temp)
			this := temp[0]
			n = int(temp[0][8])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] && k[7] == this[7] {
					n += int(k[8])
				} else {
					this[8] = uint64(n)
					temp[on] = this
					on++
					this = k
					n = int(k[8])
				}
			}
			this[8] = uint64(n)
			temp[on] = this
			on++
			t.limit64[run] = temp[0:on]
			total += on
		}
	}
	t.total = total
	
}

func (t *Counter) Build_Multithreaded() {

	var wg sync.WaitGroup
	var l, run int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal8.Slice = t.limit8[run]
				sortLimitVal8.Asc(temp)
				this := temp[0]
				n = int(temp[0][1])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] {
						n += int(k[1])
					} else {
						this[1] = uint64(n)
						temp[on] = this
						on++
						this = k
						n = int(k[1])
					}
				}
				this[1] = uint64(n)
				temp[on] = this
				on++
				t.limit8[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal16.Slice = t.limit16[run]
				sortLimitVal16.Asc(temp)
				this := temp[0]
				n = int(temp[0][2])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] {
						n += int(k[2])
					} else {
						this[2] = uint64(n)
						temp[on] = this
						on++
						this = k
						n = int(k[2])
					}
				}
				this[2] = uint64(n)
				temp[on] = this
				on++
				t.limit16[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal24.Slice = t.limit24[run]
				sortLimitVal24.Asc(temp)
				this := temp[0]
				n = int(temp[0][3])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] {
						n += int(k[3])
					} else {
						this[3] = uint64(n)
						temp[on] = this
						on++
						this = k
						n = int(k[3])
					}
				}
				this[3] = uint64(n)
				temp[on] = this
				on++
				t.limit24[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal32.Slice = t.limit32[run]
				sortLimitVal32.Asc(temp)
				this := temp[0]
				n = int(temp[0][4])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] {
						n += int(k[4])
					} else {
						this[4] = uint64(n)
						temp[on] = this
						on++
						this = k
						n = int(k[4])
					}
				}
				this[4] = uint64(n)
				temp[on] = this
				on++
				t.limit32[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal40.Slice = t.limit40[run]
				sortLimitVal40.Asc(temp)
				this := temp[0]
				n = int(temp[0][5])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] {
						n += int(k[5])
					} else {
						this[5] = uint64(n)
						temp[on] = this
						on++
						this = k
						n = int(k[5])
					}
				}
				this[5] = uint64(n)
				temp[on] = this
				on++
				t.limit40[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal48.Slice = t.limit48[run]
				sortLimitVal48.Asc(temp)
				this := temp[0]
				n = int(temp[0][6])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] {
						n += int(k[6])
					} else {
						this[6] = uint64(n)
						temp[on] = this
						on++
						this = k
						n = int(k[6])
					}
				}
				this[6] = uint64(n)
				temp[on] = this
				on++
				t.limit48[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal56.Slice = t.limit56[run]
				sortLimitVal56.Asc(temp)
				this := temp[0]
				n = int(temp[0][7])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] {
						n += int(k[7])
					} else {
						this[7] = uint64(n)
						temp[on] = this
						on++
						this = k
						n = int(k[7])
					}
				}
				this[7] = uint64(n)
				temp[on] = this
				on++
				t.limit56[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal64.Slice = t.limit64[run]
				sortLimitVal64.Asc(temp)
				this := temp[0]
				n = int(temp[0][8])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] && k[7] == this[7] {
						n += int(k[8])
					} else {
						this[8] = uint64(n)
						temp[on] = this
						on++
						this = k
						n = int(k[8])
					}
				}
				this[8] = uint64(n)
				temp[on] = this
				on++
				t.limit64[run] = temp[0:on]
			}(run)

		}
	}

	wg.Wait()

	var total int
	for run=0; run<8; run++ {
		total += len(t.limit8[run])
		total += len(t.limit16[run])
		total += len(t.limit24[run])
		total += len(t.limit32[run])
		total += len(t.limit40[run])
		total += len(t.limit48[run])
		total += len(t.limit56[run])
		total += len(t.limit64[run])
	}
	t.total = total
	
}

func (t *Counter) Build_With_Min(min int) {

	var l, run, n, total, on int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {
			var temp sortLimitVal8.Slice = t.limit8[run]
			sortLimitVal8.Asc(temp)
			this := temp[0]
			n = int(temp[0][1])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] {
					n += int(k[1])
				} else {
					if n >= min {
						this[1] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[1])
				}
			}
			if n >= min {
				this[1] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit8[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {
			var temp sortLimitVal16.Slice = t.limit16[run]
			sortLimitVal16.Asc(temp)
			this := temp[0]
			n = int(temp[0][2])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] {
					n += int(k[2])
				} else {
					if n >= min {
						this[2] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[2])
				}
			}
			if n >= min {
				this[2] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit16[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {
			var temp sortLimitVal24.Slice = t.limit24[run]
			sortLimitVal24.Asc(temp)
			this := temp[0]
			n = int(temp[0][3])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] {
					n += int(k[3])
				} else {
						if n >= min {
						this[3] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[3])
				}
			}
			if n >= min {
				this[3] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit24[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {
			var temp sortLimitVal32.Slice = t.limit32[run]
			sortLimitVal32.Asc(temp)
			this := temp[0]
			n = int(temp[0][4])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] {
					n += int(k[4])
				} else {
					if n >= min {
						this[4] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[4])
				}
			}
			if n >= min {
				this[4] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit32[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {
			var temp sortLimitVal40.Slice = t.limit40[run]
			sortLimitVal40.Asc(temp)
			this := temp[0]
			n = int(temp[0][5])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] {
					n += int(k[5])
				} else {
					if n >= min {
						this[5] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[5])
				}
			}
			if n >= min {
				this[5] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit40[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {
			var temp sortLimitVal48.Slice = t.limit48[run]
			sortLimitVal48.Asc(temp)
			this := temp[0]
			n = int(temp[0][6])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] {
					n += int(k[6])
				} else {
					if n >= min {
						this[6] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[6])
				}
			}
			if n >= min {
				this[6] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit48[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {
			var temp sortLimitVal56.Slice = t.limit56[run]
			sortLimitVal56.Asc(temp)
			this := temp[0]
			n = int(temp[0][7])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] {
					n += int(k[7])
				} else {
					if n >= min {
						this[7] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[7])
				}
			}
			if n >= min {
				this[7] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit56[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {
			var temp sortLimitVal64.Slice = t.limit64[run]
			sortLimitVal64.Asc(temp)
			this := temp[0]
			n = int(temp[0][8])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] && k[7] == this[7] {
					n += int(k[8])
				} else {
					if n >= min {
						this[8] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[8])
				}
			}
			if n >= min {
				this[8] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit64[run] = temp[0:on]
			total += on
		}
	}
	t.total = total
	
}

func (t *Counter) Build_With_Min_Multithreaded(min int) {

	var wg sync.WaitGroup
	var l, run int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal8.Slice = t.limit8[run]
				sortLimitVal8.Asc(temp)
				this := temp[0]
				n = int(temp[0][1])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] {
						n += int(k[1])
					} else {
						if n >= min {
							this[1] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[1])
					}
				}
				if n >= min {
					this[1] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit8[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal16.Slice = t.limit16[run]
				sortLimitVal16.Asc(temp)
				this := temp[0]
				n = int(temp[0][2])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] {
						n += int(k[2])
					} else {
						if n >= min {
							this[2] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[2])
					}
				}
				if n >= min {
					this[2] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit16[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal24.Slice = t.limit24[run]
				sortLimitVal24.Asc(temp)
				this := temp[0]
				n = int(temp[0][3])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] {
						n += int(k[3])
					} else {
							if n >= min {
							this[3] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[3])
					}
				}
				if n >= min {
					this[3] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit24[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal32.Slice = t.limit32[run]
				sortLimitVal32.Asc(temp)
				this := temp[0]
				n = int(temp[0][4])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] {
						n += int(k[4])
					} else {
						if n >= min {
							this[4] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[4])
					}
				}
				if n >= min {
					this[4] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit32[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal40.Slice = t.limit40[run]
				sortLimitVal40.Asc(temp)
				this := temp[0]
				n = int(temp[0][5])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] {
						n += int(k[5])
					} else {
						if n >= min {
							this[5] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[5])
					}
				}
				if n >= min {
					this[5] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit40[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal48.Slice = t.limit48[run]
				sortLimitVal48.Asc(temp)
				this := temp[0]
				n = int(temp[0][6])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] {
						n += int(k[6])
					} else {
						if n >= min {
							this[6] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[6])
					}
				}
				if n >= min {
					this[6] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit48[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal56.Slice = t.limit56[run]
				sortLimitVal56.Asc(temp)
				this := temp[0]
				n = int(temp[0][7])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] {
						n += int(k[7])
					} else {
						if n >= min {
							this[7] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[7])
					}
				}
				if n >= min {
					this[7] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit56[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal64.Slice = t.limit64[run]
				sortLimitVal64.Asc(temp)
				this := temp[0]
				n = int(temp[0][8])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] && k[7] == this[7] {
						n += int(k[8])
					} else {
						if n >= min {
							this[8] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[8])
					}
				}
				if n >= min {
					this[8] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit64[run] = temp[0:on]
			}(run)

		}
	}

	wg.Wait()

	var total int
	for run=0; run<8; run++ {
		total += len(t.limit8[run])
		total += len(t.limit16[run])
		total += len(t.limit24[run])
		total += len(t.limit32[run])
		total += len(t.limit40[run])
		total += len(t.limit48[run])
		total += len(t.limit56[run])
		total += len(t.limit64[run])
	}
	t.total = total
	
}

func (t *Counter) Build_With_Filter(filter filterFunc) {

	var l, run, n, total, on int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {
			var temp sortLimitVal8.Slice = t.limit8[run]
			sortLimitVal8.Asc(temp)
			this := temp[0]
			n = int(temp[0][1])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] {
					n += int(k[1])
				} else {
					if filter(reverse8b(k, run + 1)) {
						this[1] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[1])
				}
			}
			if filter(reverse8b(this, run + 1)) {
				this[1] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit8[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {
			var temp sortLimitVal16.Slice = t.limit16[run]
			sortLimitVal16.Asc(temp)
			this := temp[0]
			n = int(temp[0][2])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] {
					n += int(k[2])
				} else {
					if filter(reverse16b(k, run + 1)) {
						this[2] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[2])
				}
			}
			if filter(reverse16b(this, run + 1)) {
				this[2] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit16[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {
			var temp sortLimitVal24.Slice = t.limit24[run]
			sortLimitVal24.Asc(temp)
			this := temp[0]
			n = int(temp[0][3])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] {
					n += int(k[3])
				} else {
						if filter(reverse24b(k, run + 1)) {
						this[3] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[3])
				}
			}
			if filter(reverse24b(this, run + 1)) {
				this[3] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit24[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {
			var temp sortLimitVal32.Slice = t.limit32[run]
			sortLimitVal32.Asc(temp)
			this := temp[0]
			n = int(temp[0][4])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] {
					n += int(k[4])
				} else {
					if filter(reverse32b(k, run + 1)) {
						this[4] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[4])
				}
			}
			if filter(reverse32b(this, run + 1)) {
				this[4] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit32[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {
			var temp sortLimitVal40.Slice = t.limit40[run]
			sortLimitVal40.Asc(temp)
			this := temp[0]
			n = int(temp[0][5])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] {
					n += int(k[5])
				} else {
					if filter(reverse40b(k, run + 1)) {
						this[5] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[5])
				}
			}
			if filter(reverse40b(this, run + 1)) {
				this[5] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit40[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {
			var temp sortLimitVal48.Slice = t.limit48[run]
			sortLimitVal48.Asc(temp)
			this := temp[0]
			n = int(temp[0][6])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] {
					n += int(k[6])
				} else {
					if filter(reverse48b(k, run + 1)) {
						this[6] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[6])
				}
			}
			if filter(reverse48b(this, run + 1)) {
				this[6] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit48[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {
			var temp sortLimitVal56.Slice = t.limit56[run]
			sortLimitVal56.Asc(temp)
			this := temp[0]
			n = int(temp[0][7])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] {
					n += int(k[7])
				} else {
					if filter(reverse56b(k, run + 1)) {
						this[7] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[7])
				}
			}
			if filter(reverse56b(this, run + 1)) {
				this[7] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit56[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {
			var temp sortLimitVal64.Slice = t.limit64[run]
			sortLimitVal64.Asc(temp)
			this := temp[0]
			n = int(temp[0][8])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] && k[7] == this[7] {
					n += int(k[8])
				} else {
					if filter(reverse64b(k, run + 1)) {
						this[8] = uint64(n)
						temp[on] = this
						on++
					}
					this = k
					n = int(k[8])
				}
			}
			if filter(reverse64b(this, run + 1)) {
				this[8] = uint64(n)
				temp[on] = this
				on++
			}
			t.limit64[run] = temp[0:on]
			total += on
		}
	}
	t.total = total
	
}


func (t *Counter) Build_With_Min_Filter(min int, filter filterFunc) {

	var l, run, n, total, on int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {
			var temp sortLimitVal8.Slice = t.limit8[run]
			sortLimitVal8.Asc(temp)
			this := temp[0]
			n = int(temp[0][1])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] {
					n += int(k[1])
				} else {
					if n >= min {
						if filter(reverse8b(k, run + 1)) {
							this[1] = uint64(n)
							temp[on] = this
							on++
						}
					}
					this = k
					n = int(k[1])
				}
			}
			if n >= min {
				if filter(reverse8b(this, run + 1)) {
					this[1] = uint64(n)
					temp[on] = this
					on++
				}
			}
			t.limit8[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {
			var temp sortLimitVal16.Slice = t.limit16[run]
			sortLimitVal16.Asc(temp)
			this := temp[0]
			n = int(temp[0][2])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] {
					n += int(k[2])
				} else {
					if n >= min {
						if filter(reverse16b(k, run + 1)) {
							this[2] = uint64(n)
							temp[on] = this
							on++
						}
					}
					this = k
					n = int(k[2])
				}
			}
			if n >= min {
				if filter(reverse16b(this, run + 1)) {
					this[2] = uint64(n)
					temp[on] = this
					on++
				}
			}
			t.limit16[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {
			var temp sortLimitVal24.Slice = t.limit24[run]
			sortLimitVal24.Asc(temp)
			this := temp[0]
			n = int(temp[0][3])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] {
					n += int(k[3])
				} else {
					if n >= min {
						if filter(reverse24b(k, run + 1)) {
							this[3] = uint64(n)
							temp[on] = this
							on++
						}
					}
					this = k
					n = int(k[3])
				}
			}
			if n >= min {
				if filter(reverse24b(this, run + 1)) {
					this[3] = uint64(n)
					temp[on] = this
					on++
				}
			}
			t.limit24[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {
			var temp sortLimitVal32.Slice = t.limit32[run]
			sortLimitVal32.Asc(temp)
			this := temp[0]
			n = int(temp[0][4])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] {
					n += int(k[4])
				} else {
					if n >= min {
						if filter(reverse32b(k, run + 1)) {
						this[4] = uint64(n)
						temp[on] = this
						on++
						}
					}
					this = k
					n = int(k[4])
				}
			}
			if n >= min {
				if filter(reverse32b(this, run + 1)) {
					this[4] = uint64(n)
					temp[on] = this
					on++
				}
			}
			t.limit32[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {
			var temp sortLimitVal40.Slice = t.limit40[run]
			sortLimitVal40.Asc(temp)
			this := temp[0]
			n = int(temp[0][5])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] {
					n += int(k[5])
				} else {
					if n >= min {
						if filter(reverse40b(k, run + 1)) {
							this[5] = uint64(n)
							temp[on] = this
							on++
						}
					}
					this = k
					n = int(k[5])
				}
			}
			if n >= min {
				if filter(reverse40b(this, run + 1)) {
					this[5] = uint64(n)
					temp[on] = this
					on++
				}
			}
			t.limit40[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {
			var temp sortLimitVal48.Slice = t.limit48[run]
			sortLimitVal48.Asc(temp)
			this := temp[0]
			n = int(temp[0][6])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] {
					n += int(k[6])
				} else {
					if n >= min {
						if filter(reverse48b(k, run + 1)) {
							this[6] = uint64(n)
							temp[on] = this
							on++
						}
					}
					this = k
					n = int(k[6])
				}
			}
			if n >= min {
				if filter(reverse48b(this, run + 1)) {
					this[6] = uint64(n)
					temp[on] = this
					on++
				}
			}
			t.limit48[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {
			var temp sortLimitVal56.Slice = t.limit56[run]
			sortLimitVal56.Asc(temp)
			this := temp[0]
			n = int(temp[0][7])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] {
					n += int(k[7])
				} else {
					if n >= min {
						if filter(reverse56b(k, run + 1)) {
							this[7] = uint64(n)
							temp[on] = this
							on++
						}
					}
					this = k
					n = int(k[7])
				}
			}
			if n >= min {
				if filter(reverse56b(this, run + 1)) {
					this[7] = uint64(n)
					temp[on] = this
					on++
				}
			}
			t.limit56[run] = temp[0:on]
			total += on
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {
			var temp sortLimitVal64.Slice = t.limit64[run]
			sortLimitVal64.Asc(temp)
			this := temp[0]
			n = int(temp[0][8])
			on = 0
			for _, k := range temp[1:] {
				if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] && k[7] == this[7] {
					n += int(k[8])
				} else {
					if n >= min {
						if filter(reverse64b(k, run + 1)) {
							this[8] = uint64(n)
							temp[on] = this
							on++
						}
					}
					this = k
					n = int(k[8])
				}
			}
			if n >= min {
				if filter(reverse64b(this, run + 1)) {
					this[8] = uint64(n)
					temp[on] = this
					on++
				}
			}
			t.limit64[run] = temp[0:on]
			total += on
		}
	}
	t.total = total
	
}


func (t *Counter) Build_With_Min_Filter_Multithreaded(min int, filter filterFunc) {

	var wg sync.WaitGroup
	var l, run int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal8.Slice = t.limit8[run]
				sortLimitVal8.Asc(temp)
				this := temp[0]
				n = int(temp[0][1])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] {
						n += int(k[1])
					} else {
						if n >= min {
							if filter(reverse8b(k, run + 1)) {
								this[1] = uint64(n)
								temp[on] = this
								on++
							}
						}
						this = k
						n = int(k[1])
					}
				}
				if n >= min {
					if filter(reverse8b(this, run + 1)) {
						this[1] = uint64(n)
						temp[on] = this
						on++
					}
				}
				t.limit8[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal16.Slice = t.limit16[run]
				sortLimitVal16.Asc(temp)
				this := temp[0]
				n = int(temp[0][2])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] {
						n += int(k[2])
					} else {
						if n >= min {
						if filter(reverse16b(k, run + 1)) {
							this[2] = uint64(n)
							temp[on] = this
							on++
							}
						}
						this = k
						n = int(k[2])
					}
				}
				if n >= min {
					if filter(reverse16b(this, run + 1)) {
						this[2] = uint64(n)
						temp[on] = this
						on++
					}
				}
				t.limit16[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal24.Slice = t.limit24[run]
				sortLimitVal24.Asc(temp)
				this := temp[0]
				n = int(temp[0][3])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] {
						n += int(k[3])
					} else {
							if n >= min {
						if filter(reverse24b(k, run + 1)) {
							this[3] = uint64(n)
							temp[on] = this
							on++
							}
						}
						this = k
						n = int(k[3])
					}
				}
				if n >= min {
					if filter(reverse24b(this, run + 1)) {
						this[3] = uint64(n)
						temp[on] = this
						on++
					}
				}
				t.limit24[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal32.Slice = t.limit32[run]
				sortLimitVal32.Asc(temp)
				this := temp[0]
				n = int(temp[0][4])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] {
						n += int(k[4])
					} else {
						if n >= min {
						if filter(reverse32b(k, run + 1)) {
							this[4] = uint64(n)
							temp[on] = this
							on++
							}
						}
						this = k
						n = int(k[4])
					}
				}
				if n >= min {
					if filter(reverse32b(this, run + 1)) {
						this[4] = uint64(n)
						temp[on] = this
						on++
					}
				}
				t.limit32[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal40.Slice = t.limit40[run]
				sortLimitVal40.Asc(temp)
				this := temp[0]
				n = int(temp[0][5])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] {
						n += int(k[5])
					} else {
						if n >= min {
						if filter(reverse40b(k, run + 1)) {
							this[5] = uint64(n)
							temp[on] = this
							on++
							}
						}
						this = k
						n = int(k[5])
					}
				}
				if n >= min {
					if filter(reverse40b(this, run + 1)) {
						this[5] = uint64(n)
						temp[on] = this
						on++
					}
				}
				t.limit40[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal48.Slice = t.limit48[run]
				sortLimitVal48.Asc(temp)
				this := temp[0]
				n = int(temp[0][6])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] {
						n += int(k[6])
					} else {
						if n >= min {
						if filter(reverse48b(k, run + 1)) {
							this[6] = uint64(n)
							temp[on] = this
							on++
							}
						}
						this = k
						n = int(k[6])
					}
				}
				if n >= min {
					if filter(reverse48b(this, run + 1)) {
						this[6] = uint64(n)
						temp[on] = this
						on++
					}
				}
				t.limit48[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal56.Slice = t.limit56[run]
				sortLimitVal56.Asc(temp)
				this := temp[0]
				n = int(temp[0][7])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] {
						n += int(k[7])
					} else {
						if n >= min {
						if filter(reverse56b(k, run + 1)) {
							this[7] = uint64(n)
							temp[on] = this
							on++
							}
						}
						this = k
						n = int(k[7])
					}
				}
				if n >= min {
					if filter(reverse56b(this, run + 1)) {
						this[7] = uint64(n)
						temp[on] = this
						on++
					}
				}
				t.limit56[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal64.Slice = t.limit64[run]
				sortLimitVal64.Asc(temp)
				this := temp[0]
				n = int(temp[0][8])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] && k[7] == this[7] {
						n += int(k[8])
					} else {
						if n >= min {
						if filter(reverse64b(k, run + 1)) {
							this[8] = uint64(n)
							temp[on] = this
							on++
							}
						}
						this = k
						n = int(k[8])
					}
				}
				if n >= min {
					if filter(reverse64b(this, run + 1)) {
						this[8] = uint64(n)
						temp[on] = this
						on++
					}
				}
				t.limit64[run] = temp[0:on]
			}(run)

		}
	}

	wg.Wait()

	var total int
	for run=0; run<8; run++ {
		total += len(t.limit8[run])
		total += len(t.limit16[run])
		total += len(t.limit24[run])
		total += len(t.limit32[run])
		total += len(t.limit40[run])
		total += len(t.limit48[run])
		total += len(t.limit56[run])
		total += len(t.limit64[run])
	}
	t.total = total
	
}

func (t *Counter) Build_With_Filter_Multithreaded(filter filterFunc) {

	var wg sync.WaitGroup
	var l, run int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal8.Slice = t.limit8[run]
				sortLimitVal8.Asc(temp)
				this := temp[0]
				n = int(temp[0][1])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] {
						n += int(k[1])
					} else {
						if filter(reverse8b(k, run + 1)) {
							this[1] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[1])
					}
				}
				if filter(reverse8b(this, run + 1)) {
					this[1] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit8[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal16.Slice = t.limit16[run]
				sortLimitVal16.Asc(temp)
				this := temp[0]
				n = int(temp[0][2])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] {
						n += int(k[2])
					} else {
						if filter(reverse16b(k, run + 1)) {
							this[2] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[2])
					}
				}
				if filter(reverse16b(this, run + 1)) {
					this[2] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit16[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal24.Slice = t.limit24[run]
				sortLimitVal24.Asc(temp)
				this := temp[0]
				n = int(temp[0][3])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] {
						n += int(k[3])
					} else {
							if filter(reverse24b(k, run + 1)) {
							this[3] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[3])
					}
				}
				if filter(reverse24b(this, run + 1)) {
					this[3] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit24[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal32.Slice = t.limit32[run]
				sortLimitVal32.Asc(temp)
				this := temp[0]
				n = int(temp[0][4])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] {
						n += int(k[4])
					} else {
						if filter(reverse32b(k, run + 1)) {
							this[4] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[4])
					}
				}
				if filter(reverse32b(this, run + 1)) {
					this[4] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit32[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal40.Slice = t.limit40[run]
				sortLimitVal40.Asc(temp)
				this := temp[0]
				n = int(temp[0][5])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] {
						n += int(k[5])
					} else {
						if filter(reverse40b(k, run + 1)) {
							this[5] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[5])
					}
				}
				if filter(reverse40b(this, run + 1)) {
					this[5] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit40[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal48.Slice = t.limit48[run]
				sortLimitVal48.Asc(temp)
				this := temp[0]
				n = int(temp[0][6])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] {
						n += int(k[6])
					} else {
						if filter(reverse48b(k, run + 1)) {
							this[6] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[6])
					}
				}
				if filter(reverse48b(this, run + 1)) {
					this[6] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit48[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal56.Slice = t.limit56[run]
				sortLimitVal56.Asc(temp)
				this := temp[0]
				n = int(temp[0][7])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] {
						n += int(k[7])
					} else {
						if filter(reverse56b(k, run + 1)) {
							this[7] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[7])
					}
				}
				if filter(reverse56b(this, run + 1)) {
					this[7] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit56[run] = temp[0:on]
			}(run)

		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {

			wg.Add(1)
			go func(run int) {
				defer wg.Done()
				var n, on int
				var temp sortLimitVal64.Slice = t.limit64[run]
				sortLimitVal64.Asc(temp)
				this := temp[0]
				n = int(temp[0][8])
				on = 0
				for _, k := range temp[1:] {
					if k[0] == this[0] && k[1] == this[1] && k[2] == this[2] && k[3] == this[3] && k[4] == this[4] && k[5] == this[5] && k[6] == this[6] && k[7] == this[7] {
						n += int(k[8])
					} else {
						if filter(reverse64b(k, run + 1)) {
							this[8] = uint64(n)
							temp[on] = this
							on++
						}
						this = k
						n = int(k[8])
					}
				}
				if filter(reverse64b(this, run + 1)) {
					this[8] = uint64(n)
					temp[on] = this
					on++
				}
				t.limit64[run] = temp[0:on]
			}(run)

		}
	}

	wg.Wait()

	var total int
	for run=0; run<8; run++ {
		total += len(t.limit8[run])
		total += len(t.limit16[run])
		total += len(t.limit24[run])
		total += len(t.limit32[run])
		total += len(t.limit40[run])
		total += len(t.limit48[run])
		total += len(t.limit56[run])
		total += len(t.limit64[run])
	}
	t.total = total
	
}

func (t *Counter) Optimize_With_Space() {

	var l, run int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {
			if cap(t.limit8[run]) > l * 2 {
				newkey := make([][2]uint64, l, l*2)
				copy(newkey, t.limit8[run])
				t.limit8[run] = newkey
			}
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {
			if cap(t.limit16[run]) > l * 2 {
				newkey := make([][3]uint64, l, l*2)
				copy(newkey, t.limit16[run])
				t.limit16[run] = newkey
			}
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {
			if cap(t.limit24[run]) > l * 2 {
				newkey := make([][4]uint64, l, l*2)
				copy(newkey, t.limit24[run])
				t.limit24[run] = newkey
			}
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {
			if cap(t.limit32[run]) > l * 2 {
				newkey := make([][5]uint64, l, l*2)
				copy(newkey, t.limit32[run])
				t.limit32[run] = newkey
			}
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {
			if cap(t.limit40[run]) > l * 2 {
				newkey := make([][6]uint64, l, l*2)
				copy(newkey, t.limit40[run])
				t.limit40[run] = newkey
			}
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {
			if cap(t.limit48[run]) > l * 2 {
				newkey := make([][7]uint64, l, l*2)
				copy(newkey, t.limit48[run])
				t.limit48[run] = newkey
			}
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {
			if cap(t.limit56[run]) > l * 2 {
				newkey := make([][8]uint64, l, l*2)
				copy(newkey, t.limit56[run])
				t.limit56[run] = newkey
			}
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {
			if cap(t.limit64[run]) > l * 2 {
				newkey := make([][9]uint64, l, l*2)
				copy(newkey, t.limit64[run])
				t.limit64[run] = newkey
			}
		}
	}
}

func (t *Counter) Optimize() {

	var l, run int
	
	for run=0; run<8; run++ {
		if l = len(t.limit8[run]); l > 0 {
			newkey := make([][2]uint64, l)
			copy(newkey, t.limit8[run])
			t.limit8[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit16[run]); l > 0 {
			newkey := make([][3]uint64, l)
			copy(newkey, t.limit16[run])
			t.limit16[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit24[run]); l > 0 {
			newkey := make([][4]uint64, l)
			copy(newkey, t.limit24[run])
			t.limit24[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit32[run]); l > 0 {
			newkey := make([][5]uint64, l)
			copy(newkey, t.limit32[run])
			t.limit32[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit40[run]); l > 0 {
			newkey := make([][6]uint64, l)
			copy(newkey, t.limit40[run])
			t.limit40[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit48[run]); l > 0 {
			newkey := make([][7]uint64, l)
			copy(newkey, t.limit48[run])
			t.limit48[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit56[run]); l > 0 {
			newkey := make([][8]uint64, l)
			copy(newkey, t.limit56[run])
			t.limit56[run] = newkey
		}
	}
	
	for run=0; run<8; run++ {
		if l = len(t.limit64[run]); l > 0 {
			newkey := make([][9]uint64, l)
			copy(newkey, t.limit64[run])
			t.limit64[run] = newkey
		}
	}
}

// Reset() must be called before Next(). Returns whether there are any entries.
func (t *Counter) Reset() bool {
	t.onlimit = 0
	t.on8 = 0
	t.oncursor = 0
	if len(t.limit8[0]) == 0 {
		if t.total == 0 {
			return false
		} else {
			t.forward(0)
		}
	}
	return true
}

func (t *Counter) forward(l int) bool {
	t.oncursor++
	for t.oncursor >= l {
		t.oncursor = 0
		if t.on8++; t.on8 == 8 {
			t.on8 = 0
			if t.onlimit++; t.onlimit == 8 {
				t.Reset()
				return true
			}
		}
		switch t.onlimit {
			case 0: l = len(t.limit8[t.on8])
			case 1: l = len(t.limit16[t.on8])
			case 2: l = len(t.limit24[t.on8])
			case 3: l = len(t.limit32[t.on8])
			case 4: l = len(t.limit40[t.on8])
			case 5: l = len(t.limit48[t.on8])
			case 6: l = len(t.limit56[t.on8])
			case 7: l = len(t.limit64[t.on8])
		}
	}
	return false
}

func (t *Counter) Next() ([]byte, int, bool) {
	on8 := t.on8
	switch t.onlimit {
		case 0:
			v := t.limit8[on8][t.oncursor]
			eof := t.forward(len(t.limit8[on8]))
			return reverse8b(v, on8 + 1), int(v[1]), eof
		case 1:
			v := t.limit16[on8][t.oncursor]
			eof := t.forward(len(t.limit16[on8]))
			return reverse16b(v, on8 + 1), int(v[2]), eof
		case 2:
			v := t.limit24[on8][t.oncursor]
			eof := t.forward(len(t.limit24[on8]))
			return reverse24b(v, on8 + 1), int(v[3]), eof
		case 3:
			v := t.limit32[on8][t.oncursor]
			eof := t.forward(len(t.limit32[on8]))
			return reverse32b(v, on8 + 1), int(v[4]), eof
		case 4:
			v := t.limit40[on8][t.oncursor]
			eof := t.forward(len(t.limit40[on8]))
			return reverse40b(v, on8 + 1), int(v[5]), eof
		case 5:
			v := t.limit48[on8][t.oncursor]
			eof := t.forward(len(t.limit48[on8]))
			return reverse48b(v, on8 + 1), int(v[6]), eof
		case 6:
			v := t.limit56[on8][t.oncursor]
			eof := t.forward(len(t.limit56[on8]))
			return reverse56b(v, on8 + 1), int(v[7]), eof
		default:
			v := t.limit64[on8][t.oncursor]
			eof := t.forward(len(t.limit64[on8]))
			return reverse64b(v, on8 + 1), int(v[8]), eof
	}
}

func (t *Counter) Keys() [][]byte {

	var on, run int
	keys := make([][]byte, t.total)
	
	for run=0; run<8; run++ {
		for _, v := range t.limit8[run] {
			keys[on] = reverse8b(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit16[run] {
			keys[on] = reverse16b(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit24[run] {
			keys[on] = reverse24b(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit32[run] {
			keys[on] = reverse32b(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit40[run] {
			keys[on] = reverse40b(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit48[run] {
			keys[on] = reverse48b(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit56[run] {
			keys[on] = reverse56b(v, run + 1)
			on++
		}
	}
	for run=0; run<8; run++ {
		for _, v := range t.limit64[run] {
			keys[on] = reverse64b(v, run + 1)
			on++
		}
	}
	
	return keys
}


func (t *Counter) Write(w custom.Interface) {
	var run int

	// Write total
	w.WriteUint64Variable(uint64(t.total))
	
	// Write t.limit8
	for run=0; run<8; run++ {
		tmp := t.limit8[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
		}
	}
	// Write t.limit16
	for run=0; run<8; run++ {
		tmp := t.limit16[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
		}
	}
	// Write t.limit24
	for run=0; run<8; run++ {
		tmp := t.limit24[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
		}
	}
	// Write t.limit32
	for run=0; run<8; run++ {
		tmp := t.limit32[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
			w.WriteUint64(v[4])
		}
	}
	// Write t.limit40
	for run=0; run<8; run++ {
		tmp := t.limit40[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
			w.WriteUint64(v[4])
			w.WriteUint64(v[5])
		}
	}
	// Write t.limit48
	for run=0; run<8; run++ {
		tmp := t.limit48[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
			w.WriteUint64(v[4])
			w.WriteUint64(v[5])
			w.WriteUint64(v[6])
		}
	}
	// Write t.limit56
	for run=0; run<8; run++ {
		tmp := t.limit56[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
			w.WriteUint64(v[4])
			w.WriteUint64(v[5])
			w.WriteUint64(v[6])
			w.WriteUint64(v[7])
		}
	}
	// Write t.limit64
	for run=0; run<8; run++ {
		tmp := t.limit64[run]
		w.WriteUint64Variable(uint64(len(tmp)))
		for _, v := range tmp {
			w.WriteUint64(v[0])
			w.WriteUint64(v[1])
			w.WriteUint64(v[2])
			w.WriteUint64(v[3])
			w.WriteUint64(v[4])
			w.WriteUint64(v[5])
			w.WriteUint64(v[6])
			w.WriteUint64(v[7])
			w.WriteUint64(v[8])
		}
	}
}

func (t *Counter) Read(r *custom.Reader) {
	var run int
	var i, l, a, b, c, d, e, f, g, h, z uint64

	// Write total
	t.total = int(r.ReadUint64Variable())
	
	// Read t.limit8
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][2]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			tmp[i] = [2]uint64{a, b}
		}
		t.limit8[run] = tmp
	}
	// Read t.limit16
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][3]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			tmp[i] = [3]uint64{a, b, c}
		}
		t.limit16[run] = tmp
	}
	// Read t.limit24
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][4]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			tmp[i] = [4]uint64{a, b, c, d}
		}
		t.limit24[run] = tmp
	}
	// Read t.limit32
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][5]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			e = r.ReadUint64()
			tmp[i] = [5]uint64{a, b, c, d, e}
		}
		t.limit32[run] = tmp
	}
	// Read t.limit40
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][6]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			e = r.ReadUint64()
			f = r.ReadUint64()
			tmp[i] = [6]uint64{a, b, c, d, e, f}
		}
		t.limit40[run] = tmp
	}
	// Read t.limit48
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][7]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			e = r.ReadUint64()
			f = r.ReadUint64()
			g = r.ReadUint64()
			tmp[i] = [7]uint64{a, b, c, d, e, f, g}
		}
		t.limit48[run] = tmp
	}
	// Read t.limit56
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][8]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			e = r.ReadUint64()
			f = r.ReadUint64()
			g = r.ReadUint64()
			h = r.ReadUint64()
			tmp[i] = [8]uint64{a, b, c, d, e, f, g, h}
		}
		t.limit56[run] = tmp
	}
	// Read t.limit64
	for run=0; run<8; run++ {
		l = r.ReadUint64Variable()
		tmp := make([][9]uint64, l)
		for i=0; i<l; i++ {
			a = r.ReadUint64()
			b = r.ReadUint64()
			c = r.ReadUint64()
			d = r.ReadUint64()
			e = r.ReadUint64()
			f = r.ReadUint64()
			g = r.ReadUint64()
			h = r.ReadUint64()
			z = r.ReadUint64()
			tmp[i] = [9]uint64{a, b, c, d, e, f, g, h, z}
		}
		t.limit64[run] = tmp
	}
}

