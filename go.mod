module frostdb

go 1.23

require (
	github.com/google/btree v1.1.3
	github.com/hdt3213/rdb v1.3.2
	github.com/yuin/gopher-lua v1.1.2
)

require (
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/hdt3213/rdb => ./third_party/rdb

