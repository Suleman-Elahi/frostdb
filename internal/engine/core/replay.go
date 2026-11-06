package core

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"frostdb/internal/engine/object"
)

// applyCommand replays a WAL command directly against the engine.
// This avoids importing the command/registry package (which would be circular).
// Only mutating commands need replay.
func applyCommand(e *DBView, args [][]byte, entryTimestamp int64) {
	if len(args) == 0 {
		return
	}

	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "SET":
		replaySet(e, args[1:], entryTimestamp)
	case "DEL", "DELEX":
		replayDel(e, args[1:])
	case "EXPIRE":
		replayExpire(e, args[1:], entryTimestamp, false)
	case "LPUSH", "RPUSH":
		replayPush(e, args[1:], cmd == "LPUSH")
	case "LPUSHX", "RPUSHX":
		replayPushX(e, args[1:], cmd == "LPUSHX")
	case "LPOP", "RPOP", "BLPOP", "BRPOP":
		replayPop(e, args[1:], cmd == "LPOP" || cmd == "BLPOP")
	case "LMOVE", "BLMOVE":
		replayLMove(e, args[1:])
	case "RPOPLPUSH", "BRPOPLPUSH":
		replayRPopLPush(e, args[1:])
	case "LREM":
		replayLRem(e, args[1:])
	case "LINSERT":
		replayLInsert(e, args[1:])
	case "LTRIM":
		replayLTrim(e, args[1:])
	case "ZADD":
		replayZAdd(e, args[1:])
	case "ZPOPMIN", "BZPOPMIN":
		replayZPopMin(e, args[1:])
	case "ZPOPMAX", "BZPOPMAX":
		replayZPopMax(e, args[1:])
	case "ZREM":
		replayZRem(e, args[1:])
	case "ZREMRANGEBYSCORE":
		replayZRemRangeByScore(e, args[1:])
	case "XADD":
		replayXAdd(e, args[1:])
	case "XACK":
		replayXAck(e, args[1:])
	case "XGROUP":
		replayXGroup(e, args[1:])
	case "XTRIM":
		replayXTrim(e, args[1:])
	case "XDEL":
		replayXDel(e, args[1:])
	case "XDELEX":
		replayXDelEx(e, args[1:])
	case "XACKDEL":
		replayXAckDel(e, args[1:])
	case "XSETID":
		replayXSetID(e, args[1:])
	case "XREADGROUP":
		replayXReadGroup(e, args[1:])
	case "XAUTOCLAIM":
		replayXAutoClaim(e, args[1:])
	case "XCFGSET":
		replayXCfgSet(e, args[1:])
	case "XIDMPRECORD":
		replayXIDMPRecord(e, args[1:])
	case "COPY":
		replayCopy(e, args[1:])
	case "HSET", "HMSET":
		replayHSet(e, args[1:])
	case "HINCRBY":
		replayHIncrBy(e, args[1:])
	case "HINCRBYFLOAT":
		replayHIncrByFloat(e, args[1:])
	case "HDEL":
		replayHDel(e, args[1:])
	case "SADD":
		replaySAdd(e, args[1:])
	case "SREM":
		replaySRem(e, args[1:])
	case "SPOP":
		replaySPop(e, args[1:])
	case "INCR":
		replayIncrBy(e, args[1:], 1)
	case "DECR":
		replayIncrBy(e, args[1:], -1)
	case "INCRBY":
		replayIncrByArg(e, args[1:], 1)
	case "DECRBY":
		replayIncrByArg(e, args[1:], -1)
	case "INCRBYFLOAT":
		replayIncrByFloat(e, args[1:])
	case "APPEND":
		replayAppend(e, args[1:])
	case "MSET":
		replayMSet(e, args[1:], false)
	case "MSETNX":
		replayMSet(e, args[1:], true)
	case "SETEX", "PSETEX":
		// Legacy WAL entries use their entry timestamp as the origin for a
		// relative TTL. New writes are normalized to SET ... PXAT.
		replaySetEx(e, args[1:], cmd == "PSETEX", entryTimestamp)
	case "SETNX":
		replaySetNX(e, args[1:])
	case "GETSET":
		replaySet(e, args[1:], entryTimestamp)
	case "GETDEL":
		replayDel(e, args[1:])
	case "LSET":
		replayLSet(e, args[1:])
	case "SINTERSTORE":
		replaySetStore(e, args[1:], setStoreIntersect)
	case "SUNIONSTORE":
		replaySetStore(e, args[1:], setStoreUnion)
	case "SDIFFSTORE":
		replaySetStore(e, args[1:], setStoreDifference)
	case "SMOVE":
		replaySMove(e, args[1:])
	case "RENAME", "RENAMENX":
		replayRename(e, args[1:])
	case "UNLINK":
		replayDel(e, args[1:])
	case "FLUSHDB", "FLUSHALL":
		replayFlush(e)
	case "HSETNX":
		replayHSetNX(e, args[1:])
	case "PEXPIRE":
		replayExpire(e, args[1:], entryTimestamp, true)
	case "EXPIREAT":
		replayExpireAt(e, args[1:], true)
	case "PEXPIREAT":
		replayExpireAt(e, args[1:], false)
	case "PERSIST":
		replayPersist(e, args[1:])
	case "SORT":
		replaySort(e, args[1:])
	case "HEXPIRE":
		replayHExpire(e, args[1:], entryTimestamp, 1000, false, false)
	case "HPEXPIRE":
		replayHExpire(e, args[1:], entryTimestamp, 1, false, false)
	case "HEXPIREAT":
		replayHExpire(e, args[1:], entryTimestamp, 1000, true, false)
	case "HPEXPIREAT":
		replayHExpire(e, args[1:], entryTimestamp, 1, true, false)
	case "HPERSIST":
		replayHExpire(e, args[1:], entryTimestamp, 1, false, true)
	case "HSETEX":
		replayHSetEx(e, args[1:], entryTimestamp)
	case "HGETEX":
		replayHGetEx(e, args[1:], entryTimestamp)
	case "BITOP":
		replayBitOp(e, args[1:])
	case "ARSET":
		replayArSet(e, args[1:])
	case "ARDEL":
		replayArDel(e, args[1:])
	case "ARDELRANGE":
		replayArDelRange(e, args[1:])
	case "ARMSET":
		replayArMSet(e, args[1:])
	case "ARINSERT":
		replayArInsert(e, args[1:])
	case "ARRING":
		replayArRing(e, args[1:])
	case "ARSEEK":
		replayArSeek(e, args[1:])
	}
}

func replaySet(e *DBView, args [][]byte, entryTimestamp int64) {
	if len(args) < 2 {
		return
	}

	key := string(args[0])
	val := args[1]
	var expireAt int64
	keepTTL := false
	nx := false
	xx := false
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "EX":
			if i+1 >= len(args) {
				return
			}
			ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return
			}
			expireAt = entryTimestamp + ttl*1000
			i++
		case "PX":
			if i+1 >= len(args) {
				return
			}
			ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return
			}
			expireAt = entryTimestamp + ttl
			i++
		case "EXAT":
			if i+1 >= len(args) {
				return
			}
			ts, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return
			}
			expireAt = ts * 1000
			i++
		case "PXAT":
			if i+1 >= len(args) {
				return
			}
			ts, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return
			}
			expireAt = ts
			i++
		case "KEEPTTL":
			keepTTL = true
		case "NX":
			nx = true
		case "XX":
			xx = true
		}
	}
	if current, exists := e.GetRaw(key); (nx && exists) || (xx && !exists) {
		return
	} else if keepTTL && exists {
		expireAt = current.ExpireAt
	}
	if expireAt > 0 && expireAt <= time.Now().UnixMilli() {
		e.Del(key)
		return
	}
	e.Set(key, &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    val,
		ExpireAt: expireAt,
	})
}

func replayHIncrBy(e *DBView, args [][]byte) {
	if len(args) != 3 {
		return
	}
	key := string(args[0])
	field := string(args[1])
	incrStr := string(args[2])

	incr, _ := strconv.ParseInt(incrStr, 10, 64)

	s := e.getShard(key)
	obj, ok := s.Get(key)
	var hash *object.Hash
	if ok && obj.Type == object.TypeHash {
		hash = obj.Value.(*object.Hash)
	} else {
		hash = object.NewHash()
		s.Set(key, &object.Object{
			Type:     object.TypeHash,
			Encoding: object.EncodingRaw,
			Value:    hash,
		})
	}

	valStr, exists := hash.Get(field)
	var val int64
	if exists {
		val, _ = strconv.ParseInt(valStr, 10, 64)
	}
	newVal := val + incr
	hash.Set(field, strconv.FormatInt(newVal, 10))
}

func replayHIncrByFloat(e *DBView, args [][]byte) {
	if len(args) != 3 {
		return
	}
	key := string(args[0])
	field := string(args[1])
	incrStr := string(args[2])

	incr, _ := strconv.ParseFloat(incrStr, 64)

	s := e.getShard(key)
	obj, ok := s.Get(key)
	var hash *object.Hash
	if ok && obj.Type == object.TypeHash {
		hash = obj.Value.(*object.Hash)
	} else {
		hash = object.NewHash()
		s.Set(key, &object.Object{
			Type:     object.TypeHash,
			Encoding: object.EncodingRaw,
			Value:    hash,
		})
	}

	valStr, exists := hash.Get(field)
	var val float64
	if exists {
		val, _ = strconv.ParseFloat(valStr, 64)
	}
	newVal := val + incr
	hash.Set(field, strconv.FormatFloat(newVal, 'f', -1, 64))
}

func replayDel(e *DBView, args [][]byte) {
	for _, arg := range args {
		e.Del(string(arg))
	}
}

func replayExpire(e *DBView, args [][]byte, entryTimestamp int64, milliseconds bool) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return
	}
	if !milliseconds {
		ttl *= 1000
	}
	expireAt := entryTimestamp + ttl
	applyExpireAt(e, key, expireAt)
}

func applyExpireAt(e *DBView, key string, expireAt int64) {
	if expireAt <= time.Now().UnixMilli() {
		e.Del(key)
		return
	}
	s := e.getShard(key)
	obj, ok := s.GetRaw(key)
	if !ok {
		return
	}
	obj.ExpireAt = expireAt
}

func replayPush(e *DBView, args [][]byte, isLeft bool) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	var list *object.List
	if ok && obj.Type == object.TypeList {
		list = coerceListValue(obj)
		if list == nil {
			return
		}
	} else {
		list = object.NewList()
		obj = &object.Object{
			Type:     object.TypeList,
			Encoding: object.EncodingRaw,
			Value:    list,
		}
		s.Set(key, obj)
	}
	for _, arg := range args[1:] {
		if isLeft {
			list.PushFront(arg)
		} else {
			list.PushBack(arg)
		}
	}
}

func replayPushX(e *DBView, args [][]byte, isLeft bool) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeList {
		return
	}
	list := coerceListValue(obj)
	if list == nil {
		return
	}
	for _, arg := range args[1:] {
		if isLeft {
			list.PushFront(arg)
		} else {
			list.PushBack(arg)
		}
	}
}

func replayLInsert(e *DBView, args [][]byte) {
	if len(args) != 4 {
		return
	}
	key := string(args[0])
	position := strings.ToUpper(string(args[1]))
	if position != "BEFORE" && position != "AFTER" {
		return
	}
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeList {
		return
	}
	list := coerceListValue(obj)
	if list == nil {
		return
	}
	list.Insert(args[2], args[3], position == "BEFORE")
}

func replayLTrim(e *DBView, args [][]byte) {
	if len(args) != 3 {
		return
	}
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeList {
		return
	}
	list := coerceListValue(obj)
	if list == nil {
		return
	}
	if list.Trim(start, stop) == 0 {
		s.Del(key)
	}
}

func replayRPopLPush(e *DBView, args [][]byte) {
	if len(args) != 2 {
		return
	}
	sourceKey := string(args[0])
	destKey := string(args[1])
	sourceShard := e.getShard(sourceKey)
	sourceObj, ok := sourceShard.Get(sourceKey)
	if !ok || sourceObj.Type != object.TypeList {
		return
	}
	sourceList := coerceListValue(sourceObj)
	if sourceList == nil {
		return
	}
	value, popped := sourceList.PopBack()
	if !popped {
		return
	}

	destShard := e.getShard(destKey)
	destObj, destExists := destShard.Get(destKey)
	var destList *object.List
	if destExists {
		if destObj.Type != object.TypeList {
			sourceList.PushBack(value)
			return
		}
		destList = coerceListValue(destObj)
		if destList == nil {
			sourceList.PushBack(value)
			return
		}
	} else {
		destList = object.NewList()
		destShard.Set(destKey, &object.Object{
			Type:     object.TypeList,
			Encoding: object.EncodingRaw,
			Value:    destList,
		})
	}

	if sourceList.LLen() == 0 && sourceKey != destKey {
		sourceShard.Del(sourceKey)
	}
	destList.PushFront(value)
}

func replayHSet(e *DBView, args [][]byte) {
	if len(args) < 3 || len(args)%2 == 0 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)

	obj, ok := s.Get(key)
	var h *object.Hash
	if ok {
		if obj.Type != object.TypeHash {
			return
		}
		h = obj.Value.(*object.Hash)
	} else {
		h = object.NewHash()
		s.Set(key, &object.Object{
			Type:     object.TypeHash,
			Encoding: object.EncodingRaw,
			Value:    h,
		})
	}

	for i := 1; i < len(args); i += 2 {
		h.Set(string(args[i]), string(args[i+1]))
	}
}

func replayHDel(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)

	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeHash {
		return
	}
	h := obj.Value.(*object.Hash)

	fields := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		fields = append(fields, string(arg))
	}
	h.Del(fields...)

	if h.Len() == 0 {
		s.Del(key)
	}
}

func replaySAdd(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)

	obj, ok := s.Get(key)
	var setObj *object.Set
	if ok {
		if obj.Type != object.TypeSet {
			return
		}
		setObj = obj.Value.(*object.Set)
	} else {
		setObj = object.NewSet()
		s.Set(key, &object.Object{
			Type:     object.TypeSet,
			Encoding: object.EncodingRaw,
			Value:    setObj,
		})
	}

	members := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		members = append(members, string(arg))
	}
	setObj.Add(members...)
}

func replaySRem(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)

	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeSet {
		return
	}
	setObj := obj.Value.(*object.Set)

	members := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		members = append(members, string(arg))
	}
	setObj.Remove(members...)

	if setObj.Len() == 0 {
		s.Del(key)
	}
}

type setStoreOperation uint8

const (
	setStoreIntersect setStoreOperation = iota
	setStoreUnion
	setStoreDifference
)

func replaySetStore(e *DBView, args [][]byte, operation setStoreOperation) {
	if len(args) < 2 {
		return
	}
	destination := string(args[0])
	sets := make([]*object.Set, len(args)-1)
	for i, arg := range args[1:] {
		obj, ok := e.Get(string(arg))
		if !ok {
			continue
		}
		if obj.Type != object.TypeSet {
			return
		}
		setObj, ok := obj.Value.(*object.Set)
		if !ok {
			return
		}
		sets[i] = setObj
	}

	result := make(map[string]struct{})
	switch operation {
	case setStoreIntersect:
		if sets[0] == nil {
			break
		}
		for _, member := range sets[0].Members() {
			result[member] = struct{}{}
		}
		for _, setObj := range sets[1:] {
			if setObj == nil {
				clear(result)
				break
			}
			for member := range result {
				if !setObj.Contains(member) {
					delete(result, member)
				}
			}
		}
	case setStoreUnion:
		for _, setObj := range sets {
			if setObj == nil {
				continue
			}
			for _, member := range setObj.Members() {
				result[member] = struct{}{}
			}
		}
	case setStoreDifference:
		if sets[0] == nil {
			break
		}
		for _, member := range sets[0].Members() {
			result[member] = struct{}{}
		}
		for _, setObj := range sets[1:] {
			if setObj == nil {
				continue
			}
			for member := range result {
				if setObj.Contains(member) {
					delete(result, member)
				}
			}
		}
	}

	if len(result) == 0 {
		e.Del(destination)
		return
	}
	stored := object.NewSet()
	for member := range result {
		stored.Add(member)
	}
	e.Set(destination, &object.Object{Type: object.TypeSet, Value: stored})
}

func replaySMove(e *DBView, args [][]byte) {
	if len(args) != 3 {
		return
	}
	sourceKey := string(args[0])
	destinationKey := string(args[1])
	member := string(args[2])

	sourceObj, sourceExists := e.Get(sourceKey)
	destinationObj, destinationExists := e.Get(destinationKey)
	if sourceExists && sourceObj.Type != object.TypeSet {
		return
	}
	if destinationExists && destinationObj.Type != object.TypeSet {
		return
	}
	if !sourceExists {
		return
	}
	source, ok := sourceObj.Value.(*object.Set)
	if !ok || !source.Contains(member) {
		return
	}

	var destination *object.Set
	if destinationExists {
		destination, ok = destinationObj.Value.(*object.Set)
		if !ok {
			return
		}
	} else {
		destination = object.NewSet()
		e.Set(destinationKey, &object.Object{Type: object.TypeSet, Encoding: object.EncodingRaw, Value: destination})
	}

	source.Remove(member)
	destination.Add(member)
	if source.Len() == 0 {
		e.Del(sourceKey)
	}
}

func replayPop(e *DBView, args [][]byte, isLeft bool) {
	if len(args) < 1 || len(args) > 2 {
		return
	}
	count := 1
	if len(args) == 2 {
		parsed, err := strconv.Atoi(string(args[1]))
		if err != nil || parsed < 0 {
			return
		}
		count = parsed
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeList {
		return
	}
	list := coerceListValue(obj)
	if list == nil {
		return
	}
	for i := 0; i < count; i++ {
		var popped bool
		if isLeft {
			_, popped = list.PopFront()
		} else {
			_, popped = list.PopBack()
		}
		if !popped {
			break
		}
	}
	if list.LLen() == 0 && count > 0 {
		s.Del(key)
	}
}

func replayLMove(e *DBView, args [][]byte) {
	if len(args) != 4 {
		return
	}
	sourceKey := string(args[0])
	destKey := string(args[1])
	whereFrom := strings.ToUpper(string(args[2]))
	whereTo := strings.ToUpper(string(args[3]))

	s := e.getShard(sourceKey)
	sourceObj, ok := s.Get(sourceKey)
	if !ok || sourceObj.Type != object.TypeList {
		return
	}
	sourceList := coerceListValue(sourceObj)
	if sourceList == nil {
		return
	}

	var val []byte
	var popped bool
	if whereFrom == "LEFT" {
		val, popped = sourceList.PopFront()
	} else {
		val, popped = sourceList.PopBack()
	}
	if !popped {
		return
	}
	if sourceList.LLen() == 0 {
		s.Del(sourceKey)
	}

	destS := e.getShard(destKey)
	destObj, ok := destS.Get(destKey)
	var destList *object.List
	if ok && destObj.Type == object.TypeList {
		destList = coerceListValue(destObj)
		if destList == nil {
			return
		}
	} else {
		destList = object.NewList()
		destS.Set(destKey, &object.Object{
			Type:     object.TypeList,
			Encoding: object.EncodingRaw,
			Value:    destList,
		})
	}

	if whereTo == "LEFT" {
		destList.PushFront(val)
	} else {
		destList.PushBack(val)
	}
}

func replayLRem(e *DBView, args [][]byte) {
	if len(args) != 3 {
		return
	}
	key := string(args[0])
	countStr := string(args[1])
	element := args[2]

	count := int(parseFloat(countStr))

	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeList {
		return
	}
	list := coerceListValue(obj)
	if list == nil {
		return
	}
	list.Remove(element, count)
	if list.LLen() == 0 {
		s.Del(key)
	}
}

func replayZAdd(e *DBView, args [][]byte) {
	// ZADD key score member [score member ...]
	if len(args) < 3 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	var zset *object.ZSet
	if ok && obj.Type == object.TypeZSet {
		zset = obj.Value.(*object.ZSet)
	} else {
		zset = object.NewZSet()
		obj = &object.Object{
			Type:     object.TypeZSet,
			Encoding: object.EncodingRaw,
			Value:    zset,
		}
		s.Set(key, obj)
	}
	for i := 1; i+1 < len(args); i += 2 {
		score := parseFloat(string(args[i]))
		member := string(args[i+1])
		zset.Add(score, member)
	}
}

func replayZPopMin(e *DBView, args [][]byte) {
	replayZPop(e, args, true)
}

func replayZPopMax(e *DBView, args [][]byte) {
	replayZPop(e, args, false)
}

func replayZPop(e *DBView, args [][]byte, minimum bool) {
	if len(args) < 1 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeZSet {
		return
	}
	count := 1
	if len(args) > 1 {
		parsed, err := strconv.Atoi(string(args[1]))
		if err != nil || parsed < 0 {
			return
		}
		count = parsed
	}
	zset := obj.Value.(*object.ZSet)
	if minimum {
		zset.PopMin(count)
	} else {
		zset.PopMax(count)
	}
	if zset.Len() == 0 {
		s.Del(key)
	}
}

func replayZRem(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])

	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeZSet {
		return
	}
	zset := obj.Value.(*object.ZSet)
	for _, arg := range args[1:] {
		zset.Remove(string(arg))
	}
	if zset.Len() == 0 {
		s.Del(key)
	}
}

func replayZRemRangeByScore(e *DBView, args [][]byte) {
	if len(args) < 3 {
		return
	}
	key := string(args[0])
	minStr := string(args[1])
	maxStr := string(args[2])

	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeZSet {
		return
	}
	zset := obj.Value.(*object.ZSet)

	// Since parseZRangeScoreBound is in zsetcmd, we can inline basic float parsing here,
	// or we can just ignore replay since doing full parsing is complex without importing the command package.
	// We will implement a simplified boundary parsing.

	minExcl := false
	if len(minStr) > 0 && minStr[0] == '(' {
		minExcl = true
		minStr = minStr[1:]
	}
	min := parseFloat(minStr)

	maxExcl := false
	if len(maxStr) > 0 && maxStr[0] == '(' {
		maxExcl = true
		maxStr = maxStr[1:]
	}
	max := parseFloat(maxStr)

	zset.RemRangeByScore(min, max, minExcl, maxExcl)
	if zset.Len() == 0 {
		s.Del(key)
	}
}

func replayXAdd(e *DBView, args [][]byte) {
	if len(args) < 4 {
		return
	}
	key := string(args[0])
	nomkstream := false
	hasIDMP := false
	idmpAuto := false
	idmpPID := ""
	idmpIID := ""
	trimStrategy := ""
	trimApprox := false
	trimThreshold := ""
	trimLimit := -1
	trimPolicy := object.StreamTrimKeepRef
	hasRefPolicy := false

	i := 1
	for i < len(args) {
		token := strings.ToUpper(string(args[i]))
		switch token {
		case "NOMKSTREAM":
			nomkstream = true
			i++
		case "KEEPREF":
			if hasRefPolicy {
				return
			}
			trimPolicy = object.StreamTrimKeepRef
			hasRefPolicy = true
			i++
		case "DELREF":
			if hasRefPolicy {
				return
			}
			trimPolicy = object.StreamTrimDelRef
			hasRefPolicy = true
			i++
		case "ACKED":
			if hasRefPolicy {
				return
			}
			trimPolicy = object.StreamTrimAcked
			hasRefPolicy = true
			i++
		case "IDMP":
			if hasIDMP || i+2 >= len(args) {
				return
			}
			hasIDMP = true
			idmpPID = string(args[i+1])
			idmpIID = string(args[i+2])
			i += 3
		case "IDMPAUTO":
			if hasIDMP || i+1 >= len(args) {
				return
			}
			hasIDMP = true
			idmpAuto = true
			idmpPID = string(args[i+1])
			i += 2
		case "MAXLEN", "MINID":
			if trimStrategy != "" {
				return
			}
			trimStrategy = token
			i++
			if i >= len(args) {
				return
			}
			op := string(args[i])
			if op == "=" {
				trimApprox = false
				i++
			} else if op == "~" {
				trimApprox = true
				i++
			}
			if i >= len(args) {
				return
			}
			trimThreshold = string(args[i])
			i++
			if i < len(args) && strings.EqualFold(string(args[i]), "LIMIT") {
				if i+1 >= len(args) {
					return
				}
				lim, err := strconv.Atoi(string(args[i+1]))
				if err != nil || lim < 0 {
					return
				}
				trimLimit = lim
				i += 2
			}
		default:
			goto doneXAddOptions
		}
	}
doneXAddOptions:
	if i >= len(args) {
		return
	}
	id := string(args[i])
	fieldArgs := args[i+1:]
	if len(fieldArgs) == 0 || len(fieldArgs)%2 != 0 {
		return
	}

	s := e.getShard(key)
	obj, ok := s.Get(key)
	var stream *object.Stream
	created := false
	if ok {
		if obj.Type != object.TypeStream {
			return
		}
		stream, _ = obj.Value.(*object.Stream)
	}
	if stream == nil {
		if nomkstream {
			return
		}
		stream = object.NewStream()
		obj = &object.Object{
			Type:     object.TypeStream,
			Encoding: object.EncodingRaw,
			Value:    stream,
		}
		s.Set(key, obj)
		created = true
	}

	fields := make([]object.StreamField, 0, len(fieldArgs)/2)
	for j := 0; j < len(fieldArgs); j += 2 {
		fields = append(fields, object.StreamField{
			Name:  string(fieldArgs[j]),
			Value: string(fieldArgs[j+1]),
		})
	}

	var err error
	if hasIDMP {
		iid := idmpIID
		if idmpAuto {
			iid = idmpPID
		}
		_, _, err = stream.AddIDMP(id, fields, idmpPID, iid)
	} else {
		_, err = stream.Add(id, fields)
	}
	if err != nil {
		if created {
			s.Del(key)
		}
		return
	}

	switch trimStrategy {
	case "MAXLEN":
		maxlen, err := strconv.Atoi(trimThreshold)
		if err != nil || maxlen < 0 {
			return
		}
		stream.TrimMaxLenFull(maxlen, trimApprox, trimLimit, trimPolicy)
	case "MINID":
		stream.TrimMinIDFull(trimThreshold, trimApprox, trimLimit, trimPolicy)
	}
}

func replayXAck(e *DBView, args [][]byte) {
	// XACK key group id [id ...]
	if len(args) < 3 {
		return
	}
	key := string(args[0])
	group := string(args[1])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeStream {
		return
	}
	stream, _ := obj.Value.(*object.Stream)
	if stream == nil {
		return
	}
	ids := make([]string, 0, len(args)-2)
	for _, a := range args[2:] {
		ids = append(ids, string(a))
	}
	stream.Ack(group, ids...)
}

func replayXGroup(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	sub := strings.ToUpper(string(args[0]))
	key := string(args[1])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	var stream *object.Stream
	if ok && obj.Type == object.TypeStream {
		stream, _ = obj.Value.(*object.Stream)
	}

	switch sub {
	case "CREATE":
		if len(args) < 4 {
			return
		}
		groupName := string(args[2])
		startID := string(args[3])
		mkstream := false
		for _, a := range args[4:] {
			if strings.EqualFold(string(a), "MKSTREAM") {
				mkstream = true
			}
		}
		if stream == nil {
			if !mkstream {
				return
			}
			stream = object.NewStream()
			s.Set(key, &object.Object{
				Type:     object.TypeStream,
				Encoding: object.EncodingRaw,
				Value:    stream,
			})
		}
		_ = stream.CreateGroup(groupName, startID)
	case "SETID":
		if len(args) < 4 || stream == nil {
			return
		}
		_ = stream.SetGroupID(string(args[2]), string(args[3]))
	case "DESTROY":
		if len(args) < 3 || stream == nil {
			return
		}
		stream.DestroyGroup(string(args[2]))
	case "CREATECONSUMER":
		if len(args) < 4 || stream == nil {
			return
		}
		_, _ = stream.CreateConsumer(string(args[2]), string(args[3]))
	case "DELCONSUMER":
		if len(args) < 4 || stream == nil {
			return
		}
		_, _ = stream.DelConsumer(string(args[2]), string(args[3]))
	}
}

func replayXTrim(e *DBView, args [][]byte) {
	if len(args) < 3 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeStream {
		return
	}
	stream, _ := obj.Value.(*object.Stream)
	if stream == nil {
		return
	}
	strategy := strings.ToUpper(string(args[1]))
	if strategy != "MAXLEN" && strategy != "MINID" {
		return
	}
	approx := false
	idx := 2
	if string(args[idx]) == "~" {
		approx = true
		idx++
	} else if string(args[idx]) == "=" {
		idx++
	}
	if idx >= len(args) {
		return
	}
	threshold := string(args[idx])
	idx++
	limit := -1
	policy := object.StreamTrimKeepRef
	for idx < len(args) {
		tok := strings.ToUpper(string(args[idx]))
		switch tok {
		case "LIMIT":
			if idx+1 >= len(args) {
				return
			}
			lim, err := strconv.Atoi(string(args[idx+1]))
			if err != nil || lim < 0 {
				return
			}
			limit = lim
			idx += 2
		case "KEEPREF":
			policy = object.StreamTrimKeepRef
			idx++
		case "DELREF":
			policy = object.StreamTrimDelRef
			idx++
		case "ACKED":
			policy = object.StreamTrimAcked
			idx++
		default:
			idx++
		}
	}
	if strategy == "MAXLEN" {
		maxlen, err := strconv.Atoi(threshold)
		if err != nil || maxlen < 0 {
			return
		}
		stream.TrimMaxLenFull(maxlen, approx, limit, policy)
	} else {
		stream.TrimMinIDFull(threshold, approx, limit, policy)
	}
}

func replayXDel(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeStream {
		return
	}
	stream, _ := obj.Value.(*object.Stream)
	if stream == nil {
		return
	}
	ids := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		ids[i-1] = string(args[i])
	}
	stream.Delete(ids...)
}

func replayXDelEx(e *DBView, args [][]byte) {
	if len(args) < 4 {
		return
	}
	key := string(args[0])
	policy := object.StreamTrimKeepRef
	var ids []object.StreamID
	for i := 1; i < len(args); {
		tok := strings.ToUpper(string(args[i]))
		switch tok {
		case "KEEPREF":
			policy = object.StreamTrimKeepRef
			i++
		case "DELREF":
			policy = object.StreamTrimDelRef
			i++
		case "ACKED":
			policy = object.StreamTrimAcked
			i++
		case "IDS":
			if i+1 >= len(args) {
				return
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n <= 0 || i+2+n > len(args) {
				return
			}
			for j := 0; j < n; j++ {
				id, err := object.ParseStreamID(string(args[i+2+j]))
				if err != nil {
					return
				}
				ids = append(ids, id)
			}
			i += 2 + n
		default:
			return
		}
	}
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeStream {
		return
	}
	stream, _ := obj.Value.(*object.Stream)
	if stream == nil {
		return
	}
	for _, id := range ids {
		stream.DeleteWithPolicy(id, policy)
	}
}

func replayXAckDel(e *DBView, args [][]byte) {
	if len(args) < 5 {
		return
	}
	key := string(args[0])
	groupName := string(args[1])
	policy := object.StreamTrimKeepRef
	var ids []object.StreamID
	for i := 2; i < len(args); {
		tok := strings.ToUpper(string(args[i]))
		switch tok {
		case "KEEPREF":
			policy = object.StreamTrimKeepRef
			i++
		case "DELREF":
			policy = object.StreamTrimDelRef
			i++
		case "ACKED":
			policy = object.StreamTrimAcked
			i++
		case "IDS":
			if i+1 >= len(args) {
				return
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n <= 0 || i+2+n > len(args) {
				return
			}
			for j := 0; j < n; j++ {
				id, err := object.ParseStreamID(string(args[i+2+j]))
				if err != nil {
					return
				}
				ids = append(ids, id)
			}
			i += 2 + n
		default:
			return
		}
	}
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeStream {
		return
	}
	stream, _ := obj.Value.(*object.Stream)
	if stream == nil {
		return
	}
	for _, id := range ids {
		stream.AckDelete(groupName, id, policy)
	}
}

func replayXSetID(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	lastID, err := object.ParseStreamID(string(args[1]))
	if err != nil {
		return
	}
	var entriesAdded int64 = -1
	var maxDeletedID *object.StreamID
	i := 2
	for i < len(args) {
		tok := strings.ToUpper(string(args[i]))
		switch tok {
		case "ENTRIESADDED":
			if i+1 >= len(args) {
				return
			}
			ea, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ea < 0 {
				return
			}
			entriesAdded = ea
			i += 2
		case "MAXDELETEDID":
			if i+1 >= len(args) {
				return
			}
			md, err := object.ParseStreamID(string(args[i+1]))
			if err != nil {
				return
			}
			maxDeletedID = &md
			i += 2
		default:
			return
		}
	}
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeStream {
		return
	}
	stream, _ := obj.Value.(*object.Stream)
	if stream == nil {
		return
	}
	if entriesAdded >= 0 || maxDeletedID != nil {
		_ = stream.SetLastIDWithOptions(lastID, entriesAdded, maxDeletedID)
	} else {
		_ = stream.SetLastID(lastID)
	}
}

func replayXReadGroup(e *DBView, args [][]byte) {
	if len(args) < 6 {
		return
	}
	if !strings.EqualFold(string(args[0]), "GROUP") {
		return
	}
	groupName := string(args[1])
	consumerName := string(args[2])
	count := 0
	noAck := false
	claimIdleMs := int64(-1)
	idx := 3
	for idx < len(args) {
		tok := strings.ToUpper(string(args[idx]))
		if tok == "STREAMS" {
			idx++
			break
		}
		switch tok {
		case "COUNT":
			if idx+1 >= len(args) {
				return
			}
			c, err := strconv.Atoi(string(args[idx+1]))
			if err != nil {
				return
			}
			count = c
			idx += 2
		case "BLOCK":
			idx += 2
		case "NOACK":
			noAck = true
			idx++
		case "CLAIM":
			if idx+1 >= len(args) {
				return
			}
			ms, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
			if err != nil {
				return
			}
			claimIdleMs = ms
			idx += 2
		default:
			idx++
		}
	}
	remaining := args[idx:]
	if len(remaining) == 0 || len(remaining)%2 != 0 {
		return
	}
	pairs := len(remaining) / 2
	for i := 0; i < pairs; i++ {
		sKey := string(remaining[i])
		sID := string(remaining[pairs+i])
		s := e.getShard(sKey)
		obj, ok := s.Get(sKey)
		if !ok || obj.Type != object.TypeStream {
			continue
		}
		stream, _ := obj.Value.(*object.Stream)
		if stream == nil {
			continue
		}
		_ = claimIdleMs
		_, _ = stream.ReadGroup(groupName, consumerName, count, sID, noAck)
	}
}

func replayXAutoClaim(e *DBView, args [][]byte) {
	if len(args) < 5 {
		return
	}
	key := string(args[0])
	groupName := string(args[1])
	consumer := string(args[2])
	minIdleMs, err := strconv.ParseInt(string(args[3]), 10, 64)
	if err != nil || minIdleMs < 0 {
		return
	}
	start := string(args[4])
	count := 100
	justID := false
	for i := 5; i < len(args); i++ {
		tok := strings.ToUpper(string(args[i]))
		switch tok {
		case "COUNT":
			if i+1 >= len(args) {
				return
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n <= 0 {
				return
			}
			count = n
			i++
		case "JUSTID":
			justID = true
		}
	}
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeStream {
		return
	}
	stream, _ := obj.Value.(*object.Stream)
	if stream == nil {
		return
	}
	stream.AutoClaim(groupName, consumer, minIdleMs, start, count, justID)
}

func replayXCfgSet(e *DBView, args [][]byte) {
	if len(args) < 3 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeStream {
		return
	}
	stream, _ := obj.Value.(*object.Stream)
	if stream == nil {
		return
	}
	var duration, maxSize int64
	hasDuration, hasMaxSize := false, false
	for i := 1; i+1 < len(args); i += 2 {
		opt := strings.ToUpper(string(args[i]))
		val, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil {
			return
		}
		switch opt {
		case "IDMP-DURATION":
			duration = val
			hasDuration = true
		case "IDMP-MAXSIZE":
			maxSize = val
			hasMaxSize = true
		}
	}
	stream.SetIDMPConfig(duration, maxSize, hasDuration, hasMaxSize)
}

func replayXIDMPRecord(e *DBView, args [][]byte) {
	if len(args) != 4 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeStream {
		return
	}
	stream, _ := obj.Value.(*object.Stream)
	if stream == nil {
		return
	}
	_ = stream.RecordIDMP(string(args[1]), string(args[2]), string(args[3]))
}

func replayCopy(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	source := string(args[0])
	dest := string(args[1])
	replace := false
	for i := 2; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "REPLACE") {
			replace = true
		}
	}
	srcObj, ok := e.Get(source)
	if !ok {
		return
	}
	if !replace {
		if _, exists := e.Get(dest); exists {
			return
		}
	}
	e.Set(dest, srcObj.DeepCopy())
}

func parseFloat(s string) float64 {
	var f float64
	fmt_scan(s, &f)
	return f
}

func fmt_scan(s string, f *float64) {
	// Simple float parser
	val := 0.0
	neg := false
	i := 0
	if len(s) > 0 && s[0] == '-' {
		neg = true
		i++
	}
	for ; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
		val = val*10 + float64(s[i]-'0')
	}
	if i < len(s) && s[i] == '.' {
		i++
		frac := 0.1
		for ; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
			val += float64(s[i]-'0') * frac
			frac *= 0.1
		}
	}
	if neg {
		val = -val
	}
	*f = val
}

func replayIncrBy(e *DBView, args [][]byte, delta int64) {
	if len(args) < 1 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	var current int64
	if ok && obj.Type == object.TypeString {
		if b, isBytes := obj.Value.([]byte); isBytes {
			current, _ = strconv.ParseInt(string(b), 10, 64)
		}
	}
	current += delta
	s.Set(key, &object.Object{Type: object.TypeString, Value: []byte(strconv.FormatInt(current, 10))})
}

func replayIncrByArg(e *DBView, args [][]byte, sign int64) {
	if len(args) < 2 {
		return
	}
	delta, _ := strconv.ParseInt(string(args[1]), 10, 64)
	replayIncrBy(e, args[:1], delta*sign)
}

func replayIncrByFloat(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	incr, _ := strconv.ParseFloat(string(args[1]), 64)
	s := e.getShard(key)
	obj, ok := s.Get(key)
	var current float64
	if ok && obj.Type == object.TypeString {
		if b, isBytes := obj.Value.([]byte); isBytes {
			current, _ = strconv.ParseFloat(string(b), 64)
		}
	}
	current += incr
	s.Set(key, &object.Object{Type: object.TypeString, Value: []byte(strconv.FormatFloat(current, 'g', -1, 64))})
}

func replayAppend(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	appendVal := args[1]
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if ok && obj.Type == object.TypeString {
		existing, _ := obj.Value.([]byte)
		newVal := make([]byte, 0, len(existing)+len(appendVal))
		newVal = append(newVal, existing...)
		newVal = append(newVal, appendVal...)
		obj.Value = newVal
	} else {
		s.Set(key, &object.Object{Type: object.TypeString, Value: append([]byte(nil), appendVal...)})
	}
}

func replayMSet(e *DBView, args [][]byte, onlyIfAbsent bool) {
	if len(args)%2 != 0 {
		return
	}
	if onlyIfAbsent {
		for i := 0; i < len(args); i += 2 {
			if _, ok := e.GetRaw(string(args[i])); ok {
				return
			}
		}
	}
	for i := 0; i+1 < len(args); i += 2 {
		key := string(args[i])
		val := args[i+1]
		e.Set(key, &object.Object{Type: object.TypeString, Value: val})
	}
}

func replaySetNX(e *DBView, args [][]byte) {
	if len(args) != 2 {
		return
	}
	key := string(args[0])
	if _, exists := e.GetRaw(key); exists {
		return
	}
	e.Set(key, &object.Object{Type: object.TypeString, Value: args[1]})
}

func replaySetEx(e *DBView, args [][]byte, isMs bool, entryTimestamp int64) {
	if len(args) < 3 {
		return
	}
	key := string(args[0])
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return
	}
	if !isMs {
		ttl *= 1000
	}
	expireAt := entryTimestamp + ttl
	if expireAt <= time.Now().UnixMilli() {
		e.Del(key)
		return
	}
	e.Set(key, &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    args[2],
		ExpireAt: expireAt,
	})
}

func replayLSet(e *DBView, args [][]byte) {
	if len(args) < 3 {
		return
	}
	key := string(args[0])
	idx, _ := strconv.Atoi(string(args[1]))
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeList {
		return
	}
	list := coerceListValue(obj)
	if list == nil {
		return
	}
	list.Set(idx, args[2])
}

func replayRename(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	src := string(args[0])
	dst := string(args[1])
	obj, ok := e.Get(src)
	if !ok {
		return
	}
	e.Del(src)
	e.Set(dst, obj)
}

func replayFlush(e *DBView) {
	for i := 0; i < len(e.shards); i++ {
		e.shards[i].Flush()
	}
}

func replayHSetNX(e *DBView, args [][]byte) {
	if len(args) < 3 {
		return
	}
	key := string(args[0])
	field := string(args[1])
	value := string(args[2])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	var h *object.Hash
	if ok && obj.Type == object.TypeHash {
		h = obj.Value.(*object.Hash)
	} else {
		h = object.NewHash()
		s.Set(key, &object.Object{Type: object.TypeHash, Value: h})
	}
	if _, exists := h.Get(field); !exists {
		h.Set(field, value)
	}
}

func replaySPop(e *DBView, args [][]byte) {
	if len(args) < 1 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeSet {
		return
	}
	setObj := obj.Value.(*object.Set)
	members := setObj.Members()
	if len(members) > 0 {
		setObj.Remove(members[0])
	}
	if setObj.Len() == 0 {
		s.Del(key)
	}
}

func replayPExpire(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	ms, _ := strconv.ParseInt(string(args[1]), 10, 64)
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok {
		return
	}
	_ = ms
	// Can't accurately replay relative TTL without original timestamp
	_ = obj
}

func replayExpireAt(e *DBView, args [][]byte, isSecs bool) {
	if len(args) < 2 {
		return
	}
	ts, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return
	}
	if isSecs {
		ts *= 1000
	}
	applyExpireAt(e, string(args[0]), ts)
}

func replayPersist(e *DBView, args [][]byte) {
	if len(args) < 1 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok {
		return
	}
	obj.ExpireAt = 0
}

// coerceListValue normalizes legacy/raw recovered list payloads into a live list.
func coerceListValue(obj *object.Object) *object.List {
	if obj == nil || obj.Type != object.TypeList {
		return nil
	}
	if list, ok := obj.Value.(*object.List); ok {
		return list
	}
	if _, ok := obj.Value.([]byte); ok {
		list := object.NewList()
		obj.Value = list
		return list
	}
	return nil
}

func replaySort(e *DBView, args [][]byte) {
	if len(args) < 1 {
		return
	}
	storeDest := ""
	for i := 1; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "STORE") && i+1 < len(args) {
			storeDest = string(args[i+1])
			break
		}
	}
	if storeDest == "" {
		return
	}
	key := string(args[0])
	obj, ok := e.Get(key)
	if !ok {
		e.Del(storeDest)
		return
	}
	var elements []string
	switch obj.Type {
	case object.TypeList:
		vals := obj.Value.(*object.List).Snapshot()
		elements = make([]string, len(vals))
		for i, v := range vals {
			elements[i] = string(v)
		}
	case object.TypeSet:
		elements = obj.Value.(*object.Set).Members()
	case object.TypeZSet:
		elements = obj.Value.(*object.ZSet).Members()
	default:
		return
	}

	alpha := false
	desc := false
	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		if arg == "ALPHA" {
			alpha = true
		} else if arg == "DESC" {
			desc = true
		}
	}

	sort.SliceStable(elements, func(i, j int) bool {
		if alpha {
			if desc {
				return elements[i] > elements[j]
			}
			return elements[i] < elements[j]
		}
		fi, _ := strconv.ParseFloat(elements[i], 64)
		fj, _ := strconv.ParseFloat(elements[j], 64)
		if desc {
			return fi > fj
		}
		return fi < fj
	})

	list := object.NewList()
	for _, el := range elements {
		list.PushBack([]byte(el))
	}
	e.Set(storeDest, &object.Object{
		Type:     object.TypeList,
		Encoding: object.EncodingRaw,
		Value:    list,
	})
}

func replayHExpire(e *DBView, args [][]byte, entryTimestamp, timeUnit int64, isAt, isPersist bool) {
	if len(args) < 3 {
		return
	}
	nowMs := entryTimestamp
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	key := string(args[0])
	idx := 1
	var expireTime int64
	if !isPersist {
		val, err := strconv.ParseInt(string(args[idx]), 10, 64)
		if err != nil {
			return
		}
		idx++
		if isAt {
			expireTime = val * timeUnit
		} else {
			expireTime = nowMs + (val * timeUnit)
		}
	}
	condition := 0
	if idx < len(args) {
		switch strings.ToUpper(string(args[idx])) {
		case "NX":
			condition = 1
			idx++
		case "XX":
			condition = 2
			idx++
		case "GT":
			condition = 3
			idx++
		case "LT":
			condition = 4
			idx++
		}
	}
	if idx >= len(args) || !strings.EqualFold(string(args[idx]), "FIELDS") {
		return
	}
	idx++
	if idx >= len(args) {
		return
	}
	numfields, err := strconv.Atoi(string(args[idx]))
	if err != nil || numfields <= 0 || len(args)-idx-1 != numfields {
		return
	}
	idx++
	obj, ok := e.Get(key)
	if !ok || obj.Type != object.TypeHash {
		return
	}
	h := obj.Value.(*object.Hash)
	for i := 0; i < numfields; i++ {
		field := string(args[idx+i])
		if _, exists := h.Get(field); !exists {
			continue
		}
		currentTTL := h.GetTTL(field)
		hasTTL := currentTTL > 0
		if !isPersist && expireTime <= nowMs {
			if condition == 1 && hasTTL {
				continue
			}
			if condition == 2 && !hasTTL {
				continue
			}
			if condition == 3 && (!hasTTL || expireTime <= currentTTL) {
				continue
			}
			if condition == 4 && (!hasTTL || expireTime >= currentTTL) {
				continue
			}
			h.Del(field)
			continue
		}
		if (condition == 1 && hasTTL) || (condition == 2 && !hasTTL) {
			continue
		}
		if condition == 3 && (!hasTTL || expireTime <= currentTTL) {
			continue
		}
		if condition == 4 && hasTTL && expireTime >= currentTTL {
			continue
		}
		if isPersist {
			if hasTTL {
				h.DelTTL(field)
			}
		} else {
			h.SetTTL(field, expireTime)
		}
	}
	if h.Len() == 0 {
		e.Del(key)
	}
}

func replayHSetEx(e *DBView, args [][]byte, entryTimestamp int64) {
	if len(args) < 4 {
		return
	}
	nowMs := entryTimestamp
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	key := string(args[0])
	idx := 1
	condition := 0
	switch strings.ToUpper(string(args[idx])) {
	case "FNX":
		condition = 1
		idx++
	case "FXX":
		condition = 2
		idx++
	}
	var expireTime int64
	var hasExpire, keepTTL bool
	if idx < len(args) {
		switch strings.ToUpper(string(args[idx])) {
		case "EX":
			if idx+1 >= len(args) {
				return
			}
			val, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
			if err != nil {
				return
			}
			expireTime = nowMs + val*1000
			hasExpire = true
			idx += 2
		case "PX":
			if idx+1 >= len(args) {
				return
			}
			val, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
			if err != nil {
				return
			}
			expireTime = nowMs + val
			hasExpire = true
			idx += 2
		case "EXAT":
			if idx+1 >= len(args) {
				return
			}
			val, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
			if err != nil {
				return
			}
			expireTime = val * 1000
			hasExpire = true
			idx += 2
		case "PXAT":
			if idx+1 >= len(args) {
				return
			}
			val, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
			if err != nil {
				return
			}
			expireTime = val
			hasExpire = true
			idx += 2
		case "KEEPTTL":
			keepTTL = true
			idx++
		}
	}
	if idx >= len(args) || !strings.EqualFold(string(args[idx]), "FIELDS") {
		return
	}
	idx++
	if idx >= len(args) {
		return
	}
	numfields, err := strconv.Atoi(string(args[idx]))
	if err != nil || numfields <= 0 || len(args)-idx-1 != numfields*2 {
		return
	}
	idx++
	obj, ok := e.Get(key)
	var h *object.Hash
	if ok {
		if obj.Type != object.TypeHash {
			return
		}
		h = obj.Value.(*object.Hash)
	} else {
		if condition == 2 {
			return
		}
		h = object.NewHash()
		e.Set(key, &object.Object{Type: object.TypeHash, Value: h})
	}
	if condition == 1 {
		for i := 0; i < numfields; i++ {
			if _, exists := h.Get(string(args[idx+i*2])); exists {
				if h.Len() == 0 {
					e.Del(key)
				}
				return
			}
		}
	} else if condition == 2 {
		for i := 0; i < numfields; i++ {
			if _, exists := h.Get(string(args[idx+i*2])); !exists {
				if h.Len() == 0 {
					e.Del(key)
				}
				return
			}
		}
	}
	for i := 0; i < numfields; i++ {
		f := string(args[idx+i*2])
		v := string(args[idx+i*2+1])
		oldTTL := h.GetTTL(f)
		h.Set(f, v)
		if keepTTL && oldTTL > 0 {
			h.SetTTL(f, oldTTL)
		} else if hasExpire {
			if expireTime <= nowMs {
				h.Del(f)
			} else {
				h.SetTTL(f, expireTime)
			}
		}
	}
	if h.Len() == 0 {
		e.Del(key)
	}
}

func replayHGetEx(e *DBView, args [][]byte, entryTimestamp int64) {
	if len(args) < 4 {
		return
	}
	nowMs := entryTimestamp
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	key := string(args[0])
	idx := 1
	var expireTime int64
	var hasExpire, isPersist bool
	switch strings.ToUpper(string(args[idx])) {
	case "EX":
		if idx+1 >= len(args) {
			return
		}
		val, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
		if err != nil {
			return
		}
		expireTime = nowMs + val*1000
		hasExpire = true
		idx += 2
	case "PX":
		if idx+1 >= len(args) {
			return
		}
		val, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
		if err != nil {
			return
		}
		expireTime = nowMs + val
		hasExpire = true
		idx += 2
	case "EXAT":
		if idx+1 >= len(args) {
			return
		}
		val, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
		if err != nil {
			return
		}
		expireTime = val * 1000
		hasExpire = true
		idx += 2
	case "PXAT":
		if idx+1 >= len(args) {
			return
		}
		val, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
		if err != nil {
			return
		}
		expireTime = val
		hasExpire = true
		idx += 2
	case "PERSIST":
		isPersist = true
		hasExpire = true
		idx++
	}
	if !hasExpire || idx >= len(args) || !strings.EqualFold(string(args[idx]), "FIELDS") {
		return
	}
	idx++
	if idx >= len(args) {
		return
	}
	numfields, err := strconv.Atoi(string(args[idx]))
	if err != nil || numfields <= 0 || len(args)-idx-1 != numfields {
		return
	}
	idx++
	obj, ok := e.Get(key)
	if !ok || obj.Type != object.TypeHash {
		return
	}
	h := obj.Value.(*object.Hash)
	for i := 0; i < numfields; i++ {
		f := string(args[idx+i])
		if _, exists := h.Get(f); !exists {
			continue
		}
		if isPersist {
			h.DelTTL(f)
		} else if expireTime <= nowMs {
			h.Del(f)
		} else {
			h.SetTTL(f, expireTime)
		}
	}
	if h.Len() == 0 {
		e.Del(key)
	}
}

func replayBitOp(e *DBView, args [][]byte) {
	if len(args) < 3 {
		return
	}
	op := strings.ToUpper(string(args[0]))
	destKey := string(args[1])
	srcVals := make([][]byte, len(args)-2)
	maxLen := 0
	for i := 2; i < len(args); i++ {
		obj, ok := e.Get(string(args[i]))
		if !ok {
			continue
		}
		b, ok := obj.Value.([]byte)
		if !ok {
			return
		}
		if len(b) > maxLen {
			maxLen = len(b)
		}
		srcVals[i-2] = b
	}
	getB := func(b []byte, idx int) byte {
		if b == nil || idx >= len(b) {
			return 0
		}
		return b[idx]
	}
	result := make([]byte, maxLen)
	if maxLen > 0 {
		switch op {
		case "NOT":
			for i := 0; i < maxLen; i++ {
				result[i] = ^getB(srcVals[0], i)
			}
		case "AND":
			for i := 0; i < maxLen; i++ {
				b := byte(0xFF)
				for j := 0; j < len(srcVals); j++ {
					b &= getB(srcVals[j], i)
				}
				result[i] = b
			}
		case "OR":
			for i := 0; i < maxLen; i++ {
				var b byte
				for j := 0; j < len(srcVals); j++ {
					b |= getB(srcVals[j], i)
				}
				result[i] = b
			}
		case "XOR":
			for i := 0; i < maxLen; i++ {
				var b byte
				for j := 0; j < len(srcVals); j++ {
					b ^= getB(srcVals[j], i)
				}
				result[i] = b
			}
		}
	}
	e.Set(destKey, object.AcquireRawStringObject(result))
}

func parseReplayArrayIndex(s string) (int64, bool) {
	if s == "-" {
		return 0, true
	}
	if s == "+" {
		return 1<<63 - 1, true
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		un, uerr := strconv.ParseUint(s, 10, 64)
		if uerr == nil {
			if un >= (1<<63 - 1) {
				return -1, true
			}
			return int64(un), true
		}
		return 0, false
	}
	if n < 0 {
		return 0, false
	}
	return n, true
}

func getOrCreateReplayArray(e *DBView, key string, create bool) *object.Array {
	obj, exists := e.Get(key)
	if exists {
		if obj.Type != object.TypeArray {
			return nil
		}
		arr, ok := obj.Value.(*object.Array)
		if !ok {
			return nil
		}
		return arr
	}
	if !create {
		return nil
	}
	arr := object.NewArray(int64(object.DefaultSliceSize), object.DefaultSparseKmax, object.DefaultSparseKmin)
	e.Set(key, &object.Object{Type: object.TypeArray, Value: arr})
	return arr
}

func replayArSet(e *DBView, args [][]byte) {
	if len(args) < 3 {
		return
	}
	key := string(args[0])
	idx, ok := parseReplayArrayIndex(string(args[1]))
	if !ok {
		return
	}
	arr := getOrCreateReplayArray(e, key, true)
	if arr == nil {
		return
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()
	if len(args) == 3 {
		arr.Set(idx, string(args[2]))
		return
	}
	values := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		values[i-2] = string(args[i])
	}
	arr.SetContiguous(idx, values)
}

func replayArDel(e *DBView, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	arr := getOrCreateReplayArray(e, key, false)
	if arr == nil {
		return
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()
	for i := 1; i < len(args); i++ {
		idx, ok := parseReplayArrayIndex(string(args[i]))
		if !ok {
			continue
		}
		arr.Del(idx)
	}
	if arr.Count() == 0 {
		e.Del(key)
	}
}

func replayArDelRange(e *DBView, args [][]byte) {
	if len(args) < 3 || len(args)%2 != 1 {
		return
	}
	key := string(args[0])
	arr := getOrCreateReplayArray(e, key, false)
	if arr == nil {
		return
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()
	for i := 1; i < len(args); i += 2 {
		lo, ok1 := parseReplayArrayIndex(string(args[i]))
		hi, ok2 := parseReplayArrayIndex(string(args[i+1]))
		if !ok1 || !ok2 {
			continue
		}
		if lo > hi {
			lo, hi = hi, lo
		}
		arr.DelRange(lo, hi)
	}
	if arr.Count() == 0 {
		e.Del(key)
	}
}

func replayArMSet(e *DBView, args [][]byte) {
	if len(args) < 3 || len(args)%2 != 1 {
		return
	}
	key := string(args[0])
	arr := getOrCreateReplayArray(e, key, true)
	if arr == nil {
		return
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()
	for i := 1; i < len(args); i += 2 {
		idx, ok := parseReplayArrayIndex(string(args[i]))
		if !ok {
			continue
		}
		arr.Set(idx, string(args[i+1]))
	}
}

func replayArInsert(e *DBView, args [][]byte) {
	if len(args) != 2 {
		return
	}
	key := string(args[0])
	arr := getOrCreateReplayArray(e, key, true)
	if arr == nil {
		return
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()
	if arr.InsertIdx() < 0 {
		return
	}
	arr.Insert(string(args[1]))
}

func replayArRing(e *DBView, args [][]byte) {
	if len(args) != 3 {
		return
	}
	key := string(args[0])
	maxlen, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || maxlen <= 0 {
		return
	}
	arr := getOrCreateReplayArray(e, key, true)
	if arr == nil {
		return
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()
	arr.SetRingSize(maxlen)
	arr.RingInsert(maxlen, string(args[2]))
}

func replayArSeek(e *DBView, args [][]byte) {
	if len(args) != 2 {
		return
	}
	key := string(args[0])
	idx, ok := parseReplayArrayIndex(string(args[1]))
	if !ok {
		return
	}
	arr := getOrCreateReplayArray(e, key, false)
	if arr == nil {
		return
	}
	arr.SetInsertIdx(idx)
}


