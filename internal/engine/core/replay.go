package core

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
)

// applyCommand replays a WAL command directly against the engine.
// This avoids importing the command/registry package (which would be circular).
// Only mutating commands need replay.
func applyCommand(e *Engine, args [][]byte) {
	if len(args) == 0 {
		return
	}

	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "SET":
		replaySet(e, args[1:])
	case "DEL":
		replayDel(e, args[1:])
	case "EXPIRE":
		replayExpire(e, args[1:])
	case "LPUSH", "RPUSH":
		replayPush(e, args[1:], cmd == "LPUSH")
	case "LPOP", "RPOP":
		replayPop(e, args[1:], cmd == "LPOP")
	case "LMOVE":
		replayLMove(e, args[1:])
	case "LREM":
		replayLRem(e, args[1:])
	case "ZADD":
		replayZAdd(e, args[1:])
	case "ZPOPMIN":
		replayZPopMin(e, args[1:])
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
	case "HSET":
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
	case "MSET", "MSETNX":
		replayMSet(e, args[1:])
	case "SETEX", "PSETEX":
		replaySetEx(e, args[1:], cmd == "PSETEX")
	case "SETNX":
		replaySet(e, args[1:])
	case "GETSET":
		replaySet(e, args[1:])
	case "GETDEL":
		replayDel(e, args[1:])
	case "LSET":
		replayLSet(e, args[1:])
	case "LINSERT":
		// Complex to replay without full list logic; skip for now
	case "RENAME", "RENAMENX":
		replayRename(e, args[1:])
	case "UNLINK":
		replayDel(e, args[1:])
	case "FLUSHDB", "FLUSHALL":
		replayFlush(e)
	case "HSETNX":
		replayHSetNX(e, args[1:])
	case "PEXPIRE":
		replayPExpire(e, args[1:])
	case "EXPIREAT":
		replayExpireAt(e, args[1:], true)
	case "PEXPIREAT":
		replayExpireAt(e, args[1:], false)
	case "PERSIST":
		replayPersist(e, args[1:])
	}
}

func replaySet(e *Engine, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	val := string(args[1])
	obj := &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    val,
	}
	e.Set(key, obj)
}

func replayHIncrBy(e *Engine, args [][]byte) {
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

func replayHIncrByFloat(e *Engine, args [][]byte) {
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

func replayDel(e *Engine, args [][]byte) {
	for _, arg := range args {
		e.Del(string(arg))
	}
}

func replayExpire(_ *Engine, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	// We don't replay EXPIRE since the expireAt is stored in the object
	// and checkpoint captures it. WAL replay of EXPIRE would need
	// the original timestamp context. Skip for now.
	_ = key
}

func replayPush(e *Engine, args [][]byte, isLeft bool) {
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

func replayHSet(e *Engine, args [][]byte) {
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

func replayHDel(e *Engine, args [][]byte) {
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

func replaySAdd(e *Engine, args [][]byte) {
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

func replaySRem(e *Engine, args [][]byte) {
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


func replayPop(e *Engine, args [][]byte, isLeft bool) {
	if len(args) < 1 {
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
	if isLeft {
		list.PopFront()
	} else {
		list.PopBack()
	}
}

func replayLMove(e *Engine, args [][]byte) {
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

func replayLRem(e *Engine, args [][]byte) {
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

func replayZAdd(e *Engine, args [][]byte) {
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

func replayZPopMin(e *Engine, args [][]byte) {
	if len(args) < 1 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok || obj.Type != object.TypeZSet {
		return
	}
	zset := obj.Value.(*object.ZSet)
	count := 1
	if len(args) > 1 {
		count = int(parseFloat(string(args[1])))
	}
	zset.PopMin(count)
}

func replayZRem(e *Engine, args [][]byte) {
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

func replayZRemRangeByScore(e *Engine, args [][]byte) {
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

func replayXAdd(e *Engine, args [][]byte) {
	// XADD key id field value [field value ...]
	if len(args) < 4 {
		return
	}
	key := string(args[0])
	id := string(args[1])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	var stream *object.Stream
	if ok && obj.Type == object.TypeStream {
		stream, _ = obj.Value.(*object.Stream)
	}
	if stream == nil {
		stream = object.NewStream()
		obj = &object.Object{
			Type:     object.TypeStream,
			Encoding: object.EncodingRaw,
			Value:    stream,
		}
		s.Set(key, obj)
	}
	var fields []object.StreamField
	for i := 2; i+1 < len(args); i += 2 {
		fields = append(fields, object.StreamField{
			Name:  string(args[i]),
			Value: string(args[i+1]),
		})
	}
	stream.Add(id, fields)
}

func replayXAck(e *Engine, args [][]byte) {
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

func replayXGroup(e *Engine, args [][]byte) {
	// XGROUP CREATE key group startid [MKSTREAM]
	if len(args) < 4 {
		return
	}
	sub := strings.ToUpper(string(args[0]))
	if sub != "CREATE" {
		return
	}
	key := string(args[1])
	groupName := string(args[2])
	startID := string(args[3])

	s := e.getShard(key)
	obj, ok := s.Get(key)
	var stream *object.Stream
	if ok && obj.Type == object.TypeStream {
		stream, _ = obj.Value.(*object.Stream)
	}
	if stream == nil {
		stream = object.NewStream()
		obj = &object.Object{
			Type:     object.TypeStream,
			Encoding: object.EncodingRaw,
			Value:    stream,
		}
		s.Set(key, obj)
	}
	stream.CreateGroup(groupName, startID)
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

func replayIncrBy(e *Engine, args [][]byte, delta int64) {
	if len(args) < 1 {
		return
	}
	key := string(args[0])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	var current int64
	if ok && obj.Type == object.TypeString {
		current, _ = strconv.ParseInt(obj.Value.(string), 10, 64)
	}
	current += delta
	s.Set(key, &object.Object{Type: object.TypeString, Value: strconv.FormatInt(current, 10)})
}

func replayIncrByArg(e *Engine, args [][]byte, sign int64) {
	if len(args) < 2 {
		return
	}
	delta, _ := strconv.ParseInt(string(args[1]), 10, 64)
	replayIncrBy(e, args[:1], delta*sign)
}

func replayIncrByFloat(e *Engine, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	incr, _ := strconv.ParseFloat(string(args[1]), 64)
	s := e.getShard(key)
	obj, ok := s.Get(key)
	var current float64
	if ok && obj.Type == object.TypeString {
		current, _ = strconv.ParseFloat(obj.Value.(string), 64)
	}
	current += incr
	s.Set(key, &object.Object{Type: object.TypeString, Value: strconv.FormatFloat(current, 'g', -1, 64)})
}

func replayAppend(e *Engine, args [][]byte) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	appendVal := string(args[1])
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if ok && obj.Type == object.TypeString {
		obj.Value = obj.Value.(string) + appendVal
	} else {
		s.Set(key, &object.Object{Type: object.TypeString, Value: appendVal})
	}
}

func replayMSet(e *Engine, args [][]byte) {
	for i := 0; i+1 < len(args); i += 2 {
		key := string(args[i])
		val := string(args[i+1])
		e.Set(key, &object.Object{Type: object.TypeString, Value: val})
	}
}

func replaySetEx(e *Engine, args [][]byte, isMs bool) {
	if len(args) < 3 {
		return
	}
	key := string(args[0])
	ttl, _ := strconv.ParseInt(string(args[1]), 10, 64)
	val := string(args[2])
	obj := &object.Object{Type: object.TypeString, Value: val}
	if isMs {
		obj.ExpireAt = ttl // already ms from original time
	} else {
		obj.ExpireAt = ttl * 1000
	}
	e.Set(key, obj)
}

func replayLSet(e *Engine, args [][]byte) {
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

func replayRename(e *Engine, args [][]byte) {
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

func replayFlush(e *Engine) {
	e.FlushDB(0)
}

func replayHSetNX(e *Engine, args [][]byte) {
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

func replaySPop(e *Engine, args [][]byte) {
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

func replayPExpire(e *Engine, args [][]byte) {
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

func replayExpireAt(e *Engine, args [][]byte, isSecs bool) {
	if len(args) < 2 {
		return
	}
	key := string(args[0])
	ts, _ := strconv.ParseInt(string(args[1]), 10, 64)
	s := e.getShard(key)
	obj, ok := s.Get(key)
	if !ok {
		return
	}
	if isSecs {
		obj.ExpireAt = ts * 1000
	} else {
		obj.ExpireAt = ts
	}
}

func replayPersist(e *Engine, args [][]byte) {
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
