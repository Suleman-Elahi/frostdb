package types

type ValueType byte

const (
	TypeSimpleString ValueType = '+'
	TypeError        ValueType = '-'
	TypeInteger      ValueType = ':'
	TypeBulkString   ValueType = '$'
	TypeArray        ValueType = '*'
	TypeNull         ValueType = '_'
	TypeBoolean      ValueType = '#'
	TypeDouble       ValueType = ','
	TypeBigNumber    ValueType = '('
	TypeBulkError    ValueType = '!'
	TypeVerbatim     ValueType = '='
	TypeMap          ValueType = '%'
	TypeSet          ValueType = '~'
	TypePush         ValueType = '>'
	TypeNoReply      ValueType = 0
)

// Value represents any RESP value
type Value struct {
	Type  ValueType
	Str   []byte
	Num   int64
	Array []Value
	IsNull bool
}

var (
	RespOK       = Value{Type: TypeSimpleString, Str: []byte("OK")}
	RespPong     = Value{Type: TypeSimpleString, Str: []byte("PONG")}
	RespNil      = Value{Type: TypeBulkString, IsNull: true}
	RespNilArr   = Value{Type: TypeArray, IsNull: true}
	RespZero     = Value{Type: TypeInteger, Num: 0}
	RespOne      = Value{Type: TypeInteger, Num: 1}
	RespEmptyArr = Value{Type: TypeArray, Array: []Value{}}
)

func NewString(s string) Value {
	return Value{Type: TypeBulkString, Str: []byte(s)}
}

func NewBytes(b []byte) Value {
	return Value{Type: TypeBulkString, Str: b}
}

func NewSimpleString(s string) Value {
	return Value{Type: TypeSimpleString, Str: []byte(s)}
}

func NewError(s string) Value {
	return Value{Type: TypeError, Str: []byte(s)}
}

func NewInt(n int64) Value {
	return Value{Type: TypeInteger, Num: n}
}

func NewArray(vals []Value) Value {
	return Value{Type: TypeArray, Array: vals}
}

func NewNull() Value {
	return Value{Type: TypeNull, IsNull: true}
}
