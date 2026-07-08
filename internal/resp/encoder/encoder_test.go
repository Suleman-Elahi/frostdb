package encoder

import (
	"bytes"
	"testing"

	"frostdb/internal/resp/types"
)

func TestEncodeRESP3(t *testing.T) {
	tests := []struct {
		name     string
		val      types.Value
		expected string
	}{
		{
			name:     "TypeMap empty",
			val:      types.Value{Type: types.TypeMap, Array: []types.Value{}},
			expected: "%0\r\n",
		},
		{
			name: "TypeMap with values",
			val: types.Value{
				Type: types.TypeMap,
				Array: []types.Value{
					types.NewString("first"), types.NewInt(1),
					types.NewString("second"), types.NewInt(2),
				},
			},
			expected: "%2\r\n$5\r\nfirst\r\n:1\r\n$6\r\nsecond\r\n:2\r\n",
		},
		{
			name: "TypeSet with values",
			val: types.Value{
				Type: types.TypeSet,
				Array: []types.Value{
					types.NewString("apple"),
					types.NewString("orange"),
				},
			},
			expected: "~2\r\n$5\r\napple\r\n$6\r\norange\r\n",
		},
		{
			name:     "TypeBoolean true",
			val:      types.Value{Type: types.TypeBoolean, Num: 1},
			expected: "#t\r\n",
		},
		{
			name:     "TypeBoolean false",
			val:      types.Value{Type: types.TypeBoolean, Num: 0},
			expected: "#f\r\n",
		},
		{
			name:     "TypeNull",
			val:      types.Value{Type: types.TypeNull},
			expected: "_\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Encode(&buf, tt.val)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if buf.String() != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, buf.String())
			}
		})
	}
}
