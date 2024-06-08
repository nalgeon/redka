package parser

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestFlag(t *testing.T) {
	tests := []struct {
		title string
		name  string
		args  [][]byte
		match bool
		rest  [][]byte
	}{
		{
			title: "flag_flag",
			name:  "flag",
			args:  [][]byte{[]byte("flag")},
			match: true,
			rest:  [][]byte{},
		},
		{
			title: "flag_notflag",
			name:  "flag",
			args:  [][]byte{[]byte("notflag")},
			match: false,
			rest:  [][]byte{[]byte("notflag")},
		},
		{
			title: "flag_FLAG",
			name:  "flag",
			args:  [][]byte{[]byte("FLAG")},
			match: true,
			rest:  [][]byte{},
		},
		{
			title: "FLAG_flag",
			name:  "FLAG",
			args:  [][]byte{[]byte("flag")},
			match: true,
			rest:  [][]byte{},
		},
		{
			title: "flag_bytes",
			name:  "flag",
			args:  [][]byte{{0, 0, 0}},
			match: false,
			rest:  [][]byte{{0, 0, 0}},
		},
		{
			title: "flag_empty",
			name:  "flag",
			args:  [][]byte{},
			match: false,
			rest:  [][]byte{},
		},
		{
			title: "flag_flag_other",
			name:  "flag",
			args:  [][]byte{[]byte("flag"), []byte("other")},
			match: true,
			rest:  [][]byte{[]byte("other")},
		},
	}

	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			var dest bool
			parser := Flag(test.name, &dest)
			match, rest, err := parser(test.args)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, match, test.match)
			testx.AssertEqual(t, dest, test.match)
			testx.AssertEqual(t, rest, test.rest)
		})
	}
}
