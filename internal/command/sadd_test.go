package command

import (
	"reflect"
	"testing"
)

func TestParseSAdd(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSAdd(tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSAdd() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseSAdd() got = %v, want %v", got, tt.want)
			}
		})
	}
}
