package compatibility_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/server"
	"github.com/redis/go-redis/v9"
	_ "github.com/mattn/go-sqlite3"
)

type Compatibility []struct {
	Name          string   `json:"name"`
	Command       []string `json:"command"`
	Result        []any    `json:"result"`
	Since         string   `json:"since"`
	Tags          string   `json:"tags,omitempty"`
	CommandBinary bool     `json:"command_binary,omitempty"`
	Skipped       bool     `json:"skipped,omitempty"`
	SortResult    bool     `json:"sort_result,omitempty"`
}


func TestCompatibility(t *testing.T) {
	var payload Compatibility

	db, err := redka.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("could not start redka: %s", err)
	}
	defer db.Close()

	srv := server.New(":6412", db)
	srv.Start()
	defer srv.Stop()

	// https://raw.githubusercontent.com/tair-opensource/compatibility-test-suite-for-redis/main/cts.json
	contents, err := os.ReadFile("cts.json")
	if err != nil {
		t.Fatalf("could not open cts.json: %s", err)
	}

	err = json.Unmarshal(contents, &payload)
	if err != nil {
		t.Fatalf("could not unmarshal JSON: %s", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6412",
	})

	for _, test := range payload {
		if test.Skipped {
			continue
		}

		err = rdb.FlushDB(context.Background()).Err()
		if err != nil {
			t.Fatalf("could not flush db: %s", err)
		}

		for index, command := range test.Command {
			var args []interface{}
			for _, arg := range strings.Split(command, " ") {
				args = append(args, arg)
			}

			result, err := rdb.Do(context.TODO(), args...).Result()

			if err != nil {
				t.Fatalf("could not run command %q: %s", command, err)
			}

			// json keeps integers as floats
			// so marshal/unmarshal to make the result
			contents, err := json.Marshal(result)
			if err != nil {
				t.Fatalf("could not marshal: %s", err)
			}

			var actual interface{}
			json.Unmarshal(contents, &actual)

			if diff := cmp.Diff(test.Result[index], actual); diff != "" {
				t.Fatalf("%q (-want +got):\n%s", test.Name, diff)
			}
		}
	}
}