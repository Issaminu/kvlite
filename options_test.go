package kvlite

import (
	"errors"
	"reflect"
	"testing"
)

func TestOptions_DoesNotExposePageSize(t *testing.T) {
	if _, ok := reflect.TypeOf(Options{}).FieldByName("PageSize"); ok {
		t.Fatal("Options exposes PageSize")
	}
}

func TestOptions_DisableWAL(t *testing.T) {
	for _, mode := range []Sync{SyncDefault, SyncFull, SyncNormal, SyncNone} {
		for _, disableWAL := range []bool{false, true} {
			options := Options{Synchronous: mode, DisableWAL: disableWAL}
			resolved, err := resolveOptions(&options, 4096)
			if disableWAL && mode != SyncNone {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("options %+v: got %v, want ErrInvalid", options, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("options %+v: %v", options, err)
			}
			if resolved.DisableWAL != disableWAL {
				t.Fatalf("options %+v: resolved DisableWAL = %t", options, resolved.DisableWAL)
			}
		}
	}
}

func TestOptions_MaxWriteBatchSize(t *testing.T) {
	for _, test := range []struct {
		name  string
		input int
		want  int
	}{
		{name: "default", input: 0, want: 100},
		{name: "one", input: 1, want: 1},
		{name: "custom", input: 256, want: 256},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := resolveOptions(&Options{MaxWriteBatchSize: test.input}, 4096)
			if err != nil {
				t.Fatal(err)
			}
			if resolved.MaxWriteBatchSize != test.want {
				t.Fatalf("maximum write batch size: got %d, want %d", resolved.MaxWriteBatchSize, test.want)
			}
		})
	}
	if _, err := resolveOptions(&Options{MaxWriteBatchSize: -1}, 4096); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative maximum write batch size: got %v, want ErrInvalid", err)
	}
}
