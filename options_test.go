package kvlite

import (
	"reflect"
	"testing"
)

func TestOptions_DoesNotExposePageSize(t *testing.T) {
	if _, ok := reflect.TypeOf(Options{}).FieldByName("PageSize"); ok {
		t.Fatal("Options exposes PageSize")
	}
}
