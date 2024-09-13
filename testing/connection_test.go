package testing

import (
	"testing"

	"github.com/rizalarfiyan/minio_csv_to_json/utils"
)

func TestConnection(t *testing.T) {
	con, err := utils.GetMinio()
	if err != nil {
		t.Error(err)
	}

	if con == nil {
		t.Error("Connection is nil")
	}
}
