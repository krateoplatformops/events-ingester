package pg

import (
	"fmt"
	"testing"
)

func TestConnectionURL(t *testing.T) {
	params := map[string]string{
		"sslmode": "disable",
	}

	dsn, err := ConnectionURL(
		"test",
		"test",
		"postgres.demo-system.svc.cluster.local",
		5432,
		"testdb",
		params,
	)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(dsn)
}
