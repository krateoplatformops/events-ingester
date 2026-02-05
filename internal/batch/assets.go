package batch

import (
	"embed"
	"strings"
)

//go:embed assets/*.sql
var sqlFS embed.FS

func MustLoadSQL(filename string) string {
	if !strings.HasPrefix(filename, "assets") {
		filename = "assets/" + filename
	}

	b, err := sqlFS.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	return strings.TrimSpace(string(b))
}
