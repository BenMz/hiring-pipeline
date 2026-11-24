// Package migrations embeds the SQL schema of the read models.
package migrations

import (
	"embed"
	"io/fs"
	"sort"
)

//go:embed *.sql
var files embed.FS

// ReadModelTables lists every table the projector owns, in drop order.
var ReadModelTables = []string{"projector_offsets", "stage_counts", "applications_by_job", "jobs"}

// SQL returns the migration scripts in order.
func SQL() ([]string, error) {
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	scripts := make([]string, 0, len(names))
	for _, n := range names {
		b, err := files.ReadFile(n)
		if err != nil {
			return nil, err
		}
		scripts = append(scripts, string(b))
	}
	return scripts, nil
}
