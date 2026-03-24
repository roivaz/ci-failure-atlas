package migrations

import "testing"

func TestLoadUpMigrationsReturnsSortedList(t *testing.T) {
	t.Parallel()

	loaded, err := loadUpMigrations()
	if err != nil {
		t.Fatalf("loadUpMigrations returned unexpected error: %v", err)
	}
	if len(loaded) == 0 {
		t.Fatalf("expected at least one embedded migration")
	}
	for i := 1; i < len(loaded); i++ {
		if loaded[i-1].name > loaded[i].name {
			t.Fatalf("migrations are not sorted: %q > %q", loaded[i-1].name, loaded[i].name)
		}
	}
}
