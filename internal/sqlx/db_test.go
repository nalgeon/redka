package sqlx

import "testing"

func TestDataSource_Postgres(t *testing.T) {
	t.Run("rw", func(t *testing.T) {
		path := "postgres://redka:redka@localhost:5432/redka"
		ds := DataSource(path, false, &Options{Dialect: DialectPostgres})
		want := "postgres://redka:redka@localhost:5432/redka"
		if ds != want {
			t.Errorf("expected %s, got %s", want, ds)
		}
	})
	t.Run("ro", func(t *testing.T) {
		path := "postgres://redka:redka@localhost:5432/redka"
		ds := DataSource(path, true, &Options{Dialect: DialectPostgres})
		want := "postgres://redka:redka@localhost:5432/redka?default_transaction_read_only=on"
		if ds != want {
			t.Errorf("expected %s, got %s", want, ds)
		}
	})
	t.Run("pragma", func(t *testing.T) {
		path := "postgres://redka:redka@localhost:5432/redka"
		pragma := map[string]string{"sslmode": "disable"}
		ds := DataSource(path, false, &Options{Dialect: DialectPostgres, Pragma: pragma})
		want := "postgres://redka:redka@localhost:5432/redka?sslmode=disable"
		if ds != want {
			t.Errorf("expected %s, got %s", want, ds)
		}
	})
}

func TestDataSource_Sqlite(t *testing.T) {
	t.Run("memory rw", func(t *testing.T) {
		path := ":memory:"
		ds := DataSource(path, false, &Options{Dialect: DialectSqlite})
		want := "file:/redka.db?_mutex=no&_txlock=immediate&vfs=memdb"
		if ds != want {
			t.Errorf("expected %s, got %s", want, ds)
		}
	})
	t.Run("memory ro", func(t *testing.T) {
		path := ":memory:"
		ds := DataSource(path, true, &Options{Dialect: DialectSqlite})
		want := "file:/redka.db?_mutex=no&mode=ro&vfs=memdb"
		if ds != want {
			t.Errorf("expected %s, got %s", want, ds)
		}
	})
	t.Run("file rw", func(t *testing.T) {
		path := "redka.db"
		ds := DataSource(path, false, &Options{Dialect: DialectSqlite})
		want := "file:redka.db?_mutex=no&_txlock=immediate"
		if ds != want {
			t.Errorf("expected %s, got %s", want, ds)
		}
	})
	t.Run("file ro", func(t *testing.T) {
		path := "file:redka.db"
		ds := DataSource(path, true, &Options{Dialect: DialectSqlite})
		want := "file:redka.db?_mutex=no&mode=ro"
		if ds != want {
			t.Errorf("expected %s, got %s", want, ds)
		}
	})
	t.Run("pragma", func(t *testing.T) {
		path := "file:redka.db"
		pragma := map[string]string{
			"journal_mode": "wal",
			"synchronous":  "normal",
		}
		ds := DataSource(path, false, &Options{Dialect: DialectSqlite, Pragma: pragma})
		want1 := "file:redka.db?_mutex=no&_pragma=journal_mode%3Dwal&_pragma=synchronous%3Dnormal&_txlock=immediate"
		want2 := "file:redka.db?_mutex=no&_pragma=synchronous%3Dnormal&_pragma=journal_mode%3Dwal&_txlock=immediate"
		if ds != want1 && ds != want2 {
			t.Errorf("expected %s, got %s", want1, ds)
		}
	})
}
