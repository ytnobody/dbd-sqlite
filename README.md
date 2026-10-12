# NAME

`dbd.sqlite` - SQLite driver adapter for the Calcium DBI interface

# DESCRIPTION

`dbd.sqlite` provides a **SQLite driver** for the [`calcium-dbi`] DBI interface.
It pairs a tiny **Go backend** (`backend/`, embedding SQLite via
`modernc.org/sqlite` -- pure Go, CGO-free) with a **pure-Calcium adapter**
(`mod.ca`) that satisfies the `calcium-dbi` adapter contract.

The adapter launches the backend as a long-lived process and talks to it over
the DBI **text-stream protocol** (line-delimited JSON on stdin/stdout) using
`core.os`. No SQLite logic lives in Calcium, and no database code lives in
`calcium-lang` core.

```
+----------------+     NDJSON/stdio     +-----------------+       SQLite
| Calcium (DBI)  | <-----------------> | dbd-sqlite (Go) | <-->  .db file
| adapter (pure) |  core.os.proc_*     | backend         |
+----------------+                     +-----------------+
```

# INSTALLATION

## 1. Build the backend

The backend is a standalone Go binary (the only place SQLite/Go code lives):

```bash
cd backend
go build -o dbd-sqlite .
# put it on PATH, or pass its path to the adapter via options.backend
```

## 2. Use the module

```calcium
use dbi;          // calcium-dbi (pure-Calcium interface)
use dbd.sqlite;   // resolves dbd/sqlite -> sqlite.*
use core.io!;

adapter = sqlite.adapter({backend: "dbd-sqlite"});  // PATH lookup

dbh = dbi.connect(adapter, "app.db", {}) !? {
    success(v) => v
    failure(e) => { io.println(concat("connect failed: ", e)); }
};

dbi.execute(dbh, "CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT)", []);
dbi.execute(dbh, "INSERT INTO users(name) VALUES (?)", ["alice"]);
rows = dbi.query(dbh, "SELECT id, name FROM users", []) !? {
    success(v) => v
    failure(e) => []
};
rows |> map(row => io.println(row))
dbi.disconnect(dbh);
```

# ADAPTER

`sqlite.adapter(options)` returns a hash that can be passed to
`dbi.connect`. It implements the `calcium-dbi` adapter contract.

`options`:
- `backend` - path/name of the backend executable (default `dbd-sqlite`).

The backend process is launched once per connection and is terminated when the
parent program/REPL exits (and on `os.exit`), so it never outlives its parent.
A backend also exits on its own when its stdin reaches EOF.

# TESTS

```bash
cd backend && go build -o /tmp/dbd-sqlite .
cd ..
mkdir -p dbd/sqlite && ln -sf ../../mod.ca dbd/sqlite/mod.ca
calcium mod.test.ca
```

The integration test (`mod.test.ca`) runs the real backend through the adapter
and covers DDL/DML, query, prepared statements, transactions (BEGIN/ROLLBACK/
COMMIT), and error handling.

# CONSTRAINT GENERATION FROM SCHEMA TYPES

`dbi.query_meta(dbh, sql, params)` returns `{columns, column_types, rows}`.
SQLite-specific type information is turned into per-column validators here in
the DBD (driver), not in the generic DBI layer:

```calcium
meta = dbi.query_meta(dbh, "SELECT id, name, age FROM users", []) !? {
    success(v) => v failure(e) => {}
};

sc = sqlite.schema(meta);       // { age: {dbtype:.., type_of:.., is: <fn>}, ... }

sqlite.validate(sc, {id: 1, name: "alice", age: 30}) !? {
    success(_) => io.println("row ok")
    failure(e) => io.println(e)   // column 'INTEGER' type violation for value ...
};
```

`schema(meta)` maps each column to `{dbtype, type_of, is}` where `is` is a
validator function (`value -> bool`). Type mapping follows SQLite affinity
(`INTEGER`->int, `REAL/FLOAT/DOUBLE`->float, `BOOL`->bool, `TEXT/CHAR/CLOB`->
string, `NUMERIC`->number, `BLOB`->string) and also recognises the Go
reflection names the driver reports for expressions / untyped columns
(`int64`, `float64`, `string`, `bool`, `[]uint8`). There is **no permissive
"any" constraint**: an unknown / untyped column is `type_of: "unknown"` and
its validator rejects (fail-closed), so unverifiable columns are surfaced
rather than silently accepted.

# NOTES

- The backend keeps a single underlying SQLite connection so SQL-level
  transactions are consistent.
- The protocol is documented in the `calcium-dbi` repository
  (`protocol.md`); any RDBMS can be added with another small backend that
  speaks it.

[`calcium-dbi`]: https://github.com/ytnobody/calcium-dbi