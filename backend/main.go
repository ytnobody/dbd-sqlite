// dbd-sqlite is the SQLite driver backend for the DBI text-stream protocol.
//
// It is a long-lived process launched once per DBI connection. It reads
// NDJSON requests from stdin and writes NDJSON responses to stdout (see
// ../protocol.md / calcium-dbi). When stdin reaches EOF (the parent exited or
// closed the pipe), the backend shuts down, guaranteeing the child never
// outlives its parent.
//
// The driver is modernc.org/sqlite (pure Go, CGO-free).
package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

type backend struct {
	db    *sql.DB
	tx    *sql.Tx
	stmts map[int]*sql.Stmt
	next  atomic.Int64
}

func main() {
	if len(os.Args) < 2 {
		respond(nil, "dbd-sqlite: missing dsn argument")
		os.Exit(2)
	}
	dsn := os.Args[1]

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		respond(nil, fmt.Sprintf("dbd-sqlite: open: %v", err))
		os.Exit(2)
	}
	if err := db.Ping(); err != nil {
		respond(nil, fmt.Sprintf("dbd-sqlite: ping: %v", err))
		os.Exit(2)
	}
	// One backend = one DBI connection: pin a single underlying connection so
	// SQL-level transactions (BEGIN/COMMIT/ROLLBACK) are consistent.
	db.SetMaxOpenConns(1)

	b := &backend{db: db, stmts: map[int]*sql.Stmt{}}

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	out := bufio.NewWriter(os.Stdout)

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var req map[string]interface{}
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			respondLine(out, errResp(err))
			continue
		}
		op, _ := req["op"].(string)
		resp := b.handle(op, req)
		respondLine(out, resp)
	}
	if err := sc.Err(); err != nil {
		out.Flush()
		os.Exit(2)
	}

	// stdin closed or EOF: parent is gone; shut down cleanly.
	b.closeAll()
	db.Close()
	out.Flush()
	os.Exit(0)
}

func respondLine(w *bufio.Writer, m map[string]interface{}) {
	data, err := json.Marshal(m)
	if err != nil {
		data = []byte(`{"ok":false,"error":"dbd-sqlite: marshal error"}`)
	}
	w.Write(data)     //nolint:errcheck
	w.WriteByte('\n') //nolint:errcheck
	w.Flush()         //nolint:errcheck
}

// respond is used before the scanner loop starts (startup errors).
func respond(m map[string]interface{}, errMsg string) {
	if m == nil {
		m = map[string]interface{}{}
	}
	switch {
	case errMsg != "":
		m["ok"] = false
		m["error"] = errMsg
	default:
		m["ok"] = true
	}
	data, _ := json.Marshal(m)
	fmt.Println(string(data))
}

func errResp(err error) map[string]interface{} {
	return map[string]interface{}{"ok": false, "error": err.Error()}
}

func okResp(extra map[string]interface{}) map[string]interface{} {
	m := map[string]interface{}{"ok": true}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func (b *backend) handle(op string, req map[string]interface{}) map[string]interface{} {
	switch op {
	case "execute":
		return b.exec(req, "exec")
	case "query":
		return b.query(req, "query")
	case "query_row":
		return b.query(req, "query_row")
	case "prepare":
		return b.prepare(req)
	case "execute_prepared":
		return b.execPrepared(req, "exec")
	case "query_prepared":
		return b.execPrepared(req, "query")
	case "finish":
		return b.finishStmt(req)
	case "begin":
		return b.begin()
	case "commit":
		return b.commit()
	case "rollback":
		return b.rollback()
	case "ping":
		return okResp(map[string]interface{}{"pong": true, "driver": "sqlite", "version": "1.0.0"})
	case "close":
		b.closeAll()
		b.db.Close()
		os.Exit(0)
		return nil
	default:
		return map[string]interface{}{"ok": false, "error": fmt.Sprintf("dbd-sqlite: unknown op %q", op)}
	}
}

func (b *backend) sqlParams(v interface{}) ([]interface{}, error) {
	arr, ok := v.([]interface{})
	if !ok {
		return nil, errors.New("params must be an array")
	}
	return arr, nil
}

func (b *backend) exec(req map[string]interface{}, kind string) map[string]interface{} {
	sqlStr, ok := req["sql"].(string)
	if !ok {
		return errResp(errors.New("missing sql string"))
	}
	params, err := b.sqlParams(req["params"])
	if err != nil {
		return errResp(err)
	}
	var res sql.Result
	if b.tx != nil {
		res, err = b.tx.Exec(sqlStr, params...)
	} else {
		res, err = b.db.Exec(sqlStr, params...)
	}
	if err != nil {
		return errResp(err)
	}
	rowsAffected, _ := res.RowsAffected()
	lastID, _ := res.LastInsertId()
	return okResp(map[string]interface{}{"rows_affected": rowsAffected, "last_insert_id": lastID})
}

func (b *backend) query(req map[string]interface{}, kind string) map[string]interface{} {
	sqlStr, ok := req["sql"].(string)
	if !ok {
		return errResp(errors.New("missing sql string"))
	}
	params, err := b.sqlParams(req["params"])
	if err != nil {
		return errResp(err)
	}

	var rows *sql.Rows
	if b.tx != nil {
		rows, err = b.tx.Query(sqlStr, params...)
	} else {
		rows, err = b.db.Query(sqlStr, params...)
	}
	if err != nil {
		return errResp(err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	all := []map[string]interface{}{}
	for rows.Next() {
		row := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range row {
			ptrs[i] = &row[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return errResp(err)
		}
		m := map[string]interface{}{}
		for i, c := range cols {
			m[c] = normalize(row[i])
		}
		all = append(all, m)
		if kind == "query_row" {
			return okResp(map[string]interface{}{"row": all[0]})
		}
	}
	if err := rows.Err(); err != nil {
		return errResp(err)
	}
	return okResp(map[string]interface{}{"columns": cols, "rows": all})
}

func (b *backend) prepare(req map[string]interface{}) map[string]interface{} {
	sqlStr, ok := req["sql"].(string)
	if !ok {
		return errResp(errors.New("missing sql string"))
	}
	var stmt *sql.Stmt
	var err error
	if b.tx != nil {
		stmt, err = b.tx.Prepare(sqlStr)
	} else {
		stmt, err = b.db.Prepare(sqlStr)
	}
	if err != nil {
		return errResp(err)
	}
	id := int(b.next.Add(1))
	b.stmts[id] = stmt
	return okResp(map[string]interface{}{"stmt": id})
}

func (b *backend) finishStmt(req map[string]interface{}) map[string]interface{} {
	id, ok := req["stmt"].(float64)
	if !ok {
		return errResp(errors.New("missing stmt id"))
	}
	stmt := b.stmts[int(id)]
	if stmt != nil {
		stmt.Close() //nolint:errcheck
		delete(b.stmts, int(id))
	}
	return okResp(nil)
}

func (b *backend) execPrepared(req map[string]interface{}, mode string) map[string]interface{} {
	id, ok := req["stmt"].(float64)
	if !ok {
		return errResp(errors.New("missing stmt id"))
	}
	stmt := b.stmts[int(id)]
	if stmt == nil {
		return errResp(errors.New("unknown statement id"))
	}
	params, err := b.sqlParams(req["params"])
	if err != nil {
		return errResp(err)
	}
	if mode == "exec" {
		res, err := stmt.Exec(params...)
		if err != nil {
			return errResp(err)
		}
		rowsAffected, _ := res.RowsAffected()
		lastID, _ := res.LastInsertId()
		return okResp(map[string]interface{}{"rows_affected": rowsAffected, "last_insert_id": lastID})
	}
	return b.queryRows(stmt, params)
}

func (b *backend) queryRows(stmt *sql.Stmt, params []interface{}) map[string]interface{} {
	rows, err := stmt.Query(params...)
	if err != nil {
		return errResp(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	all := []map[string]interface{}{}
	for rows.Next() {
		row := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range row {
			ptrs[i] = &row[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return errResp(err)
		}
		m := map[string]interface{}{}
		for i, c := range cols {
			m[c] = normalize(row[i])
		}
		all = append(all, m)
	}
	if err := rows.Err(); err != nil {
		return errResp(err)
	}
	return okResp(map[string]interface{}{"columns": cols, "rows": all})
}

func (b *backend) begin() map[string]interface{} {
	if b.tx != nil {
		return errResp(errors.New("transaction already active"))
	}
	tx, err := b.db.Begin()
	if err != nil {
		return errResp(err)
	}
	b.tx = tx
	return okResp(nil)
}

func (b *backend) commit() map[string]interface{} {
	if b.tx == nil {
		return errResp(errors.New("no active transaction"))
	}
	err := b.tx.Commit()
	b.tx = nil
	if err != nil {
		return errResp(err)
	}
	return okResp(nil)
}

func (b *backend) rollback() map[string]interface{} {
	if b.tx == nil {
		return errResp(errors.New("no active transaction"))
	}
	err := b.tx.Rollback()
	b.tx = nil
	if err != nil {
		return errResp(err)
	}
	return okResp(nil)
}

func (b *backend) closeAll() {
	for id, stmt := range b.stmts {
		stmt.Close() //nolint:errcheck
		delete(b.stmts, id)
	}
}

func normalize(v interface{}) interface{} {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.Format(time.RFC3339)
	default:
		return v
	}
}
