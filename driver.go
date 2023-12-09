package driver

import (
  "context"
  "database/sql"
  "fmt"
  "strconv"
  "time"
  _ "github.com/go-sql-driver/mysql"
)

const (
  MySQL            = "mysql"
  keyCreated       = "created"
  All              = -1
)

// Defines: Driver single database configuration (UNIX socket specific)
type Driver struct {
  context     context.Context
  database    string
  sqlDB       *sql.DB
}

// Defines: Interface for table lookup
type Tables interface {
  RecordTable() string
  ContentTable() string
}

// Defines: Interface for a type which can be queried
type Queryable interface {
  QueryGetRows(z *Table) string
  QueryGetRow(z *Table) string
  QueryInsertContentRow(z *Table, timeStamp time.Time) string
  QueryInsertRecordRow(z *Table, cID int) string
  QueryUpdateRow(z *Table, timeStamp time.Time) string
  QueryDeleteRow(z *Table) string
}

// Defines: Interface for a type which can be read from sql members
type SQLType [T any] interface {
  FromRowMeta(rows *sql.Row) (T, error)
  FromRowFull(rows *sql.Row) (T, error)
  FromNewRecord(timeStamp time.Time, rID int) T
  FromUpdatedRecord(timeStamp time.Time) T
}

// DSN compiles data source name (DSN) from arguments for go-sql-driver
func DSN (unixSocket, username, password, database string) string {
  return fmt.Sprintf("%s:%s@unix(%s)/%s", username, password, unixSocket,
    database)
}

// Init opens and validates the data source name
func (d *Driver) Init (unixSocket, username, password, database string) (string, error) {
  dsn := DSN(unixSocket, username, password, database)
  db, err := sql.Open(MySQL, dsn)
  if nil != err {
    return dsn, fmt.Errorf("Bad Open with DSN %s: %w", dsn, err)
  }
  err = db.Ping()
  if nil != err {
    return dsn, fmt.Errorf("Bad Ping with DSN %s: %w", dsn, err)
  }
  d.context = context.Background()
  d.database = database
  d.sqlDB = db
  return dsn, nil
}

// Stop closes the DB and prevents new queries 
// (waits for outstanding ones to finish first)
func (d *Driver) Stop () error {
  err := d.sqlDB.Close()
  if nil != err {
    return fmt.Errorf("Bad Close for DB %s: %w", d.database, err)
  }
  return nil
}

// StaticPage returns the content of a named page which is 
// looked up in the page content table cTable using the
// given hash table hTable
func (d *Driver) StaticPage (cTable, hTable, name string) ([]byte, error) {
  query := fmt.Sprintf(
    "SELECT body FROM %s WHERE id = " +
    "(SELECT content_id FROM %s WHERE url_hash = unhex(md5(\"%s\")))",
    cTable, hTable, name)
  buffer := []byte{}
  rows, err := d.sqlDB.Query(query)
  defer rows.Close()
  if nil != err {
    return buffer, fmt.Errorf("Bad query %s: %w", query, err)
  }
  if rows.Next() {
    err = rows.Scan(&buffer)
  } else {
    return buffer, fmt.Errorf("Bad query %s: no rows!", query)
  }
  if nil != err {
    return buffer, fmt.Errorf("Bad row scan: %w", err)
  }
  return buffer, nil
}

// Rows returns a slice of rows from the specified database tables.
// q: Queryable type
// z: Pointer to type implementing Tables interface
func (d *Driver) Rows [T SQLType[T], P interface{*T;Queryable}] (q P, z *Tables) ([]T, error) {
  t, ts, query := SQLType(*q), []T{}, q.QueryGetRows(z)
  rows, err := d.sqlDB.Query(query)
  defer rows.Close()
  if nil != err {
    return ts, fmt.Errorf("Bad query %s: %w", query, err)
  }
  for rows.Next() {
    t, err = t.FromRowMeta(rows)
    if nil != err {
      return ts, fmt.Errorf("Bad row scan: %w", err)
    }
    ts = append(ts, t)
  }
  return ts, nil
}

// Row returns a row from the specified database tables.
// q: Queryable type. Used here to provide key information to query
// z: Pointer to type implementing the Tables interface
func (d *Driver) Row [T SQLType[T], P interface{*T;Queryable}] (q P, z *Tables) (T, error) {
  t, query := SQLType(*q), q.QueryGetRow(z)
  rows, err := d.sqlDB.Query(query)
  defer rows.Close()
  if nil != err {
    return t, fmt.Errorf("Bad query %s: %w", query, err)
  }
  if rows.Next() {
    t, err = t.FromRowFull(rows)
  } else {
    return t, fmt.Errorf("Bad query %s: no such row!", query)
  }
  if nil != err {
    return t, fmt.Errorf("Bad row scan: %w", err)
  }
  return t, nil
}

// Insert inserts the given type into the tables, and returns the new type data
// q: Queryable type. Used here to contain the data to be inserted
// z: Pointer to type implementing the Tables interface
func (d *Driver) Insert [T SQLType[T], P interface{*T;Queryable}] (q P, z *Tables) (T, error) {
  t, rID, cID, timeStamp := SQLType(*q), int64(-1), int64(-1), time.Now().UTC()

  fail := func(err error) (T, error) {
    return T{}, fmt.Errorf("Bad insert: %v", err)
  }

  // Prepare transaction
  tx, err := d.sqlDB.BeginTx(d.context, nil)
  if nil != err {
    return fail(err)
  }
  defer tx.Rollback() // Rollback has no effect if transaction succeeds
  
  // Insert content; fail on bad res(ult)
  res, err := tx.ExecContext(d.context, q.QueryInsertContentRow(z, timeStamp))
  if nil != res {
    return fail(err)
  }

  // Fetch last insert id (content)
  cID, err = res.LastInsertId()
  if nil != err {
    return fail(err)
  }

  // Insert record; fail on bad res(ult)
  res, err := tx.ExecContext(d.context, q.QueryInsertRecordRow(z, cID))
  if nil != res {
    return fail(err)
  }

  // Fetch last insert id (record)
  rID, err = res.LastInsertId()
  if nil != err {
    return fail(err)
  }

  // Commit transaction
  if err = tx.Commit(); nil != err {
    return fail(err)
  }

  // Return new type
  return t.FromNewRecord(timeStamp, rID), nil
}

// Updates the given type in the tables, and returns the updated type data
// q: Queryable type. Used here to contain the data to be updated
// z: Pointer to type implementing the Tables interface
func (d *Driver) Update [T SQLType[T], P interface{*T;Queryable}] (q P, z *Tables) (T, error) {
  t, timeStamp := SQLType(*q), time.Now().UTC()

  fail := func (err error) (T, error) {
    return T{}, fmt.Errorf("Bad update: %v", err)
  }

  // Reserve connection
  conn, err := d.sqlDB.Conn(d.context)
  if nil != err {
    return fail(err)
  }
  defer conn.Close()

  // Update tables
  res, err := conn.ExecContext(d.context, q.QueryUpdateRow(z, timeStamp))
  if nil != err {
    return fail(err)
  }

  // Verify rows affected
  rows, err := res.RowsAffected()
  if nil != err {
    return fail(err)
  }
  if 0 == rows {
    return fail(fmt.Errorf("Expected at least 1 row affected, got 0"))
  }
  return t.FromUpdatedRecord(timeStamp), nil
}

func (d *Driver) Delete [T SQLType[T], P interface{*T;Queryable}] (q P, z *Tables) error {

  fail := func (err error) error {
    return fmt.Errorf("Bad delete: %v", err)
  }

  // Reserve connection
  conn, err := d.sqlDB.Conn(d.context)
  if nil != err {
    return fail(err)
  }

  // Delete from tables
  res, err := conn.ExecContext(d.context, q.QueryDeleteRow(z))
  if nil != err {
    return fail(err)
  }

  // Verify rows affected
  rows, err := res.RowsAffected()
  if nil != err {
    return fail(err)
  }
  if 2 != rows {
    return fail(fmt.Errorf("Expected 2 rows affected, got %d", rows))
  }

  return nil
}

