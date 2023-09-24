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

// Defines: Page structure
type Page struct {
  ID          string `json:"id"`
  Title       string `json:"title"`
  Subtitle    string `json:"subtitle"`
  Tag         string `json:"tag"`
  Created     string `json:"created"` 
  Updated     string `json:"updated"` 
  Filename    string `json:"filename"`
  Body        string `json:"body"`
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


// IndexedPages returns all pages from the given indexed table
// rTable (record table) ordered by creation date using the 
// cTable (content table)
func (d *Driver) IndexedPages (rTable, cTable string) ([]Page, error) {
  query := fmt.Sprintf(
    "SELECT a.id, a.title, a.subtitle, a.tag, b.created, b.updated " +
    "FROM %s AS a INNER JOIN %s AS B " +
    "ON a.content_id = b.id " +
    "ORDER BY b.created", rTable, cTable)
  pages := []Page{}
  rows, err := d.sqlDB.Query(query)
  defer rows.Close()
  if nil != err {
    return pages, fmt.Errorf("Bad query %s: %w", query, err)
  }
  for rows.Next() {
    var p Page
    err = rows.Scan(&p.ID, &p.Title, &p.Subtitle, &p.Tag, &p.Created,
      &p.Updated)
    if nil != err {
      return pages, fmt.Errorf("Bad row scan: %w", err)
    }
    pages = append(pages, p)
  }
  return pages, nil
}

// IndexedPage returns the body of an indexed page from the given 
// cTable (content table) using the given id
func (d *Driver) IndexedPage (rTable, cTable, id string) (Page, error) {
  var p Page;
  query := fmt.Sprintf(
    "SELECT a.id, a.title, a.subtitle, a.tag, b.created, b.updated, b.body " +
    "FROM %s AS a INNER JOIN %s AS b ON a.content_id = b.id " +
    "WHERE a.id = %s", rTable, cTable, id)
  rows, err := d.sqlDB.Query(query)
  defer rows.Close()
  if nil != err {
    return p, fmt.Errorf("Bad query %s: %w", query, err)
  }
  fmt.Println("Scanning rows ...")
  if rows.Next() {
    err = rows.Scan(&p.ID, &p.Title, &p.Subtitle, &p.Tag, &p.Created,
      &p.Updated, &p.Body)
  } else {
    return p, fmt.Errorf("Bad query %s: no rows!", query)
  }
  if nil != err {
    return p, fmt.Errorf("Bad row scan: %w", err)
  }
  return p, nil
}

// InsertIndexedPage inserts the given Page form, and returns the new Page data
func (d *Driver) InsertIndexedPage (rTable, cTable string, form Page) (Page, error) {
  blog_id, content_id := int64(-1), int64(-1)
  
  fail := func(err error) (Page, error) {
    return Page{}, fmt.Errorf("Bad insert: %v", err)
  }

  query := fmt.Sprintf("INSERT INTO %s (created,updated,body) VALUES (?,?,?)", cTable)

  // Prepare transactions
  t, err := d.sqlDB.BeginTx(d.context, nil)
  if nil != err {
    return fail(err)
  }
  defer t.Rollback() // Rollback has no effect if transaction succeeds

  // Perform insert
  now := time.Now()
  r, err := t.ExecContext(d.context, query, now, now, form.Body)
  if nil != err {
    return fail(err)
  }

  // Get the content ID
  content_id, err = r.LastInsertId()
  if nil != err {
    return fail(err)
  }

  // Prepare secondary commit
  query = fmt.Sprintf("INSERT INTO %s (title,subtitle,tag,content_id) VALUES (?,?,?,?)", rTable)
  r, err = t.ExecContext(d.context, query, form.Title, form.Subtitle, form.Tag, content_id)
  if nil != err {
    return fail(err)
  }

  // Get last insert ID
  blog_id, err = r.LastInsertId()
  if nil != err {
    return fail(err)
  }

  // Commit the transaction
  if err = t.Commit(); nil != err {
    return fail(err)
  }

  // Return the Page
  blog_id_str, date_str := strconv.FormatInt(blog_id, 10), now.Format("2006-01-02")
  return Page {
    ID:       blog_id_str,
    Title:    form.Title,
    Subtitle: form.Subtitle,
    Tag:      form.Tag,
    Created:  date_str,
    Updated:  date_str,
    Filename: "",
  }, nil
}

func (d *Driver) UpdateIndexedPage (rTable, cTable string, form Page) (Page, error) {
  
  fail := func (err error) (Page, error) {
    return Page{}, fmt.Errorf("Bad delete: %v", err)
  }

  query := fmt.Sprintf(
    "UPDATE %s LEFT JOIN %s on %s.content_id = %s.id " +
    "SET %s.title = ?, %s.subtitle = ?, %s.updated = ?, %s.body = ? " +
    "WHERE %s.id = ?", rTable, cTable, rTable, cTable, rTable, rTable, cTable,
     cTable, rTable);

  // Reserve connection
  c, err := d.sqlDB.Conn(d.context)
  if nil != err {
    return fail(err)
  }
  defer c.Close()

  // Perform operation
  now := time.Now()
  result, err := c.ExecContext(d.context, query, form.Title, form.Subtitle,
    now, form.Body, form.ID)
  if nil != err {
    return fail(err)
  }

  // Retrieve affected rows
  rows, err := result.RowsAffected()
  if nil != err {
    return fail(err)
  }
  if 0 == rows {
    return fail(fmt.Errorf("Expected at least 1 row affected, got 0"))
  }

  date_str := now.Format("2006-01-02")
  return Page {
    ID:       form.ID,
    Title:    form.Title,
    Subtitle: form.Subtitle,
    Tag:      form.Tag,
    Created:  form.Created,
    Updated:  date_str,
  }, nil
}

func (d *Driver) DeleteIndexedPage (rTable, cTable string, form Page) error {
  
  fail := func (err error) error {
    return fmt.Errorf("Bad delete: %v", err)
  }

  query := fmt.Sprintf(
    "DELETE %s, %s FROM %s INNER JOIN %s " + 
    "ON %s.content_id = %s.id WHERE %s.id = %s", 
    rTable, cTable, rTable, cTable, rTable, cTable, rTable, form.ID);

  // Reserve connection
  c, err := d.sqlDB.Conn(d.context)
  if nil != err {
    return fail(err)
  }
  defer c.Close()

  // Perform operation
  result, err := c.ExecContext(d.context, query)
  if nil != err {
    return fail(err)
  }

  // Retrieve affected rows
  rows, err := result.RowsAffected()
  if nil != err {
    return fail(err)
  }
  if 2 != rows {
    return fail(fmt.Errorf("Expected 2 rows to be affected, got %d\n", rows))
  }
  return nil
}

//func (p page) Created () time.Time {
//  t, err := time.Parse("2006-01-02", p.created)
//  // TODO: Failover hard or return default time like
//  //       time.Unix(0,0)
//  if nil != err {
//    return time.Unix(0,0)
//    //panic(err.Error())
//  }
//  return t
//}

