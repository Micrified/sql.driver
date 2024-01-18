package driver

import (
  "database/sql"
  "fmt"
  "strconv"
  "time"
)

// Defines: Paste structure
type Paste struct {
  ID          string `json:"id"`
  Filename    string `json:"filename"`
  Filetype    string `json:"filetype"`
  Created     string `json:"created"`
  Updated     string `json:"updated"`
  Body        string `json:"body"`
}

func (p *Paste) QueryGetRows(z Tables) string {
  rTable, cTable := z.RecordTable(), z.ContentTable()
  q := "SELECT a.id, a.filename, a.filetype, b.created, b.updated " + 
       "FROM %s AS a INNER JOIN %s AS b " +
       "ON a.content_id = b.id " + 
       "ORDER BY b.created"
  return fmt.Sprintf(q, rTable, cTable)
}

func (p *Paste) QueryGetRow(z Tables) string {
  rTable, cTable := z.RecordTable(), z.ContentTable()
  q := "SELECT a.id, a.filename, a.filetype, b.created, b.updated, b.body " +
       "FROM %s AS a INNER JOIN %s AS b ON a.content_id = b.id " +
       "WHERE a.id = %s"
  return fmt.Sprintf(q, rTable, cTable, p.ID)
}

func (p *Paste) QueryInsertContentRow(z Tables, timeStamp time.Time) string {
  t, cTable := timeStamp.Format("2006-01-02 15:04:05"), z.ContentTable()
  q := "INSERT INTO %s (created,updated,body) VALUES ('%s','%s','%s')"
  return fmt.Sprintf(q, cTable, t, t, escapeSQL(p.Body))
}

func (p *Paste) QueryInsertRecordRow(z Tables, cID int64) string {
  rTable := z.RecordTable()
  q := "INSERT INTO %s (filename,filetype,content_id) " + 
       "VALUES ('%s','%s',%d)"
  return fmt.Sprintf(q, rTable, p.Filename, p.Filetype, cID)
}

func (p *Paste) QueryUpdateRow(z Tables, timeStamp time.Time) string {
  rTable, cTable := z.RecordTable(), z.ContentTable()
  t := timeStamp.Format("2006-01-02 15:04:05")
  q := "UPDATE %s LEFT JOIN %s ON %s.content_id = %s.id " + 
       "SET %s.filename = '%s', %s.filetype = '%s', %s.updated = '%s', %s.body = \"%s\" " +
       "WHERE %s.id = %s"
  return fmt.Sprintf(q, rTable, cTable, rTable, cTable, rTable, p.Filename,
    rTable, p.Filetype, cTable, t, cTable, escapeSQL(p.Body), rTable, p.ID)
}
func (p *Paste) QueryDeleteRow(z Tables) string {
  rTable, cTable := z.RecordTable(), z.ContentTable()
  q := "DELETE %s, %s FROM %s INNER JOIN %s " +
       "ON %s.content_id = %s.id WHERE %s.id = %s"
  return fmt.Sprintf(q, rTable, cTable, rTable, cTable, rTable, cTable, rTable,
    p.ID)
}

func (p Paste) FromRowMeta(rows *sql.Rows) (Paste, error) {
  err := rows.Scan(&p.ID, &p.Filename, &p.Filetype, &p.Created, &p.Updated)
  return p, err
}

func (p Paste) FromRowFull(rows *sql.Rows) (Paste, error) {
  err := rows.Scan(&p.ID, &p.Filename, &p.Filetype, &p.Created, &p.Updated,
    &p.Body)
  return p, err
}

func (p Paste) FromNewRecord(timeStamp time.Time, rID int64) Paste {
  p.Created = timeStamp.Format("2006-01-02 15:04:05")
  p.Updated = p.Created
  p.ID = strconv.FormatInt(rID, 10)
  return p
}

func (p Paste) FromUpdatedRecord(timeStamp time.Time) Paste {
  p.Updated = timeStamp.Format("2006-01-02 15:04:05")
  return p
}
