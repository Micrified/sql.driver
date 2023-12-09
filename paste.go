package driver

// Defines: Paste structure
type Paste struct {
  ID          string `json:"id"`
  Filename    string `json:"filename"`
  Filetype    string `json:"filetype"`
  Created     string `json:"created"`
  Updated     string `json:"updated"`
  Body        string `json:"body"`
}

func (p *Paste) QueryGetRows(rTable, cTable string) string {
  q := "SELECT a.id, a.filename, a.filetype, b.created, b.updated " + 
       "FROM %s AS a INNER JOIN %s AS b " +
       "ON a.content_id = b.id " + 
       "ORDER BY b.created"
  return fmt.Sprintf(q, rTable, cTable)
}
func (p *Paste) QueryGetRow(rTable, cTable) string {
  q := "SELECT a.id, a.filename, a.filetype, b.created, b.updated, b.body " +
       "FROM %s AS a INNER JOIN %s AS b ON a.content_id = b.id " +
       "WHERE a.id = %s"
  return fmt.Sprintf(q, rTable, cTable, p.ID)
}
func (p *Paste) QueryInsertContentRow(cTable string, t time.Time) string {
  q := "INSERT INTO %s (created,updated,body) VALUES ('%s','%s','%s')"
  now := t.Format("2006-01-02 15:04:05")
  return fmt.Sprintf(q, cTable, now, now, p.Body)
}
func (p *Paste) QueryInsertRecordRow(rTable string, cID int) string {
  q := "INSERT INTO %s (filename,filetype,content_id) " + 
       "VALUES ('%s','%s',%d)"
  return fmt.Sprintf(q, rTable, p.Filename, p.Filetype, cID)
  return fmt.Sprintf(q, rTable)
}
func (p *Paste) QueryUpdateRow(rTable, cTable string) string {
  q := "UPDATE %s LEFT JOIN %s ON %s.content_id = %s.id " + 
       "SET %s.filename = ?, %s.filetype = ?, %s.updated = ?, %s.body = ? " +
       "WHERE %s.id = ?"
  return fmt.Sprintf(q, rTable, cTable, rTable, cTable, rTable, rTable, cTable,
    cTable, rTable)
}
func (p *Paste) QueryDeleteRow(rTable, cTable string) string {
  q := "DELETE %s, %s FROM %s INNER JOIN %s " +
       "ON %s.content_id = %s.id WHERE %s.id = %s"
  return fmt.Sprintf(q, rTable, cTable, rTable, cTable, rTable, cTable, rTable,
    p.ID)
}

func (p Paste) FromRowMeta(rows *sql.Row) error {
  err := rows.Scan(&p.ID, &p.Filename, &p.Filetype, &p.Created, &p.Updated)
  return err
}

func (p Paste) FromRowFull(rows *sql.Row) error {
  err := rows.Scan(&p.ID, &p.Filename, &p.Filetype, &p.Created, &p.Updated,
    &p.Body)
  return err
}

func (p Paste) FromNewRecord(t time.Time, rID int) Paste {
  p.Updated = t.Format("2006-01-02 15:04:05")
  p.ID = rID
  return p
}
