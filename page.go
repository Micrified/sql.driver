package driver

// Defines: Page structure
type Page struct {
  ID          string `json:"id"`
  Title       string `json:"title"`
  Subtitle    string `json:"subtitle"`
  Tag         string `json:"tag"`
  Created     string `json:"created"` 
  Updated     string `json:"updated"` 
  Body        string `json:"body"`
}

func (p *Page) QueryGetRows(rTable, cTable string) string {
  q := "SELECT a.id, a.title, a.subtitle, a.tag, b.created, b.updated " + 
       "FROM %s AS a INNER JOIN %s AS b " +
       "ON a.content_id = b.id " + 
       "ORDER BY b.created"
  return fmt.Sprintf(q, rTable, cTable)
}
func (p *Page) QueryGetRow(rTable, cTable) string {
  q := "SELECT a.id, a.title, a.subtitle, a.tag, b.created, b.updated, b.body " +
       "FROM %s AS a INNER JOIN %s AS b ON a.content_id = b.id " +
       "WHERE a.id = %s"
  return fmt.Sprintf(q, rTable, cTable, p.ID)
}
func (p *Page) QueryInsertContentRow(z *Table, timeStamp time.Time) string {
  q := "INSERT INTO %s (created,updated,body) VALUES ('%s','%s','%s')"
  t, cTable := timeStamp.Format("2006-01-02 15:04:05"), z.ContentTable()
  return fmt.Sprintf(q, cTable, t, t, p.Body)
}
func (p *Page) QueryInsertRecordRow(rTable string, cID int) string {
  q := "INSERT INTO %s (title,subtitle,tag,content_id) " + 
       "VALUES ('%s','%s','%s',%d)"
  return fmt.Sprintf(q, rTable, p.Title, p.Subtitle, p.Tag, cID)
}
func (p *Page) QueryUpdateRow(z *Table, timeStamp time.Time) string {
  q := "UPDATE %s LEFT JOIN %s ON %s.content_id = %s.id " + 
       "SET %s.title = '%s', %s.subtitle = '%s', %s.updated = '%s', %s.body = '%s'" +
       "WHERE %s.id = %d"
  t := timeStamp.Format("2006-01-02 15:04:05")
  rTable, cTable := z.RecordTable(), z.ContentTable()
  return fmt.Sprintf(q, rTable, cTable, rTable, cTable, rTable, p.Title,
    rTable, p.Subtitle, cTable, t, cTable, p.Body, rTable, p.ID)
}
func (p *Page) QueryDeleteRow(z *Table) string {
  q := "DELETE %s, %s FROM %s INNER JOIN %s " +
       "ON %s.content_id = %s.id WHERE %s.id = %s"
  rTable, cTable := z.RecordTable(), z.ContentTable()
  return fmt.Sprintf(q, rTable, cTable, rTable, cTable, rTable, cTable, rTable,
    p.ID)
}

func (p Page) FromRowMeta(rows *sql.Row) (Page, error) {
  err := rows.Scan(&p.ID, &p.Title, &p.Subtitle, &p.Tag, &p.Created,
    &p.Updated)
  return p, err
}

func (p Page) FromRowFull(rows *sql.Row) error {
  err := rows.Scan(&p.ID, &p.Title, &p.Subtitle, &p.Tag, &p.Created,
    &p.Updated, &p.Body)
  return p, err
}

func (p Page) FromNewRecord(timeStamp time.Time, rID int) Page {
  p.Created = p.Updated = timeStamp.Format("2006-01-02 15:04:05")
  p.ID = rID
  return p
}

