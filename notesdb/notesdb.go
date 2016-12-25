package notesdb

import (
	"time"
	"log"
	"database/sql"
	"errors"
	
	_ "github.com/go-sql-driver/mysql"
	"github.com/satori/go.uuid"
)

type DbCredentials struct {
	User string
	Password string
	Host string
	Port string
}

type Note struct {
	id uuid.UUID
	sender uuid.UUID
	recipient uuid.UUID
	note string
	latitude float64
	longitude float64
	timeSent time.Time
	read bool
	deleted bool
}

func OpenDb(credentials *DbCredentials) (*sql.DB, error) {
	// TODO: add parseTime=true
	dsn := credentials.User + ":" + credentials.Password + "@tcp(" + 
		credentials.Host + ":" + credentials.Port + ")/geonote?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Print("Failed to open db:", err)
		return nil, err
	}
	return db, nil
}

func InsertNote(db *sql.DB, note *Note) error {
	insertSql := "INSERT INTO notes " + 
		" (id, sender, recipient, note, latitude, longitude, timesent, isread, isdeleted) VALUES " +
		" (?, ?, ?, ?, ?, ?, ?, ?, ?)"

	statement, err := db.Prepare(insertSql)
	if err != nil {
		log.Printf("Failed to prepare statement %v. Err: %v", insertSql, err)
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(
		note.id.String(),
		note.sender.String(),
		note.recipient.String(),
		note.note,
		note.latitude,
		note.longitude,
		note.timeSent,
		note.read,
		note.deleted,
	)
	if err != nil {
		log.Printf("Failed to insert note. Err:", err)
		return err
	}

	return nil
}

func DeleteNote(db *sql.DB, id uuid.UUID) error {
	deleteSql := "DELETE FROM notes where id = ?"
	statement, err := db.Prepare(deleteSql)
	if err != nil {
		log.Printf("Failed to prepare statement %v. Err: %v", deleteSql, err)
		return err
	}
	defer statement.Close()

	result, err := statement.Exec(id.String())
	if err != nil {
		log.Printf("Delete statement failed with err %v", err)
		return err
	}

	if rowsAffected, err := result.RowsAffected(); err != nil || rowsAffected != 1 {
		if err != nil  {
			log.Printf("Error getting rows affected: %v", err)
			return err
		}
		if rowsAffected != 1 {
			message := "Note delete stmt did not delete one row. Actual: " + string(rowsAffected)
			log.Print(message)
			return errors.New(message)
		}
	}

	return nil
}

func MarkNoteRead(db *sql.DB, id uuid.UUID) error {
	updateSql := "UPDATE notes SET isread = 1 where id = ?"
	statement, err := db.Prepare(updateSql)
	if err != nil {
		log.Printf("Failed to prepare statement to mark note with id %v as read. Err: %v", id, err)
			return err
	}
	defer statement.Close()

	result, err := statement.Exec(id.String())
	if err != nil {
		log.Printf("Update statement for note id %v failed with err: %v", id, err)
		return err
	}

	if rowsAffected, err := result.RowsAffected(); err != nil || rowsAffected != 1 {
		if err != nil {
			log.Printf("Error getting rows affected: %v", err)
			return err
		}
		if rowsAffected != 1 {
			message := "Mark as read failed to update exactly one row. Actual: " + string(rowsAffected)
			log.Print(message)
			return errors.New(message)
		}
	}

	return nil
}

func GetNotesBySender(db *sql.DB, senderId uuid.UUID) ([]*Note, error) {
	selectSql := "SELECT " +
		"id, sender, recipient, note, latitude, longitude, " +
		"timesent, isread, isdeleted " +
		"FROM notes " +
		"WHERE sender = ?"
	statement, err := db.Prepare(selectSql)
	if err != nil {
		log.Printf("Failed to prepare statement to select notes from sender %v. Err: %v", 
			senderId, err)
		return nil, err
	}
	defer statement.Close()

	var notes []*Note
	rows, err := statement.Query(senderId.String())
	defer rows.Close()
	for rows.Next() {
		note, err := noteFromRow(rows)
		if err != nil {
			panic(err.Error())
		}
		notes = append(notes, note)
	}

	return notes, nil
}

func GetNotesByRecipient(db *sql.DB, recipientId uuid.UUID) ([]*Note, error) {
	selectSql := "SELECT " +
		"id, sender, recipient, note, latitude, longitude, " +
		"timesent, isread, isdeleted " +
		"FROM notes " +
		"WHERE recipient = ?"
	statement, err := db.Prepare(selectSql)
	if err != nil {
		log.Printf("Failed to prepare statement to select notes from recipient %v. Err: %v", 
			recipientId, err)
		return nil, err
	}
	defer statement.Close()

	var notes []*Note
	rows, err := statement.Query(recipientId.String())
	defer rows.Close()
	for rows.Next() {
		note, err := noteFromRow(rows)
		if err != nil {
			panic(err.Error())
		}
		notes = append(notes, note)
	}

	return notes, nil
}

func GetNotesByIds(db *sql.DB, ids []uuid.UUID) ([]*Note, error) {
	var notes []*Note
	for _, id := range ids {
		note, err := GetNoteById(db, id)
		if err != nil {
			log.Printf("Failed to get note for id %v. Err: %v", id, err.Error())
			return nil, err
		}
		notes = append(notes, note)
	}

	return notes, nil
}

func GetNoteById(db *sql.DB, id uuid.UUID) (*Note, error) {
	var note *Note

	selectSql := "SELECT " +
		"id, sender, recipient, note, latitude, longitude, " +
		"timesent, isread, isdeleted " +
		"FROM notes " +
		"WHERE id = ?"

	statement, err := db.Prepare(selectSql)
	if err != nil {
		log.Printf("Failed to prepare statement to select notes by id. Err: %v", err)
		return nil, err
	}
	defer statement.Close()

	rows, err := statement.Query(id)
	if err != nil {
		log.Printf("Failed to query by ids. Err: %v\n", err.Error())
		return note, err
	}

	defer rows.Close()

	if rows.Next() {
		note, err = noteFromRow(rows)
		if err != nil {
			panic(err.Error())
		}
	}

	return note, nil
}

func noteFromRow(rows *sql.Rows) (*Note, error) {
	var note Note
	
	err := rows.Scan(
		&note.id, 
		&note.sender,
		&note.recipient, 
		&note.note, 
		&note.latitude,
		&note.longitude,
		&note.timeSent,
		&note.read,
		&note.deleted,
	)

	if err != nil {
		log.Printf("Failed to scan row. err: %v", err)
		return nil, err
	}

	return &note, err
}

func idStrings(ids []uuid.UUID) []string {
	results := make([]string, len(ids))
	for i, id := range ids {
		results[i] = id.String()
	}
	return results
}
