package notesdb

import (
	"testing"
	"log"
	"time"
	"sort"
	"io/ioutil"
	"database/sql"

	"github.com/go-yaml/yaml"
	"github.com/satori/go.uuid"
)

func TestInsertNote(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	sender := uuid.NewV4()
	recipient := uuid.NewV4()

	db, err := OpenDb(credentials)
	if err != nil {
		t.Fatal()
	}

	note := getTestNote(sender, recipient)
	if err = InsertNote(db, note); err != nil {
		t.Fatal()
	}
	defer DeleteNote(db, note.id)
}

func TestMarkNoteRead(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	sender := uuid.NewV4()
	recipient := uuid.NewV4()
	
	db, err := OpenDb(credentials)
	if err != nil {
		t.Fatal()
	}

	note := getTestNote(sender, recipient)
	if err = InsertNote(db, note); err != nil {
		t.Fatal()
	}
	defer DeleteNote(db, note.id)

	if err = MarkNoteRead(db, note.id); err != nil {
		t.Fatal()
	}
}

func TestGetNotesBySender(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	db, err := OpenDb(credentials)
	if err != nil {
		t.Fatal()
	}

	numNotes := 5
	sender := uuid.NewV4()
	sender2 := uuid.NewV4()
	notes := getTestNotes(numNotes, sender, uuid.NewV4())
	notes2 := getTestNotes(numNotes, sender2, uuid.NewV4())

	for _, note := range notes {
		if err = InsertNote(db, note); err != nil {
			t.Fatal()
		}
	}

	for _, note := range notes2 {
		if err = InsertNote(db, note); err != nil {
			t.Fatal()
		}
	}
	defer deleteNotes(db, append(notes, notes2...))

	resultNotes, err := GetNotesBySender(db, sender)
	if err != nil {
		t.Fatal()
	}

	if !allNotesAreEqual(notes, resultNotes) {
		t.Fatal()
	}
}

func TestGetNotesByRecipient(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	db, err := OpenDb(credentials)
	if err != nil {
		t.Fatal()
	}

	numNotes := 5
	recipient := uuid.NewV4()
	recipient2 := uuid.NewV4()
	notes := getTestNotes(numNotes, uuid.NewV4(), recipient)
	notes2 := getTestNotes(numNotes, uuid.NewV4(), recipient2)

	for _, note := range notes {
		if err = InsertNote(db, note); err != nil {
			t.Fatal()
		}
	}

	for _, note := range notes2 {
		if err = InsertNote(db, note); err != nil {
			t.Fatal()
		}
	}
	defer deleteNotes(db, append(notes, notes2...))

	resultNotes, err := GetNotesByRecipient(db, recipient)
	if err != nil {
		t.Fatal()
	}

	if !allNotesAreEqual(notes, resultNotes) {
		t.Fatal()
	}
}

func TestGetNotesById(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	db, err := OpenDb(credentials)
	if err != nil {
		t.Fatal()
	}

	numNotes := 10000
	notes := getTestNotes(numNotes, uuid.NewV4(), uuid.NewV4())
	for _, note := range notes {
		if err = InsertNote(db, note); err != nil {
			t.Fatal()
		}
	}
	defer deleteNotes(db, notes)

	toFetch := notes[:numNotes - 3]
	var idsToFetch []uuid.UUID
	for _, note := range toFetch {
		idsToFetch = append(idsToFetch, note.id)
	}

	resultNotes, err := GetNotesByIds(db, idsToFetch)
	if err != nil {
		t.Fatal()
	}

	if !allNotesAreEqual(resultNotes, toFetch) {
		t.Fatal()
	}
}

func deleteNotes(db *sql.DB, notes []*Note) error {
	for _, note := range notes {
		if err := DeleteNote(db, note.id); err != nil {
			return err
		}
	}

	return nil
}

func getTestNotes(numNotes int, sender uuid.UUID, recipient uuid.UUID) []*Note {
	var notes []*Note
	for i := 0; i < numNotes; i++ {
		notes = append(notes, getTestNote(sender, recipient))
	}
	return notes
}

func allNotesAreEqual(expected []*Note, actual []*Note) bool {
	sort.Sort(ById(expected))
	sort.Sort(ById(actual))
	if len(expected) != len(actual) {
		return false
	}

	for idx, _ := range expected {
		if !notesAreEqual(expected[idx], actual[idx]) {
			return false
		}
	}

	return true
}

func notesAreEqual(lhs *Note, rhs *Note) bool {
	if lhs.id != rhs.id {
		return false
	}

	if lhs.sender != rhs.sender {
		return false 
	}

	if lhs.recipient != rhs.recipient {
		return false 
	}

	if lhs.note != rhs.note {
		return false 
	}

	if lhs.latitude != rhs.latitude {
		return false 
	}

	if lhs.longitude != rhs.longitude {
		return false 
	}

	if lhs.timeSent != rhs.timeSent {
		return false 
	}

	if lhs.read != rhs.read {
		return false 
	}

	if lhs.deleted != rhs.deleted {
		return false 
	}

	return true
}

type ById []*Note

func (s ById) Len() int {
	return len(s)
}

func (s ById) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ById) Less (i int, j int) bool {
	firstId := s[i].id
	secondId := s[j].id

	for ii, _ := range firstId {
		if firstId[ii] < secondId[ii] {
			return true
		}

		if firstId[ii] > secondId[ii] {
			return false
		}
	}

	return false // they are the same
}

func getTestNote(sender uuid.UUID, recipient uuid.UUID) *Note {
	id := uuid.NewV4()

	return &Note{
		id: id,
		sender: sender,
		recipient: recipient,
		note: "This is a test note",
		latitude: 42.2,
		longitude: 24.4,
		timeSent: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		read: false,
		deleted: true,
	}
}

func parseDbCredentials(filename string) (*DbCredentials, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Print("Failed to open db credentials file. err:", err)
		return nil, err
	}

	var credentials DbCredentials
	if err = yaml.Unmarshal(data, &credentials); err != nil {
		log.Print("Failed to unmarshal db credentials. Err:", err)
		return nil, err
	}
	
	return &credentials, nil
}
