package notesdb

import (
	"testing"
	"log"
	"time"
	"sort"
	"io/ioutil"

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

	db, err := NewMysqlNotesdb(credentials)
	if err != nil {
		t.Fatal()
	}

	note := getTestNote(sender, recipient)
	if err = db.InsertNote(note); err != nil {
		t.Fatal()
	}
	defer db.PurgeNote(note.id)
}

func TestMarkNoteRead(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	sender := uuid.NewV4()
	recipient := uuid.NewV4()
	
	db, err := NewMysqlNotesdb(credentials)
	if err != nil {
		t.Fatal()
	}

	note := getTestNote(sender, recipient)
	if err = db.InsertNote(note); err != nil {
		t.Fatal()
	}
	defer db.PurgeNote(note.id)

	if err = db.MarkNoteRead(note.id); err != nil {
		t.Fatal()
	}

	resultNotes, err := db.GetNotesByIds([]uuid.UUID{note.id})
	if err != nil || len(resultNotes) != 1 {
		t.Fatal("Failed to fetch note with id:", note.id, ", err: ", err)
	}

	if !resultNotes[0].read {
		t.Fatal("Failed to actually mark note read.")
	}
}

func TestMarkNoteDeleted(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	sender := uuid.NewV4()
	recipient := uuid.NewV4()
	
	db, err := NewMysqlNotesdb(credentials)
	if err != nil {
		t.Fatal()
	}

	note := getTestNote(sender, recipient)
	if err = db.InsertNote(note); err != nil {
		t.Fatal()
	}
	defer db.PurgeNote(note.id)

	if err = db.MarkNoteDeleted(note.id); err != nil {
		t.Fatal()
	}

	resultNotes, err := db.GetNotesByIds([]uuid.UUID{note.id})
	if err != nil || len(resultNotes) != 1 {
		t.Fatal("Failed to fetch note with id: ", note.id)
	}

	if !resultNotes[0].deleted {
		t.Fatal("Failed to actually mark note deleted.")
	}
}

func TestGetNotesBySender(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	db, err := NewMysqlNotesdb(credentials)
	if err != nil {
		t.Fatal()
	}

	numNotes := 5
	sender := uuid.NewV4()
	sender2 := uuid.NewV4()
	notes := getTestNotes(numNotes, sender, uuid.NewV4())
	notes2 := getTestNotes(numNotes, sender2, uuid.NewV4())

	for _, note := range notes {
		if err = db.InsertNote(note); err != nil {
			t.Fatal()
		}
	}

	for _, note := range notes2 {
		if err = db.InsertNote(note); err != nil {
			t.Fatal()
		}
	}
	defer deleteNotes(db, append(notes, notes2...))

	maxCount := 10
	offset := 0
	resultNotes, err := db.GetNotesBySender(sender, maxCount, offset)
	if err != nil {
		t.Fatal()
	}

	if !allNotesAreEqual(notes, resultNotes) {
		t.Fatal()
	}
}

func TestPaginatedGetNotesBySender(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	db, err := NewMysqlNotesdb(credentials)
	if err != nil {
		t.Fatal()
	}

	numNotes := 5
	sender := uuid.NewV4()
	notes := getTestNotes(numNotes, sender, uuid.NewV4())

	for idx, _ := range notes {
		notes[idx].timeSent = notes[idx].timeSent.AddDate(-idx, 0, 0)
		
		if err = db.InsertNote(notes[idx]); err != nil {
			t.Fatal()
		}
	}

	defer deleteNotes(db, notes)

	maxCount := 2
	offset := 0

	for offset < numNotes {
		resultNotes, err := db.GetNotesBySender(sender, maxCount, offset)
		if err != nil {
			t.Fatal()
		}

		upperLimit := offset + maxCount
		if upperLimit > numNotes {
			upperLimit = numNotes
		}
		if !allNotesAreEqual(notes[offset:upperLimit], resultNotes) {
			t.Fatal()
		}

		offset += maxCount
	}
}

func TestPaginatedGetNotesByRecipient(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	db, err := NewMysqlNotesdb(credentials)
	if err != nil {
		t.Fatal()
	}

	numNotes := 5
	recipient := uuid.NewV4()
	notes := getTestNotes(numNotes, uuid.NewV4(), recipient)

	for idx, _ := range notes {
		notes[idx].timeSent = notes[idx].timeSent.AddDate(-idx, 0, 0)
		
		if err = db.InsertNote(notes[idx]); err != nil {
			t.Fatal()
		}
	}

	defer deleteNotes(db, notes)

	maxCount := 2
	offset := 0

	for offset < numNotes {
		resultNotes, err := db.GetNotesByRecipient(recipient, maxCount, offset)
		if err != nil {
			t.Fatal()
		}

		upperLimit := offset + maxCount
		if upperLimit > numNotes {
			upperLimit = numNotes
		}
		if !allNotesAreEqual(notes[offset:upperLimit], resultNotes) {
			t.Fatal()
		}

		offset += maxCount
	}
}

func TestGetNotesByRecipient(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		log.Print("Failed to parse db credentials. Err:", err)
		t.Fatal()
	}

	db, err := NewMysqlNotesdb(credentials)
	if err != nil {
		t.Fatal()
	}

	numNotes := 5
	recipient := uuid.NewV4()
	recipient2 := uuid.NewV4()
	notes := getTestNotes(numNotes, uuid.NewV4(), recipient)
	notes2 := getTestNotes(numNotes, uuid.NewV4(), recipient2)

	for _, note := range notes {
		if err = db.InsertNote(note); err != nil {
			t.Fatal()
		}
	}

	for _, note := range notes2 {
		if err = db.InsertNote(note); err != nil {
			t.Fatal()
		}
	}
	defer deleteNotes(db, append(notes, notes2...))

	maxCount := 10
	offset := 0
	resultNotes, err := db.GetNotesByRecipient(recipient, maxCount, offset)
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

	db, err := NewMysqlNotesdb(credentials)
	if err != nil {
		t.Fatal()
	}

	numNotes := 10
	notes := getTestNotes(numNotes, uuid.NewV4(), uuid.NewV4())
	for _, note := range notes {
		if err = db.InsertNote(note); err != nil {
			t.Fatal()
		}
	}
	defer deleteNotes(db, notes)

	toFetch := notes[:numNotes - 3]
	var idsToFetch []uuid.UUID
	for _, note := range toFetch {
		idsToFetch = append(idsToFetch, note.id)
	}

	resultNotes, err := db.GetNotesByIds(idsToFetch)
	if err != nil {
		t.Fatal()
	}

	if !allNotesAreEqual(resultNotes, toFetch) {
		t.Fatal()
	}
}

func deleteNotes(db NotesdbConnection, notes []*Note) error {
	for _, note := range notes {
		if err := db.PurgeNote(note.id); err != nil {
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
		deleted: false,
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
