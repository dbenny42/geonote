package solrnotes

import (
	"testing"
	"time"
	"sort"

	"github.com/satori/go.uuid"
	"github.com/rtt/Go-Solr"
)

func TestAddDoc(t *testing.T) {
	conn, err := solr.Init("localhost", 8983, "geonotes")
	if err != nil {
		t.Fatal()
	}
	
	doc := getTestDoc(uuid.NewV4(), uuid.NewV4())
	err = AddDoc(conn, doc)
	if err != nil {
		t.Fatal("Add doc failed.")
	}
	defer DeleteDocs(conn, []uuid.UUID{doc.id})

	result, err := GetDoc(conn, doc.id)
	if err != nil {
		t.Fatal("Get doc failed.")
	}

	if !docsEqual(doc, *result) {
		t.Fatal("Result doc did not match original doc.")
	}
}

func TestFindDocsNearby(t *testing.T) {
	conn, err := solr.Init("localhost", 8983, "geonotes")
	if err != nil {
		t.Fatal("Failed to connect to solr. Err: %v", err)
	}
	
	sender := uuid.NewV4()
	recipient := uuid.NewV4()
	nearby1 := getTestDocAtLocation(sender, recipient, 40.810260, -73.94694)
	nearby2 := getTestDocAtLocation(sender, recipient, 40.808612, -73.944443)
	farAway1 := getTestDocAtLocation(sender, recipient, 40.758320, -73.988327)
	docs := []Document{nearby1, nearby2, farAway1}
	for _, doc := range docs {
		err := AddDoc(conn, doc)
		if err != nil {
			t.Fatal("Failed to add doc. Err: %v", err)
		}
	}

	defer DeleteDocs(conn, []uuid.UUID{nearby1.id, nearby2.id, farAway1.id})

	searchLat := 40.809322
	searchLon := -73.944587
	searchRadiusKm := .5
	maxRows := 10
	results, err := FindDocsNearby(conn, recipient, searchLat, searchLon, searchRadiusKm, maxRows)
	if err != nil {
		t.Fatal("Error from FindDocsNearby: ", err)
	}
	
	expectedResults := []*Document{&nearby1, &nearby2}
	if !allDocsEqual(expectedResults, results) {
		t.Fatal("Results are not what we expected.")
	}
}

func TestFindDocsIgnoresDeleted(t *testing.T) {
	conn, err := solr.Init("localhost", 8983, "geonotes")
	if err != nil {
		t.Fatal("Failed to connect to solr. Err: %v", err)
	}
	
	sender := uuid.NewV4()
	recipient := uuid.NewV4()
	nearby1 := getTestDocAtLocation(sender, recipient, 40.810260, -73.94694)
	nearby1.deleted = true
	nearby2 := getTestDocAtLocation(sender, recipient, 40.808612, -73.944443)
	farAway1 := getTestDocAtLocation(sender, recipient, 40.758320, -73.988327)
	docs := []Document{nearby1, nearby2, farAway1}
	for _, doc := range docs {
		err := AddDoc(conn, doc)
		if err != nil {
			t.Fatal("Failed to add doc. Err: %v", err)
		}
	}

	defer DeleteDocs(conn, []uuid.UUID{nearby1.id, nearby2.id, farAway1.id})

	searchLat := 40.809322
	searchLon := -73.944587
	searchRadiusKm := .5
	maxRows := 10
	results, err := FindDocsNearby(conn, recipient, searchLat, searchLon, searchRadiusKm, maxRows)
	if err != nil {
		t.Fatal("Error from FindDocsNearby: ", err)
	}
	
	expectedResults := []*Document{&nearby2}
	if !allDocsEqual(expectedResults, results) {
		t.Fatal("Results are not what we expected.")
	}
}

func getTestDoc(sender uuid.UUID, recipient uuid.UUID) Document {
	lat := 42.4
	lon := 69.9
	return getTestDocAtLocation(sender, recipient, lat, lon)
}

func getTestDocAtLocation(
	sender uuid.UUID, 
	recipient uuid.UUID,
  lat float64,
	lon float64) Document {

	return Document{
		id: uuid.NewV4(),
		sender: sender,
		recipient: recipient,
		latitude: lat,
		longitude: lon,
		timeSent: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		read: true,
		deleted: false,
	}
}

func allDocsEqual(lhs []*Document, rhs []*Document) bool {
	sort.Sort(ById(lhs))
	sort.Sort(ById(rhs))
	if len(lhs) != len(rhs) {
		return false
	}

	for i, _ := range lhs {
		if !docsEqual(*lhs[i], *rhs[i]) {
			return false
		}
	}
	return true
}

func docsEqual(lhs Document, rhs Document) bool {
	if lhs.id != rhs.id {
		return false
	}

	if lhs.sender != rhs.sender {
		return false 
	}

	if lhs.recipient != rhs.recipient {
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

type ById []*Document

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
