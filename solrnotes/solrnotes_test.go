package solrnotes

import (
	"testing"
	"time"

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

func getTestDoc(sender uuid.UUID, recipient uuid.UUID) Document {
	return Document{
		id: uuid.NewV4(),
		sender: sender,
		recipient: recipient,
		latitude: 42.4,
		longitude: 69.9,
		timeSent: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		read: true,
		deleted: true,
	}
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
