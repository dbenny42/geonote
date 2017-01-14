package solrnotes

import (
	"time"
	"log"
	"errors"
	"strings"
	"strconv"
	
	"github.com/rtt/Go-Solr"
	"github.com/satori/go.uuid"
)

type Document struct {
	id uuid.UUID
	sender uuid.UUID
	recipient uuid.UUID
	latitude float64
	longitude float64
	timeSent time.Time
	read bool
	deleted bool
}

const  (
	ID = "id"
	SENDER = "sender_s"
	RECIPIENT = "recipient_s"
	LOCATION = "location_p"
	TIMESENT = "timeSent_dt"
	READ = "read_b"
	DELETED = "deleted_b"

	ISO8601_LAYOUT = time.RFC3339
)

func AddDoc(conn *solr.Connection, doc Document) error {
	update := map[string]interface{}{
		"add": []interface{}{
			map[string]interface{}{
				ID: doc.id.String(),
				SENDER: doc.sender.String(),
				RECIPIENT: doc.recipient.String(),
				LOCATION: getCoordinateString(doc),
				TIMESENT: doc.timeSent.Format(ISO8601_LAYOUT),
				READ: doc.read,
				DELETED: doc.deleted,
			},
		},
	}

	commit := true
	_, err := conn.Update(update, commit)

	if err != nil {
		log.Printf("Failed to add doc to solr. Id: %v, Error: %#v", 
			doc.id.String(), err)
		return err
	}

	return nil
}

// func FindDocsNearby(recipient uuid.UUID, latitude float64, longitude float64) (Document, error) {

// }

func GetDoc(conn *solr.Connection, id uuid.UUID) (*Document, error) {
	q := solr.Query{
		Params: solr.URLParamMap{
			"q":           []string{"id:" + id.String()},
		},
		Rows: 1,
	}

	response, err := conn.Select(&q)
	if err != nil {
		log.Print(err)
		return nil, err
	}

	results := response.Results

	docs := docsFromResults(results)
	if len(docs) > 1 {
		message := "Somehow found far too many documents while querying for id: " + id.String()
		log.Print(message)
		return nil, errors.New(message)
	}

	if len(docs) < 1 {
		message := "Could not find any document for id: " + id.String()
		log.Print(message)
		return nil, errors.New(message)
	}
		
	return &docs[0], nil
}

func DeleteDocs(conn *solr.Connection, ids []uuid.UUID) error {
	deleteIds := make([]string, len(ids))
	for i, id := range ids {
		deleteIds[i] = id.String()
	}
	
	update := map[string]interface{}{
		"delete" : deleteIds,
	}
	
	commit := true
	_, err := conn.Update(update, commit)
	if err != nil {
		log.Print("Failed to delete docs.")
		return err
	}

	return nil
}

func docsFromResults(results *solr.DocumentCollection) []Document {
	var err error
	docs := make([]Document, results.Len())
	for i := 0; i < results.Len(); i++ {
		currDoc := results.Get(i)

		docs[i].id, err = uuid.FromString(currDoc.Field(ID).(string))
		if err != nil {
			log.Print("Failed to parse document id. Src id:", currDoc.Field(ID))
			continue
		}

		docs[i].sender, err = uuid.FromString(currDoc.Field(SENDER).(string))
		if err != nil {
			log.Print("Failed to parse sender id. Src id:", currDoc.Field(SENDER))
			continue
		}		

		docs[i].recipient, err = uuid.FromString(currDoc.Field(RECIPIENT).(string))
		if err != nil {
			log.Print("Failed to parse recipient id. Src id:", currDoc.Field(RECIPIENT))
			continue
		}

		docs[i].latitude, docs[i].longitude, err = 
			coordinatesFromString(currDoc.Field(LOCATION).(string))
		if err != nil {
			log.Print(err)
			continue
		}
		
		docs[i].timeSent, err = time.Parse(ISO8601_LAYOUT, currDoc.Field(TIMESENT).(string))
		if err != nil {
			log.Print("Failed to parse time: ", err)
			continue
		}

		docs[i].read = currDoc.Field(READ).(bool)
		docs[i].deleted = currDoc.Field(DELETED).(bool)
	}

	return docs
}

func getCoordinateString(doc Document) string {
	return formatCoordinateFloat(doc.latitude) + "," + formatCoordinateFloat(doc.longitude)
}

func formatCoordinateFloat(c float64) string {
	return strconv.FormatFloat(c, 'f', -1, 64)
}

func coordinatesFromString(s string) (float64, float64, error) {
	tokens := strings.Split(s, ",")
	if len(tokens) != 2 {
		return -1.0, -1.0, errors.New("Failed to parse coordinates.")
	}

	lat, err := strconv.ParseFloat(tokens[0], 64)
	if err != nil {
		return -1.0, -1.0, err
	}
	
	lon, err := strconv.ParseFloat(tokens[1], 64)
	if err != nil {
		return -1.0, -1.0, err
	}
	
	return lat, lon, nil
}
