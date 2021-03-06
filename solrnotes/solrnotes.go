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

type SolrConnection interface {
	AddDoc(doc Document) error
	FindDocsNearby(
		recipient uuid.UUID,
		latitude float64, 
		longitude float64, 
		radiusKm float64,
		maxRows int) ([]*Document, error)
	GetDoc(id uuid.UUID) (*Document, error)
	PurgeDocs(ids []uuid.UUID) error
	MarkDocDeleted(id uuid.UUID) error
	MarkDocRead(id uuid.UUID) error
}

type SolrNoteConnection struct {
	conn *solr.Connection
}

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

func NewSolrNoteConnection() (*SolrNoteConnection, error) {
	conn, err := solr.Init("localhost", 8983, "geonotes")
	if err != nil {
		return nil, err
	}
	return &SolrNoteConnection{conn}, nil
}

func (sc SolrNoteConnection) AddDoc(doc Document) error {
	update := getUpdateJson(&doc)

	commit := true
	_, err := sc.conn.Update(update, commit)

	if err != nil {
		log.Printf("Failed to add doc to solr. Id: %v, Error: %#v", 
			doc.id.String(), err)
		return err
	}

	return nil
}

func (sc SolrNoteConnection) FindDocsNearby(
	recipient uuid.UUID,
	latitude float64, 
	longitude float64, 
	radiusKm float64,
	maxRows int) ([]*Document, error) {

	geofilter := formatGeofilter(latitude, longitude, radiusKm)
	
	q := solr.Query{
		Params: solr.URLParamMap{
			"q": []string{"*:*"},
			"fq": []string{
				RECIPIENT + ":" + recipient.String(),
				"!" + DELETED + ":" + "true",
				geofilter,
			},
		},
		Rows: maxRows,
	}

	response, err := sc.conn.Select(&q)
	if err != nil {
		log.Print(err)
		return nil, err
	}

	results := response.Results
	return docsFromResults(results), nil
}

func (sc SolrNoteConnection) GetDoc(id uuid.UUID) (*Document, error) {
	q := solr.Query{
		Params: solr.URLParamMap{
			"q": []string{ID + ":" + id.String()},
			"fq": []string{
				ID + ":" + id.String(),
			},
		},
		Rows: 1,
	}

	response, err := sc.conn.Select(&q)
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
		
	return docs[0], nil
}

func (sc SolrNoteConnection) PurgeDocs(ids []uuid.UUID) error {
	deleteIds := make([]string, len(ids))
	for i, id := range ids {
		deleteIds[i] = id.String()
	}
	
	update := map[string]interface{}{
		"delete" : deleteIds,
	}
	
	commit := true
	_, err := sc.conn.Update(update, commit)
	if err != nil {
		log.Print("Failed to delete docs.")
		return err
	}

	return nil
}

func (sc SolrNoteConnection) MarkDocDeleted(id uuid.UUID) error {
	doc, err := sc.GetDoc(id)
	if err != nil {
		log.Println("Failed to get doc to mark deleted with id: ", id, " Err: ", err)
	}

	doc.deleted = true
	update := getUpdateJson(doc)

	commit := true
	_, err = sc.conn.Update(update, commit)

	if err != nil {
		log.Printf("Failed to mark doc deleted in solr. Id: %v, Error: %#v", 
			id.String(), err)
		return err
	}

	return nil
}

func (sc SolrNoteConnection) MarkDocRead(id uuid.UUID) error {
	doc, err := sc.GetDoc(id)
	if err != nil {
		log.Println("Failed to get doc to mark deleted with id: ", id, " Err: ", err)
	}

	doc.read = true
	update := getUpdateJson(doc)

	commit := true
	_, err = sc.conn.Update(update, commit)

	if err != nil {
		log.Printf("Failed to mark doc deleted in solr. Id: %v, Error: %#v", 
			id.String(), err)
		return err
	}

	return nil
}

func docsFromResults(results *solr.DocumentCollection) []*Document {
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

	return docPointers(docs)
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

func formatGeofilter(lat float64, lon float64, radiusKm float64) string {
	latStr := formatCoordinateFloat(lat)
	lonStr := formatCoordinateFloat(lon)
	radiusStr := formatCoordinateFloat(radiusKm)
	geofilter := 
		"{!geofilt sfield=" + LOCATION + " pt=" + latStr + "," + lonStr + " d=" + radiusStr + "}"
	return geofilter
}

func docPointers(docs []Document) []*Document {
	dps := make([]*Document, len(docs))
	for i, _ := range docs {
		dps[i] = &docs[i]
	}
	return dps
}

func getUpdateJson(doc *Document) map[string]interface{} {
	return map[string]interface{}{
		"add": []interface{}{
			map[string]interface{}{
				ID: doc.id.String(),
				SENDER: doc.sender.String(),
				RECIPIENT: doc.recipient.String(),
				LOCATION: getCoordinateString(*doc),
				TIMESENT: doc.timeSent.Format(ISO8601_LAYOUT),
				READ: doc.read,
				DELETED: doc.deleted,
			},
		},
	}
}
