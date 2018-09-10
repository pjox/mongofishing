package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/globalsign/mgo/bson"

	"github.com/globalsign/mgo"
)

// Binary represents a GridFS document in the anhalytics database
type Binary struct {
	ID           bson.ObjectId `bson:"_id"`
	AnhalyticsID string        `bson:"anhalyticsId"`
	Filename     string        `bson:"filename"`
	Aliases      interface{}   `bson:"aliases"`
	ChunkSize    int64         `bson:"chunkSize"`
	UploadDate   time.Time     `bson:"uploadDate"`
	Length       int64         `bson:"length"`
	contentType  interface{}   `bson:"contentType"`
	Md5          string        `bson:"md5"`
}

var (
	server   string
	queryLoc string
	datab    string
	mongo    string
	maxnbr   int
)

func init() {
	flag.StringVar(&server, "s", "http://traces1.inria.fr/nerd/service/disambiguate", "the server address")
	flag.StringVar(&queryLoc, "q", "query.json", "the name of the query file")
	flag.StringVar(&mongo, "mgo", "localhost:27017", "The server address")
	flag.StringVar(&datab, "db", "anhalytics_istex_sample", "the name of the database")
	flag.IntVar(&maxnbr, "maxnb", 12, "maximun number of concurrent requests")
}

func newFishingRequest(client *http.Client, url string, fileID bson.ObjectId, session *mgo.Session) (*http.Request, error) {
	conn := session.Copy()
	defer conn.Close()

	db := conn.DB(datab)
	file, err := db.GridFS("binaries").OpenId(fileID)
	defer file.Close()
	// TODO: change this for a constant,
	// unless people think they need a different query for each file.
	query, err := os.Open(queryLoc)
	if err != nil {
		return nil, err
	}

	// Prepare the reader instances to encode
	values := map[string]io.Reader{
		"file":  file,
		"query": query,
	}

	// Prepare a form to be submitted to the URL.
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, r := range values {
		var part io.Writer
		if x, ok := r.(io.Closer); ok {
			defer x.Close()
		}
		// Add a file
		if x, ok := r.(*os.File); ok {
			if part, err = writer.CreateFormFile(key, x.Name()); err != nil {
				return nil, err
			}
		} else {
			// Add other fields
			if part, err = writer.CreateFormField(key); err != nil {
				return nil, err
			}
		}
		if _, err := io.Copy(part, r); err != nil {
			return nil, err
		}

	}
	// Close the multipart writer.
	// so that the request won't be missing the terminating boundary.
	writer.Close()

	// Submit the form to the handler.
	request, err := http.NewRequest("POST", url, &body)
	if err != nil {
		return nil, err
	}
	// Set the content type, this will contain the boundary.
	request.Header.Set("Content-Type", writer.FormDataContentType())

	return request, err
}

func doFishingRequest(client *http.Client, fileID, halID bson.ObjectId, session *mgo.Session) {
	request, err := newFishingRequest(client, server, fileID, session)
	if err != nil {
		panic(err)
	}

	// Submit the request
	res, err := client.Do(request)
	if err != nil {
		log.Fatalln(err)
	}

	// Check the response
	if res.StatusCode != http.StatusOK {
		err = fmt.Errorf("bad status: %s", res.Status)
	}
	body := &bytes.Buffer{}
	_, err = body.ReadFrom(res.Body)
	if err != nil {
		log.Fatal(err)
	}
	res.Body.Close()

	var jsonFile []byte
	jsonFile = body.Bytes()

	var v map[string]interface{}

	if err := json.Unmarshal(jsonFile, &v); err != nil {
		log.Println(err)
		log.Println(halID)
		log.Println(string(jsonFile[:]))
		log.Println("BAD DOCUMENT")
	}

	conn := session.Copy()
	defer conn.Close()

	nerdcon := conn.DB(datab).C("pdf_nerd_annotations")
	meta := conn.DB(datab).C("biblio_objects")

	var bibObject map[string]interface{}

	err = meta.FindId(halID).One(&bibObject)
	if err != nil {
		log.Println(err)
		panic(err)
	}

	i := bson.NewObjectId()

	if err = nerdcon.Insert(bson.M{"_id": i, "repositoryDocId": bibObject["repositoryDocId"], "anhalyticsId": halID, "isIndexed": bibObject["isIndexed"], "annotation": v}); err != nil {
		log.Println(err)
		panic(err)
	}

	selector := bson.M{"_id": halID}
	updator := bson.M{"$set": bson.M{"PDFNERD": true}}
	if err = meta.Update(selector, updator); err != nil {
		log.Println(halID)
		log.Println(err)
		panic(err)
	}
}

func fish() time.Duration {
	client := &http.Client{}

	session, err := mgo.Dial(mongo)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	session.SetMode(mgo.Strong, true)

	conn := session.Copy()
	defer conn.Close()

	pdfs := conn.DB(datab).GridFS("binaries")
	iter := pdfs.Files.Find(nil).Iter()
	var result Binary

	var wg sync.WaitGroup
	maxGoroutines := maxnbr
	guard := make(chan struct{}, maxGoroutines)

	infochan := make(chan time.Duration)

	for iter.Next(&result) {
		var halID, fileID bson.ObjectId
		fileID = result.ID
		halID = bson.ObjectIdHex(result.AnhalyticsID)
		//fmt.Println(result)

		wg.Add(1)
		go func(fileID, halID bson.ObjectId) {
			guard <- struct{}{}
			start := time.Now()
			doFishingRequest(client, fileID, halID, session)
			stop := time.Since(start)

			if err != nil {
				log.Println(result)
				log.Println(err)
			}
			infochan <- stop
			<-guard
			wg.Done()
		}(fileID, halID)
	}

	go func() {
		wg.Wait()
		close(infochan)
		if iter.Timeout() {
			log.Println("Timeout MongoDB")
		}
		if err := iter.Close(); err != nil {
			return
		}
	}()

	var sysTime time.Duration

	for inform := range infochan {
		sysTime += inform
	}

	return sysTime
}

func main() {
	flag.Parse()
	start := time.Now()
	sysTime := fish()
	usrTime := time.Since(start)

	fmt.Printf("%v (User time)\n", usrTime)
	fmt.Printf("%v (System time)\n", sysTime)
}
