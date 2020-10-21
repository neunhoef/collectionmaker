package database

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/arangodb/go-driver"
	err2 "github.com/pkg/errors"
	"io"
	"math/rand"
	"strconv"
)

const oneHundredMB = 100000000

var ErrCountZero = errors.New("expected count can not be 0")

// IsNameSystemReserved checks if name of arangod resource is forbidden.
func IsNameSystemReserved(name string) bool {
	if len(name) > 0 && name[0] == '_' {
		return true
	}

	return false
}

// CreateOrGetDatabase returns handle to a database. If database does not exist then it is created.
func CreateOrGetDatabase(ctx context.Context, client driver.Client, DBName string,
	options *driver.CreateDatabaseOptions) (driver.Database, error) {
	if IsNameSystemReserved(DBName) {
		return client.Database(ctx, DBName)
	}

	handle, err := client.CreateDatabase(ctx, DBName, options)
	if err != nil {
		if driver.IsConflict(err) {
			return client.Database(ctx, DBName)
		}
		return nil, err
	}

	return handle, nil
}

// CreateOrGetCollection returns handle to a collection. If collection does not exist then it is created.
func CreateOrGetCollection(ctx context.Context, DBHandle driver.Database, colName string,
	options *driver.CreateCollectionOptions) (driver.Collection, error) {

	colHandle, err := DBHandle.CreateCollection(ctx, colName, options)

	if err == nil {
		return colHandle, nil
	}

	if driver.IsConflict(err) {
		// collection already exists
		return DBHandle.Collection(ctx, colName)
	}

	return nil, err
}

// CreateOrGetDatabaseCollection returns handle to a collection. Creates database and collection if needed.
func CreateOrGetDatabaseCollection(ctx context.Context, client driver.Client, DBName, colName string,
	options *driver.CreateCollectionOptions) (driver.Collection, error) {

	DBHandle, err := CreateOrGetDatabase(context.Background(), client, DBName, nil)
	if err != nil {
		return nil, err2.Wrap(err, "can not create/get database")
	}

	colHandle, err := CreateOrGetCollection(context.Background(), DBHandle, colName, options)
	if err == nil {
		return colHandle, nil
	}

	if driver.IsConflict(err) {
		// collection already exists
		return DBHandle.Collection(ctx, colName)
	}

	return nil, err
}

// DocumentGenerator describes the behaviour how to add document.
type DocumentGenerator interface {
	Init(currentCount int64) (int64, error)
	Add(currentSize int64) (int64, []interface{}) // Add adds new documents.
}

type Collection struct {
	documentGenerator DocumentGenerator
	colHandle         driver.Collection
	ShowProgress      bool
	documents         []interface{}
}

// NewCollectionCreator creates new collection creator.
func NewCollectionCreator(documentGenerator DocumentGenerator, colHandle driver.Collection) Collection {

	return Collection{
		documentGenerator: documentGenerator,
		colHandle:         colHandle,
		ShowProgress:      true,
	}
}

func (c *Collection) Progress(currentCount, expectedCount int64) {
	if !c.ShowProgress {
		return
	}

	if expectedCount == 0 {
		fmt.Printf("\r%s.%s Count: %d", c.colHandle.Database().Name(), c.colHandle.Name(), currentCount)
		return
	}

	if currentCount == expectedCount {
		fmt.Printf("\r%s.%s Count: %d/%d\n", c.colHandle.Database().Name(), c.colHandle.Name(), currentCount, expectedCount)
		return
	}

	fmt.Printf("\r%s.%s Count: %d/%d", c.colHandle.Database().Name(), c.colHandle.Name(), currentCount, expectedCount)
	return
}

func (c *Collection) generateDocuments(currentCount int64) {
	var sentSize int64

	for {
		size, newDocuments := c.documentGenerator.Add(currentCount)
		c.documents = append(c.documents, newDocuments...)
		if newDocuments == nil || len(newDocuments) == 0 {
			break
		}
		currentCount += int64(len(newDocuments))
		sentSize += size
		if sentSize > oneHundredMB {
			break
		}
	}

	return
}

// CreateDocuments writes documents using specific document generator.
func (c *Collection) CreateDocuments(ctx context.Context) error {

	if c.colHandle == nil {
		return fmt.Errorf("collection handler can not be nil")
	}

	currentCount, err := c.colHandle.Count(ctx)
	if err != nil {
		return err2.Wrapf(err, "con not get count of documents for a collection %s", c.colHandle.Name())
	}

	// expectedCount can be 0 when the number of documents is not known at that stage.
	expectedCount, err := c.documentGenerator.Init(currentCount)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err2.Wrapf(err, "can not initialize documents to write")
	}

	for {
		c.documents = make([]interface{}, 0, 2000)
		c.generateDocuments(currentCount)

		currentCount += int64(len(c.documents))
		if len(c.documents) > 0 {
			if _, _, err = c.colHandle.CreateDocuments(context.Background(), c.documents); err != nil {
				return err2.Wrap(err, "can not write documents")
			}

			// clear only length, the memory will be reused.
			//c.documents = c.documents[:0]
		} else if expectedCount == 0 {
			// now it is known how many document it expected
			expectedCount = currentCount
		}

		c.Progress(currentCount, expectedCount)
		if len(c.documents) == 0 {
			break
		}
		c.documents = nil
		if currentCount == expectedCount {
			break
		}
	}

	return nil
}

// DataTest is the example data with one field to write as a one document.
type DataTest struct {
	FirstField string `json:"a,omitempty"`
}

// DocumentsWithEqualLength creates one document with one field with the same length.
type DocumentsWithEqualLength struct {
	sizeOfEachDocument int64
	ExpectedCount      int64
	ExpectedSize       int64
}

func (d *DocumentsWithEqualLength) Add(currentCount int64) (int64, []interface{}) {

	if d.ExpectedCount == 0 || currentCount >= d.ExpectedCount {
		return 0, nil
	}

	d1 := DataTest{
		FirstField: MakeRandomString(int(d.sizeOfEachDocument)),
	}

	docs := make([]interface{}, 0, 1)
	docs = append(docs, &d1)

	return d.sizeOfEachDocument, docs

}

func (d *DocumentsWithEqualLength) Init(_ int64) (int64, error) {
	if d.ExpectedCount <= 0 {
		return 0, ErrCountZero
	}

	d.sizeOfEachDocument = d.ExpectedSize / d.ExpectedCount
	return d.ExpectedCount, nil
}

// MakeRandomString creates slice of bytes for the provided length.
// Each byte is in range from 33 to 123.
func MakeRandomString(length int) string {
	b := make([]byte, length, length)

	for i := 0; i < length; i++ {
		s := rand.Int()%90 + 33
		b[i] = byte(s)
	}

	return string(b)
}

// DocumentsFromFile creates many documents from file.
type DocumentsFromFile struct {
	Scanner *bufio.Scanner
}

func (d DocumentsFromFile) Add(_ int64) (int64, []interface{}) {

	if !d.Scanner.Scan() {
		return 0, nil
	}
	count, _ := strconv.Atoi(d.Scanner.Text())
	if count <= 0 {
		return 0, nil
	}

	if !d.Scanner.Scan() {
		return 0, nil
	}
	size, _ := strconv.Atoi(d.Scanner.Text())
	if size <= 0 {
		return 0, nil
	}

	documents := make([]interface{}, 0, count)
	bytes := int64(size * count)
	for count > 0 {
		documents = append(documents, &DataTest{
			FirstField: MakeRandomString(size),
		})
		count--
	}

	return bytes, documents
}

func (d DocumentsFromFile) Init(currentCount int64) (int64, error) {

	// omit records which were written beforehand. It makes that idempotent.
	for currentCount > 0 {
		if !d.Scanner.Scan() {
			return 0, io.EOF
		}
		if !d.Scanner.Scan() {
			return 0, io.EOF
		}
		currentCount--
	}

	return 0, nil
}
