package database

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	err2 "github.com/pkg/errors"
	"math/rand"
)

// IsNameSystemReserved checks if name of arangod resource is forbidden.
func IsNameSystemReserved(name string) bool {
	if len(name) > 0 && name[0] == '_' {
		return true
	}

	return false
}

// CreateOrGetDatabase returns handle to a database. If database does not exist then it is created.
func CreateOrGetDatabase(ctx context.Context, client driver.Client, DBName string) (driver.Database, error) {
	if IsNameSystemReserved(DBName) {
		return client.Database(ctx, DBName)
	}

	handle, err := client.CreateDatabase(ctx, DBName, nil)
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

// DocumentGenerator describes the behaviour how to add document.
type DocumentGenerator interface {
	Add(sizeOfDocument int64) interface{} // Add adds new document.
}

type Collection struct {
	expectedSize      int64 // number of bytes for the whole collection
	expectedCount     int64 // number of document to create.
	documentGenerator DocumentGenerator
	colHandle         driver.Collection
}

// NewCollectionCreator creates new collection creator.
func NewCollectionCreator(expectedSize, expectedCount int64, documentGenerator DocumentGenerator,
	colHandle driver.Collection) Collection {

	return Collection{
		expectedSize:      expectedSize,
		expectedCount:     expectedCount,
		documentGenerator: documentGenerator,
		colHandle:         colHandle,
	}
}

// CreateDocuments writes documents using specific document generator.
func (c Collection) CreateDocuments(ctx context.Context) error {

	if c.colHandle == nil {
		return fmt.Errorf("collection handler can not be nil")
	}

	currentCount, err := c.colHandle.Count(ctx)
	if err != nil {
		return err2.Wrapf(err, "con not get count of documents for a collection %s", c.colHandle.Name())
	}

	howManyDocuments := c.expectedCount - currentCount
	if howManyDocuments <= 0 {
		return nil
	}

	documents := make([]interface{}, 0, howManyDocuments)
	sizeOfEachDocument := c.expectedSize / c.expectedCount

	for i := 0; i < int(howManyDocuments); i++ {
		documents = append(documents, c.documentGenerator.Add(sizeOfEachDocument))
	}

	if _, _, err = c.colHandle.CreateDocuments(context.Background(), documents); err != nil {
		return err2.Wrap(err, "can not write documents")
	}

	return nil
}

// DocumentWithOneField creates one document with one field.
type DocumentWithOneField struct{}

// DataTest is the example data with one field to write as a one document.
type DataTest struct {
	FirstField string `json:"h,omitempty"`
}

func (d DocumentWithOneField) Add(sizeOfDocument int64) interface{} {
	return DataTest{
		FirstField: MakeRandomString(int(sizeOfDocument)),
	}
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
