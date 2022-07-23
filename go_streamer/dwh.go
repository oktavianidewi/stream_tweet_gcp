package go_streamer

import (
	"context"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/bigquery"
)

// printTableInfo demonstrates fetching metadata from a table and printing some basic information to an io.Writer.
func PrintTableInfo(w io.Writer, projectID, datasetID, tableID string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	fmt.Printf("Client %v \n", client)

	meta, err := client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return err
	}

	// Print basic information about the table.
	fmt.Printf("Schema has %d top-level fields\n", len(meta.Schema))
	fmt.Printf("Description: %s\n", meta.Description)
	fmt.Printf("Rows in managed storage: %d\n", meta.NumRows)

	return nil
}

// createTableExplicitSchema demonstrates creating a new BigQuery table and specifying a schema.
func CreateTableExplicitSchema(projectID, datasetID, tableID string) error {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	tweetSchema := bigquery.Schema{
		{Name: "created_at", Type: bigquery.StringFieldType},
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "possibly_sensitive", Type: bigquery.BooleanFieldType},
		{Name: "quote_count", Type: bigquery.IntegerFieldType},
		{Name: "reply_count", Type: bigquery.IntegerFieldType},
		{Name: "retweet_count", Type: bigquery.IntegerFieldType},
		{Name: "retweeted", Type: bigquery.BooleanFieldType},
		{Name: "source", Type: bigquery.StringFieldType},
		{Name: "text", Type: bigquery.StringFieldType},
		{Name: "lang", Type: bigquery.StringFieldType},
		{Name: "user", Type: bigquery.RecordFieldType, Repeated: false, Schema: bigquery.Schema{
			{Name: "created_at", Type: bigquery.StringFieldType},
			{Name: "followers_count", Type: bigquery.IntegerFieldType},
			{Name: "friends_count", Type: bigquery.IntegerFieldType},
			{Name: "id", Type: bigquery.IntegerFieldType},
			{Name: "location", Type: bigquery.StringFieldType},
			{Name: "name", Type: bigquery.StringFieldType},
			{Name: "protected", Type: bigquery.BooleanFieldType},
			{Name: "screen_name", Type: bigquery.StringFieldType},
		},
		},
	}

	metaData := &bigquery.TableMetadata{
		Schema:         tweetSchema,
		ExpirationTime: time.Now().AddDate(1, 0, 0), // Table will be automatically deleted in 1 year.
	}
	tableRef := client.Dataset(datasetID).Table(tableID)

	err = tableRef.Create(ctx, metaData)
	fmt.Printf("ERR %v \n", err)
	if err != nil {
		return err
	}

	defer client.Close()

	return nil
}
