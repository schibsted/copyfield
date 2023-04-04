// Copyright 2023 Schibsted. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

// package main contains the processBatch function and the main function for this little utility
package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	_ "github.com/lib/pq"
)

// Processed ID numbers are recorded to this file
const fileName = "progress.txt"

// Signal channel for handling ctrl-c
var sigChan = make(chan os.Signal, 1)

// WriterFlusher is an interface for a type that can both write and flush (like a file writer)
type WriterFlusher interface {
	Write(p []byte) (n int, err error)
	Flush() error
}

// processBatch generates and executes a semicolon-separated string of SQL queries
func processBatch(db *sql.DB, queries strings.Builder, queryBatch map[int64]string, modifiedCounter *uint64, lenQueryBatch uint64, wf WriterFlusher) {
	// Commit the current batch, and save the processed IDs
	queries.Reset()
	for _, query := range queryBatch {
		queries.WriteString(query)
	}
	if lenQueryBatch == 1 {
		fmt.Print(queryBatch[0])
	} else {
		fmt.Printf("%s (and %d more)", queryBatch[0], lenQueryBatch-1)
	}
	if _, err := db.Exec(queries.String()); err != nil {
		log.Fatalf("Error updating the table: %v", err)
	}
	// Write the last processed IDs to file
	for rowID := range queryBatch {
		fmt.Fprintf(wf, "%d\n", rowID)
	}
	wf.Flush()
	// Count it as modified
	*modifiedCounter += lenQueryBatch
	// Clear the queryBatch map
	for rowID := range queryBatch {
		delete(queryBatch, rowID)
	}
}

func main() {

	var (
		host     = flag.String("host", "127.0.0.1", "postgres host")
		port     = flag.Int("port", 5432, "postgres port")
		user     = flag.String("user", "userGoesHere", "postgres user")
		password = flag.String("password", "passwordGoesHere", "postgres password")
		dbname   = flag.String("dbname", "dbNameGoesHere", "postgres db name")
		sslMode  = flag.Bool("sslmode", false, "sslmode: true means \"require\", false means \"disable\"")

		tableName = flag.String("table", "tableNameGoesHere", "db table name")
		colNameID = flag.String("id", "id", "ID column name")

		colName1 = flag.String("src", "sourceFieldGoesHere", "table column to get the value from")
		colName2 = flag.String("dst", "destinationFieldGoesHere", "table column to set (and overwrite) the value of")

		newType = flag.String("newcol", "", "type of the new column, like BOOLEAN DEFAULT FALSE NOT NULL")

		batchSize = flag.Int("batch", 1, "the number of SQL statements that should be batched into a transaction")
	)

	flag.Parse()

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("Exiting...")
		os.Exit(0)
	}()

	sslModeString := "disable"
	if *sslMode {
		sslModeString = "require"
	}

	// Connect to the database
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		*host, *port, *user, *password, *dbname, sslModeString)
	fmt.Println(psqlInfo)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer db.Close()

	// Check the connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}

	// Get the total number of rows in the table
	var total int64
	err = db.QueryRow("SELECT COUNT(" + *colName1 + ") FROM " + *tableName + " WHERE " + *colName1 + " <> " + *colName2).Scan(&total)
	if err != nil {
		log.Fatalf("Error getting the total number of rows: %v", err)
	}

	var file *os.File
	var writer *bufio.Writer
	processedMap := make(map[int64]bool, total)

	// Read all the processed indices from progress.txt, into processedMap
	data, err := os.ReadFile(fileName)
	if err != nil {
		file, err = os.Create(fileName)
		if err != nil {
			log.Fatalf("Error creating file: %v", err)
		}
		defer file.Close()
		writer = bufio.NewWriter(file)
	} else {
		file, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Error opening file for writing: %v", err)
		}
		defer file.Close()
		writer = bufio.NewWriter(file)
		byteLines := bytes.Split(data, []byte{'\n'})
		for _, byteLine := range byteLines {
			trimmedLine := strings.TrimSpace(string(byteLine))
			if trimmedLine == "" {
				continue
			}
			processedIndex, err := strconv.ParseInt(trimmedLine, 10, 64)
			if err != nil {
				log.Fatalf("This line in %s does not appear to be a number: %s", fileName, trimmedLine)
			}
			processedMap[processedIndex] = true
		}
	}

	// Execute query to get all trade_id values that does not have the correct value set
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s <> %s", *colNameID, *tableName, *colName1, *colName2)
	fmt.Println(query)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error fetching all ID numbers: %v", err)
	}
	defer rows.Close()

	// Scan results into int64 slice
	var tableIDs []int64
	for rows.Next() {
		var rowID int64
		if err := rows.Scan(&rowID); err != nil {
			log.Fatalf("Error reading row ID: %v", err)
		}
		tableIDs = append(tableIDs, rowID)
	}
	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		log.Fatalf("Error during iteration: %v", err)
	}

	// Output an informative message
	fmt.Printf("Copying data from %s to %s, for %d rows.\n", *colName1, *colName2, total)

	// Create the column that is copied to, if it's missing
	if newType != nil && *newType != "" {
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", *tableName, *colName2, *newType))
		if err != nil {
			log.Fatalf("Error creating column %s in table %s with type %s: %v", *tableName, *colName2, *newType, err)
		}
	}

	modifiedCounter := uint64(0)
	counter := uint64(0)
	queryBatch := make(map[int64]string, *batchSize)
	var queries strings.Builder

	for progressIndex, rowID := range tableIDs {
		percentage := (float64(progressIndex+1) / float64(total)) * 100.0
		fmt.Printf("[%6.1f%% (%d/%d)] ", percentage, progressIndex+1, total)
		if _, ok := processedMap[rowID]; ok {
			fmt.Printf("Already processed ID %d, skipping.\n", rowID)
			continue
		}

		// Queue up a query for this row
		queryBatch[rowID] = fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s = %d;", *tableName, *colName2, *colName1, *colNameID, rowID)
		fmt.Printf("Queued %d\n", rowID)
		// Total counter, used for batching
		counter++

		if lenQueryBatch := uint64(len(queryBatch)); counter%uint64(*batchSize) == 0 && lenQueryBatch > 0 {
			// Process this batch of queries
			processBatch(db, queries, queryBatch, &modifiedCounter, lenQueryBatch, writer)
			// Output an indication that these row IDs has been processed, together with a newline
			fmt.Println(" DONE")
		}

	}

	fileEntries := modifiedCounter + uint64(len(processedMap))
	fmt.Printf("\nData copy completed successfully. Updated %d rows. %s has %d entries.\n", modifiedCounter, fileName, fileEntries)
}
