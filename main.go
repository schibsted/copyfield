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

const fileName = "index.txt"

var sigChan = make(chan os.Signal, 1)

func main() {

	var (
		host     = flag.String("host", "127.0.0.1", "postgres host")
		port     = flag.Int("port", 5432, "postgres port")
		user     = flag.String("user", "userGoesHere", "postgres user")
		password = flag.String("password", "passwordGoesHere", "postgres password")
		dbname   = flag.String("dbname", "dbNameGoesHere", "postgres db name")

		tableName = flag.String("table", "tableNameGoesHere", "db table name")
		colNameID = flag.String("id", "id", "ID column name")

		colName1 = flag.String("src", "sourceFieldGoesHere", "table column to get the value from")
		colName2 = flag.String("dst", "destinationFieldGoesHere", "table column to set (and overwrite) the value of")

		newType = flag.String("newcol", "", "type of the new column, like BOOLEAN DEFAULT FALSE NOT NULL")
	)

	flag.Parse()

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("Exiting...")
		os.Exit(0)
	}()

	// Connect to the database
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		*host, *port, *user, *password, *dbname)

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
	var total int
	err = db.QueryRow("SELECT COUNT(" + *colName1 + ") FROM " + *tableName).Scan(&total)
	if err != nil {
		log.Fatalf("Error getting the total number of rows: %v", err)
	}

	var file *os.File
	var writer *bufio.Writer
	processedMap := make(map[int]bool, total)

	// Read all the processed indices from index.txt, into processedMap
	data, err := os.ReadFile(fileName)
	if err != nil {
		file, err = os.Create(fileName)
		if err != nil {
			log.Fatalf("Error creating file: %v", err)
		}
		defer file.Close()
		writer = bufio.NewWriter(file)
	} else {
		file, err = os.Open(fileName)
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
			processedIndex, err := strconv.Atoi(trimmedLine)
			if err != nil {
				log.Fatalf("This line in %s does not appear to be a number: %s", fileName, trimmedLine)
			}
			processedMap[processedIndex] = true
		}
	}

	// Execute query to get all trade_id values
	query := fmt.Sprintf("SELECT %s FROM %s", *colNameID, *tableName)
	fmt.Println(query)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error fetching all ID numbers: %v", err)
	}
	defer rows.Close()

	// Scan results into int slice
	var tableIDs []int
	for rows.Next() {
		var rowID int
		if err := rows.Scan(&rowID); err != nil {
			log.Fatalf("Error reading row ID: %v", err)
		}
		fmt.Printf("Found ID %d\n", rowID)
		tableIDs = append(tableIDs, rowID)
	}
	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		log.Fatalf("Error during iteration: %v", err)
	}

	// Output an informative message
	//total = len(tableIDs)
	fmt.Printf("Copying data from %s to %s, for %d rows.\n", *colName1, *colName2, total)

	// Create the column that is copied to, if it's missing
	if newType != nil && *newType != "" {
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", *tableName, *colName2, *newType))
		if err != nil {
			log.Fatalf("Error creating column %s in table %s with type %s: %v", *tableName, *colName2, *newType, err)
		}
	}

	modifiedCounter := 0
	for progressIndex, rowID := range tableIDs {
		fmt.Printf("%d / %d: ", progressIndex, total)
		if _, ok := processedMap[rowID]; ok {
			fmt.Printf("Already processed ID %d, skipping.\n", rowID)
			continue
		}

		query := fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s = %d;", *tableName, *colName2, *colName1, *colNameID, rowID)
		fmt.Println(query)
		_, err = db.Exec(query)
		if err != nil {
			log.Fatalf("Error updating the table: %v", err)
		}
		// Write the last processed ID to file
		fmt.Fprintf(writer, "%d\n", rowID)
		writer.Flush()
		// Also store it in memory
		//processedMap[rowID] = true
		// And count it as modified
		modifiedCounter++
		// And announce it
		fmt.Printf("Done with ID %d\n", rowID)
	}

	fmt.Printf("\nData copy completed successfully. Updated %d rows.\n", modifiedCounter)
}
