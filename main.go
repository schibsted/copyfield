package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	_ "github.com/lib/pq"
)

var sigChan = make(chan os.Signal, 1)

func main() {

	var (
		host        = flag.String("host", "localhost", "postgres host")
		port        = flag.Int("port", 5432, "postgres port")
		user        = flag.String("user", "postgres", "postgres user")
		password    = flag.String("password", "postgres", "postgres password")
		dbname      = flag.String("dbname", "mydatabase", "postgres db name")
		tableName   = flag.String("table", "items", "db table name")
		fieldName1  = flag.String("fromfield", "transactional", "field to get the value from")
		fieldName2  = flag.String("tofield", "shipping", "field to set (and overwrite) the value of")
		batchSize   = flag.Int("batchsize", 100, "rows at a time, per betch")
		idFieldName = flag.String("id", "id", "id field name")
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

	var (
		lastProcessed int
		fileName      = "last_processed.txt"
	)

	// Read the last processed position from the file
	file, err := os.Open(fileName)
	if err == nil {
		defer file.Close()
		_, err = fmt.Fscanf(file, "%d", &lastProcessed)
		if err != nil {
			log.Fatalf("Error reading last processed position: %v", err)
		}
	} else if !os.IsNotExist(err) {
		log.Fatalf("Error opening file: %v", err)
	}

	// Get the total number of rows in the table
	var total int
	err = db.QueryRow("SELECT COUNT(*) FROM $1", *tableName).Scan(&total)
	if err != nil {
		log.Fatalf("Error getting the total number of rows: %v", err)
	}

	fmt.Printf("Copying data from %s to %s, starting from position %d\n", *fieldName1, *fieldName2, lastProcessed+1)

	// Copy the data from field1 to field2 in batches
	start := lastProcessed + 1
	for start <= total {
		end := start + *batchSize - 1
		if end > total {
			end = total
		}

		// Create a slice to store the IDs for the current batch
		ids := make([]int, end-start+1)

		// Fill the slice with the IDs
		for i := start; i <= end; i++ {
			ids[i-start] = i
		}

		// Copy the data in batches
		for _, id := range ids {
			err = db.QueryRow("UPDATE mytable SET $1 = $2 WHERE $3 = $4", *fieldName2, *fieldName1, *idFieldName, id).Scan()
			if err != nil {
				log.Fatalf("Error updating the table: %v", err)
			}

			// Write the last processed position to the file
			file, err := os.OpenFile(fileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				log.Fatalf("Error opening file: %v", err)
			}
			defer file.Close()

			_, err = file.WriteString(strconv.Itoa(id))
			if err != nil {
				log.Fatalf("Error writing to file: %v", err)
			}

			// Print a dot to indicate progress
			fmt.Printf(".")
		}

		start = end + 1
	}

	fmt.Println("\nData copy completed successfully.")
}
