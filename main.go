package main

import (
	"fmt"
	"log"
	"os"
	"sync"
)

func main() {

	fileName := "result.sql"
	conString := "admin:IrVpkJdwvaMl@tcp(192.168.0.60:3306)/complex"

	logger := log.New(os.Stdout, "", 0)

	logger.Println("application has been started")
	defer logger.Println("application has been completed")

	// opening DB connection
	db, err := NewMySql(conString)
	if err != nil {
		logger.Fatal("can't connect to DB", err)
	}

	// closing the connection after the procedure will complete
	defer db.Close()

	// getting the list of all tables
	tables, err := db.GetAllTables()
	if err != nil {
		logger.Fatal("can't get the list of all tables", err)
	}

	// opening the file
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		logger.Fatal(fmt.Sprintf("can't open file: %s", fileName), err)
	}

	defer file.Close()

	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(tables))
	// getting the list of all columns for each table
	for _, t := range tables {

		go func(t string) {
			defer wg.Done()

			columns, err := db.GetTableColumns(t)
			if err != nil {
				logger.Fatal(fmt.Sprintf("can't get the list of all columns for %s table", t), err)
			}

			// creating SQL scripts for each table
			b := db.CreateSqlScript(t, columns)

			mutex.Lock()

			// saving sql script to the file
			if _, err := file.Write(b); err != nil {
				logger.Fatal("can't write sql to the file", err)
			}

			mutex.Unlock()
		}(t)
	}

	wg.Wait()
}
