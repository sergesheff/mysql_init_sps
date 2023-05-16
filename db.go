package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

const paramPrefix = "_"

type DB struct {
	db *sql.DB
}

// NewDB is opening connection to MySQL db
func NewDB(conString string) (*DB, error) {

	db, err := sql.Open("mysql", conString)
	if err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

// Close is closing connection to a MySQL db
func (d DB) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

// GetAllTables us getting the list of all tables in the DB
func (d DB) GetAllTables() ([]string, error) {
	if d.db == nil {
		return nil, errors.New("DB is not initialized")
	}

	// getting all tables
	rows, err := d.db.Query(`SHOW TABLES`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	result := []string{}
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}

		result = append(result, table)
	}

	return result, err
}

// GetTableColumns is getting the list of all columns with data types for the specific table
func (d DB) GetTableColumns(table string) ([]*DbColumn, error) {
	if d.db == nil {
		return nil, errors.New("DB is not initialized")
	}

	// getting the columns details from the table
	rows, err := d.db.Query(fmt.Sprintf(`SHOW columns FROM %s`, table))
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	result := []*DbColumn{}

	for rows.Next() {
		var column, dataType, null, pk, auto string

		if err := rows.Scan(&column, &null, &dataType, &pk, "", &auto); err != nil {
			return nil, err
		}

		// checking for NULL
		isNull := strings.EqualFold(null, "yes")

		// checking if column is a primary key
		isPk := strings.EqualFold(pk, "pri")

		// checking for autoincrement
		isAutoincrement := strings.EqualFold(auto, "auto_increment")

		// adding new column to the slice
		result = append(result, &DbColumn{
			Name:            column,
			Type:            dataType,
			IsNull:          isNull,
			IsPrimary:       isPk,
			IsAutoincrement: isAutoincrement,
		})
	}

	return result, nil
}

// CreateSqlScript is creating SQL script for Insert, Update and Delete
func (DB) CreateSqlScript(table string, columns []*DbColumn) ([]byte, error) {
	buf := bytes.Buffer{}

	buf.WriteString(fmt.Sprintf("##### TABLE: %s\n", table))

	// creating sql script for insert

}

func (d DB) createSqlClause(table string, columns []*DbColumn, spType StoredProcedureTypes, buf *bytes.Buffer) {

	buf.WriteString(fmt.Sprintf("##### %s\n", strings.ToUpper(string(spType))))

	var clause string
	switch spType {
	case StoredProcedureTypeInsert:
		// getting the list of columns
		cols := make([]string, len(columns))
		vals := make([]string, len(columns))

		for i, c := range columns {
			cols[i] = c.Name
			vals[i] = d.getColumnName(c.Name)
		}

		clause = fmt.Sprintf(`
INSERT INTO %s (%s) 
VALUES(%s)`, table, strings.Join(cols, ","), strings.Join(vals, ","))

	case StoredProcedureTypeUpdate:

		// getting the list of columns
		cols := make([]string, len(columns))
		pk := []string{}

		for i, c := range columns {
			cols[i] = fmt.Sprintf("%s = %s", c.Name, d.getColumnName(c.Name))
			if c.IsPrimary {
				pk = append(pk, fmt.Sprintf("%s = %s", c.Name, d.getColumnName(c.Name)))
			}
		}

		if len(pk) == 0 {
			buf.WriteString("table should have a public key\n")
			return
		}

		clause = fmt.Sprintf(`
UPDATE 
    %s 
SET 
    %s 
WHERE 
    %s`, table, strings.Join(cols, ","), strings.Join(cols, ","), strings.Join(pk, " AND "))

	case StoredProcedureTypeDelete:
		// getting the list of columns
		pk := []string{}

		for _, c := range columns {
			if c.IsPrimary {
				pk = append(pk, fmt.Sprintf("%s = %s", c.Name, d.getColumnName(c.Name)))
			}
		}

		if len(pk) == 0 {
			buf.WriteString("table should have a public key\n")
			return
		}

		clause = fmt.Sprintf(`
DELETE FROM 
    %s 
WHERE 
    %s`, table, strings.Join(pk, " AND "))

	}

	buf.WriteString(d.createStoredProcedure(StoredProcedureTypeInsert, table, columns, clause))
}

// createStoredProcedure is creating a stored procedure body
func (db DB) createStoredProcedure(spType StoredProcedureTypes, table string, columns []*DbColumn, clause string) string {

	// prearing list of parameters
	params := make([]string, len(columns))
	for i, c := range columns {
		params[i] = fmt.Sprintf("IN %s %s", c.Name, c.Type)

		i++
	}

	return fmt.Sprintf(`CREATE PROCEDURE usp_%s_%s (%s) \n BEGIN\n %s\n END`, table, spType, strings.Join(params, ","), clause)
}

func (db DB) getColumnName(column string) string {
	return fmt.Sprintf("%s%s", paramPrefix, column)
}

type StoredProcedureTypes string

const (
	StoredProcedureTypeInsert StoredProcedureTypes = "insert"
	StoredProcedureTypeUpdate StoredProcedureTypes = "update"
	StoredProcedureTypeDelete StoredProcedureTypes = "delete"
)

type DbColumn struct {
	Name            string
	Type            string
	IsNull          bool
	IsPrimary       bool
	IsAutoincrement bool
}
