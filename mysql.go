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

type MySqlDB struct {
	db *sql.DB
}

// NewMySql is opening connection to MySQL db
func NewMySql(conString string) (*MySqlDB, error) {

	db, err := sql.Open("mysql", conString)
	if err != nil {
		return nil, err
	}

	return &MySqlDB{db: db}, nil
}

// Close is closing connection to a MySQL db
func (d MySqlDB) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

// GetAllTables us getting the list of all tables in the MySqlDB
func (d MySqlDB) GetAllTables() ([]string, error) {
	if d.db == nil {
		return nil, errors.New("MySqlDB is not initialized")
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
func (d MySqlDB) GetTableColumns(table string) ([]*DbColumn, error) {
	if d.db == nil {
		return nil, errors.New("MySqlDB is not initialized")
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
		var defaultValue *string

		if err := rows.Scan(&column, &dataType, &null, &pk, &defaultValue, &auto); err != nil {
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
func (d MySqlDB) CreateSqlScript(table string, columns []*DbColumn) []byte {

	buf := bytes.Buffer{}

	buf.WriteString(fmt.Sprintf("----- TABLE: %s\n", table))

	// creating sql scripts
	d.createSqlClause(table, columns, StoredProcedureTypeInsert, &buf)
	d.createSqlClause(table, columns, StoredProcedureTypeUpdate, &buf)
	d.createSqlClause(table, columns, StoredProcedureTypeDelete, &buf)

	return buf.Bytes()
}

func (d MySqlDB) createSqlClause(table string, columns []*DbColumn, spType StoredProcedureTypes, buf *bytes.Buffer) {

	buf.WriteString(fmt.Sprintf("----- %s\n", strings.ToUpper(string(spType))))

	var clause string
	switch spType {
	case StoredProcedureTypeInsert:
		// getting the list of columns
		cols := []string{}
		vals := []string{}

		for _, c := range columns {
			// inserting non-autoincrement columns only
			if !c.IsAutoincrement {
				cols = append(cols, c.Name)
				vals = append(vals, d.getColumnName(c.Name))
			}
		}

		clause = fmt.Sprintf(`
INSERT INTO %s (%s) 
VALUES(%s);`, table, strings.Join(cols, ", "), strings.Join(vals, ", "))

	case StoredProcedureTypeUpdate:

		// getting the list of columns
		cols := []string{}
		pk := []string{}

		for _, c := range columns {
			// updating non-autoincrement columns only
			if !c.IsAutoincrement {
				cols = append(cols, fmt.Sprintf("%s = %s", c.Name, d.getColumnName(c.Name)))
			}

			// getting primary keys
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
%s;`, table, strings.Join(cols, ",\n"), strings.Join(pk, "\nAND "))

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
%s;`, table, strings.Join(pk, "\nAND "))
	}

	// adding new line symbol
	clause += "\n"

	buf.WriteString(d.createStoredProcedure(spType, table, columns, clause))
}

// createStoredProcedure is creating a stored procedure body
func (db MySqlDB) createStoredProcedure(spType StoredProcedureTypes, table string, columns []*DbColumn, clause string) string {

	// prearing list of parameters
	params := make([]string, len(columns))
	for i, c := range columns {
		params[i] = fmt.Sprintf("IN %s %s", db.getColumnName(c.Name), c.Type)

		i++
	}

	return fmt.Sprintf(`
CREATE PROCEDURE usp_%s_%s (%s) 
BEGIN
%s
END;`+"\n", table, spType, strings.Join(params, ", "), clause)
}

func (db MySqlDB) getColumnName(column string) string {
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
