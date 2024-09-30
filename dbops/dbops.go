package dbops

import (
	"os"
	"fmt"
	"reflect"
	"path/filepath"
	// "database/sql"
	"strings"
	"log"
	_ "github.com/mattn/go-sqlite3"
	"github.com/jmoiron/sqlx"
)

var (
	dbFile string
	InitDBFile string
)

func init() {

	currentDir, err := os.Getwd()
	if err != nil {
			log.Fatal(err)
	}
	// currentDir = filepath.Dir(currentDir)
	dbFile = filepath.Join(currentDir, "DB", "TAXO2.db")
	InitDBFile = filepath.Join(currentDir, "DB", "init_db.sql")
}

type DB struct {
	DB *sqlx.DB
}

type DbInterface interface {
	Select(tableName string, where_clause string, out interface{}) error
	// Insert(tableName string, values ) error
	// Update(tableName string) error
	// Delete(tableName string) error
}



func GetConn() (db DB) {
	conn, err := sqlx.Open("sqlite3", dbFile)
	if err != nil {
			log.Fatal(err)
	}
	db = DB{conn}
	return
}

func InitDB() {
	log.Println("INITIALIZING DATABASE ...")
	db := GetConn()
	defer db.DB.Close()
	if c, ioErr := os.ReadFile(InitDBFile); ioErr!=nil {
		log.Fatal("WHILE OPENING INIT DB FILE ==> ", ioErr)
	} else {
		db.DB.MustExec(string(c))
	}
}

func GenerateInsertQuery(tableName string, s interface{}) string {
	v := reflect.ValueOf(s)
	t := reflect.TypeOf(s)

	if t.Kind() != reflect.Struct {
		return ""
	}

	var columns []string
	var values []string

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		columns = append(columns, field.Name)
		values = append(values, fmt.Sprintf("'%v'", value.Interface()))
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(values, ", "))

	return query
}
//
// DeleteTablesExcept deletes all tables except for the specified subset.
func DeleteTablesExcept(db DB, keepTables []string) error {
    // Convert the slice of tables to keep into a map for quick lookup
    keepMap := make(map[string]struct{})
    for _, table := range keepTables {
        keepMap[table] = struct{}{}
    }

    // Query to get all table names
    var tableNames []string
    err := db.DB.Select(&tableNames, "SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        return fmt.Errorf("failed to query table names: %v", err)
    }

    // Iterate over all tables and drop those not in the keep list
    for _, tableName := range tableNames {
        if _, keep := keepMap[tableName]; !keep {
            dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
            if _, err := db.DB.Exec(dropQuery); err != nil {
                return fmt.Errorf("failed to drop table %s: %v", tableName, err)
            }
            log.Printf("Dropped table: %s\n", tableName)
        }
    }

    return nil
}
