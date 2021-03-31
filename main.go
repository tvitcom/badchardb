package main

import (
	"fmt"
	"log"
	"net/http"
	"database/sql"
	"unicode/utf8"
	_ "github.com/go-sql-driver/mysql"
	"my.localhost/funny/gotools/badcharsdb/models"
)

const (
	PRODMODE         = false
	DIRSEP           = "/"
	DSN              = "myhouse:pass_to_myhouse@/myhouse"
	DSN_INFOSCHEMA   = "myhouse:pass_to_myhouse@/information_schema"
	DRIVER           = "mysql"
	DBNAME           = "myhouse"
)

var (
	err   error
	db *sql.DB
	httpAddr string = "0.0.0.0:3000"
)

type (
	Env struct {
    	db, infoschema *sql.DB
	}
	RecordError struct {
		Tab string
		Col string
		Id int64
		Val string
	}
)

func init() {
	models.PRODMODE = PRODMODE
}

func main() {
	// Init the connections pool to database
	db := models.InitDB(DRIVER, DSN)
	dbinf := models.InitDB(DRIVER, DSN_INFOSCHEMA)

    env := &Env{
    	db: db,
    	infoschema: dbinf,
    }

	// Run web server for observ current progress results:
	fmt.Println("Web server start on " + httpAddr)
	http.HandleFunc("/", env.homeHandler)
	http.ListenAndServe(httpAddr, nil)
}

func (env *Env) homeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, http.StatusText(405), 405)
		return
	}
	
	//Gather all tables, columns with varchar type:
	allStringColumns, err := models.GatherAllVarcharTablesColumns(env.infoschema, DBNAME)
	if err != nil {
		log.Panic(err)
	}
	
	var badRecords []RecordError

	for _, value := range allStringColumns {
		badRecords = utfValidationTable(value.Table, value.Column, env.db) //badResults []*RecordError
		if len(badRecords)>0 {
			fmt.Println(value.Table, value.Column)
			fmt.Fprintln(w, badRecords)
		} else {
			fmt.Fprintln(w, value.Table + "all normal utf-8 strings are")
		}
	}
	return
}

func utfValidationTable(table, column string, db *sql.DB) []RecordError {
	var badResults []RecordError
	var bad RecordError
	var datas []*models.Record
	withId := tableHasFieldId(db, table)
	if withId == true {
		datas = models.GetColumnRecordsWithId(db, table, column) // []*Record
	} else {
		datas = models.GetColumnRecordsWithoutId(db, table, column) // []*Record
	}
	for _, v := range datas {
		for _, s := range v.Val {		
			if s == utf8.RuneError {
				bad = RecordError{
					Tab: table,
					Col: column,
					Id: v.Id,
					Val: v.Val,
				}
				badResults = append(badResults, bad)
			}
		}
	}
	return badResults
}

func tableHasFieldId(db *sql.DB, table string) bool {
	var result bool
	columns := models.GetTableColumns(db, table)
	for _, col := range columns {
		// fmt.Println("TAB:",table,"COL:",col)
		if "id" == col {
			result = true
		}
	}
	return result
}