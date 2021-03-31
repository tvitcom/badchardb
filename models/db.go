package models

import (
    "os"
    "fmt"
    "time"
    "log"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)

var (
    dbt, conn *sql.DB
    PRODMODE bool
)

type (
    VarcharColumn struct {
        Table string
        Column string
    }
    Record struct {
        Id int64
        Val string
    }
)

// Common pool prepare db connection
func InitDB(driverName, dataSourceName string) (*sql.DB) {
    conn, err := sql.Open(driverName, dataSourceName)
    
    log.Println("open main db conn")
    
    if err != nil {
        log.Fatal("DB is not connected")
    }
    
    if err = conn.Ping(); err != nil {
        log.Fatal("DB is not responded")
    }
    
    return conn
}

func GetListAllDbTables(conn *sql.DB/*, filterText string*/) (tabList []string, err error) {
    rows, err := conn.Query("SHOW TABLES")
    if err != nil {
        log.Panic(err)
    }
    defer rows.Close()
    var table string
    // var tabList []string
    for rows.Next() {
        err := rows.Scan(&table)
        if err != nil {
            log.Panic(err)
        }
        tabList = append(tabList, table)
        if !PRODMODE {
            fmt.Println("GATHERED:", table)
        }
    }
    if err = rows.Err(); err != nil {
        return tabList, err
    }
    return tabList, nil
}

// Gather all appropriate columns with varchar type in target database
func GatherAllVarcharTablesColumns(db *sql.DB, dbName string) ([]VarcharColumn, error) {
    sql := "SELECT TABLE_NAME, COLUMN_NAME FROM columns WHERE TABLE_SCHEMA = '" + dbName + "' AND DATA_TYPE like '%char' ORDER BY 1;"
    rows, err := db.Query(sql)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var columns []VarcharColumn
    for rows.Next() {
        varCol := VarcharColumn{}
        err := rows.Scan(&varCol.Table, &varCol.Column)
        if err != nil {
            return nil, err
        }
        columns = append(columns, varCol)
    }
    if err = rows.Err(); err != nil {
        return nil, err
    }
    return columns, nil
}

// Get descript of the column on manner DESC the_some_table;
// with output by print version. From: 
// https://stackoverflow.com/questions/47662614/http-responsewriter-write-with-interface
func DescriptTable(db *sql.DB, tab string) error {
    rows, err := db.Query("DESC "+tab)
    if err != nil {
        return err
    }
    defer rows.Close()
    cols, err := rows.Columns()
    if err != nil {
        return err
    }
    if cols == nil {
        return nil
    }

    // Make header for description:
    vals := make([]interface{}, len(cols))
    for i := 0; i < len(cols); i++ {
        vals[i] = new(interface{})
        if i != 0 {
            fmt.Print("\t\t")
        }
        fmt.Print(cols[i])
    }
    fmt.Println()

    // Make description table:
    for rows.Next() {
        err = rows.Scan(vals...)
        if err != nil {
            fmt.Println(err)
            continue
        }
        for i := 0; i < len(vals); i++ {
            if i != 0 {
                fmt.Print("\t\t")
            }
            printRawValue(vals[i].(*interface{}))
        }
        fmt.Println()

    }
    if rows.Err() != nil {
        return rows.Err()
    }
    return nil
}

func printRawValue(pval *interface{}) {
    switch v := (*pval).(type) {
    case nil:
        fmt.Print("NULL")
    case bool:
        if v {
            fmt.Print("1")
        } else {
            fmt.Print("0")
        }
    case []byte:
        fmt.Print(string(v))
    case time.Time:
        fmt.Print(v.Format("2006-01-02 15:04:05.999"))
    default:
        fmt.Print(v)
    }
}

func GetColumnRecordsWithoutId(db *sql.DB, table, column string) []*Record {
    rows, err := db.Query("SELECT " + column + " FROM " + table)
    if err != nil {
        fmt.Fprintf(os.Stderr, "In "+table+":"+column+" %s\n", err)
        os.Exit(1)
    }
    defer rows.Close()

    result := make([]*Record, 0)
    var nulledValue sql.NullString
    for rows.Next() {
        rec := new(Record)
        err := rows.Scan(
            &nulledValue,
        )

       // Here we can check if the value is nil (NULL value)
        if nulledValue.Valid {
            rec.Val = string(nulledValue.String)
        } else {
            rec.Val = "NULL"
        }
        
        if err != nil {
            fmt.Fprintf(os.Stderr, table+":"+column+"%s\n", err)
            os.Exit(1)
        }
        rec.Id = 0
        result = append(result, rec)
    }
    if err = rows.Err(); err != nil {
        fmt.Fprintf(os.Stderr, "%s\n", err)
        os.Exit(1)
    }
    return result
}

func GetColumnRecordsWithId(db *sql.DB, table, column string) []*Record {
    rows, err := db.Query("SELECT id, " + column + " FROM " + table)
    if err != nil {
        fmt.Fprintf(os.Stderr, "In "+table+":"+column+" %s\n", err)
        os.Exit(1)
    }
    defer rows.Close()

    result := make([]*Record, 0)
    var nulledValue sql.NullString
    for rows.Next() {
        rec := new(Record)
        err := rows.Scan(
            &rec.Id,
            &nulledValue,
        )

       // Here we can check if the value is nil (NULL value)
        if nulledValue.Valid {
            rec.Val = string(nulledValue.String)
        } else {
            rec.Val = "NULL"
        }
        
        if err != nil {
            fmt.Fprintf(os.Stderr, table+":"+column+"%s\n", err)
            os.Exit(1)
        }

        result = append(result, rec)
    }
    if err = rows.Err(); err != nil {
        fmt.Fprintf(os.Stderr, "%s\n", err)
        os.Exit(1)
    }
    return result
}

func GetTableColumns(db *sql.DB, tab string) []string {
    rows, err := db.Query("SELECT * FROM "+tab+" LIMIT 1")
    if err != nil {
        fmt.Fprintf(os.Stderr, tab + " %s\n", err)
        os.Exit(1)
    }
    defer rows.Close()
    cols, err := rows.Columns()
    if err != nil {
        fmt.Fprintf(os.Stderr, tab + " %s\n", err)
        os.Exit(1)
    }
    return cols
}