package main

import (
	// "fmt"
	"os"
	"path/filepath"
	// "database/sql"
	"flag"
	"log"
	"taxo2/dbops"
	"taxo2/ih08_proc"
	"taxo2/schema"
	"taxo2/taxo"
	// _ "github.com/mattn/go-sqlite3"
	// "github.com/jmoiron/sqlx"
)

var (
	ih08_file   string
	ih08_trans  string
	taxo_report string
	db          dbops.DB
	parseIH08   bool
	loadTaxo    bool
	taxoReport  bool
	rebootTaxo  bool
	intoTaxo    bool
)

func init() {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	ih08_file = filepath.Join(currentDir, "datos", "ih08.csv")
	ih08_trans = filepath.Join(currentDir, "datos", "ih08.json")
	taxo_report = filepath.Join(currentDir, "report", "taxo_report.csv")
	db = dbops.GetConn()

	flag.BoolVar(&parseIH08, "pih08", false, "LOAD IH08")
	flag.BoolVar(&loadTaxo, "load-taxo", false,
		"Parsear la TAXO y calcular el lugar de los equipos")
	flag.BoolVar(&taxoReport, "taxo-report", false, "Entregar reporte para SAP")
	flag.BoolVar(&rebootTaxo, "reboot-taxo", false, "Borrar las tablas internas de la TAXO")
	flag.BoolVar(&intoTaxo, "into-taxo", false, "Calcular el lugar de los equipos en la TAXO")
}

func main() {

	flag.Parse()
	if rebootTaxo {
		dbops.DeleteTablesExcept(db, []string{"IH08", "IH08F", "Clases"})
	}

	dbops.InitDB()
	defer db.DB.Close()
	//
	//
	if parseIH08 {
		ih08, err := ih08_proc.Parse(ih08_file, ih08_trans)
		if err != nil {
			log.Fatal("ERROR WHILE PARSING IH08", err)
		}
		ih08_ad := ih08_proc.AdjustPlates(ih08)
		log.Println("TOTAL ENTRIES ==> ", len(ih08_ad))
		if err := schema.InsertEQS(ih08_ad, &db); err != nil {
			log.Fatal("WHILE INSERTING IH08", err)
		}

		// var ih08_ad []schema.IH08Post
		// db.DB.Select(&ih08_ad, "SELECT * FROM IH08")
		log.Println("INSERT IH08 DONE!")
		ih08f := ih08_proc.ToFixed(ih08_ad, db)
		log.Println("TO FIXED FINISHED TOTAL ENTRIES ==> ", len(ih08f))
		if err := schema.InsertEQS(ih08f, &db); err != nil {
			log.Fatal("WHILE INSERTING IH08F => ", err)
		}
	}

	if loadTaxo {
		var ih08f []schema.IH08FPost
		err := db.DB.Select(&ih08f, "SELECT * FROM IH08F WHERE equipo<2000000000")
		if err != nil {
			log.Fatal("WHILE LOADING => ", err)
		}
		log.Println("DATA LOADED => ", len(ih08f))
		taxo.LoadTaxo(ih08f, db)
		taxo.IntoTaxo(ih08f, db)
	}

	if intoTaxo {
		var ih08f []schema.IH08FPost
		err := db.DB.Select(&ih08f, "SELECT * FROM IH08F")
		if err != nil {
			log.Fatal("WHILE LOADING => ", err)
		}
		log.Println("DATA LOADED => ", len(ih08f))
		taxo.IntoTaxo(ih08f, db)
	}

	if taxoReport {
		taxo.IntoReport(taxo_report, db)
	}

}
