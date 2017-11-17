// Filter Wikidata JSON dump  Order of lines is not preserved.
//
//   go run ./code/wdlabelparse.go

package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/lib/pq"
	"github.com/miku/parallel"
	"github.com/tidwall/gjson"

	//"compress/gzip"
	gzip "github.com/klauspost/pgzip"
)

type SafeCounter struct {
	v   uint64
	mux sync.Mutex
}

func (c *SafeCounter) Inc() {
	c.mux.Lock()
	c.v++
	c.mux.Unlock()
}

func main() {

	filename := "/wof/wikidata_dump/latest-all.json.gz"
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	gz, err := gzip.NewReader(file)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	defer gz.Close()
	gzipreader := bufio.NewReader(gz)

	// PG setup
	connStr := "sslmode=disable connect_timeout=10"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	createTableStr := []string{
		"CREATE SCHEMA IF NOT EXISTS wdlabels;",
		"DROP TABLE IF EXISTS wdlabels.en CASCADE;",
		"CREATE TABLE wdlabels.en (wd_id TEXT PRIMARY KEY , wd_label TEXT );",
	}

	for _, str := range createTableStr {
		fmt.Println("executing:", str)
		_, err := txn.Exec(str)
		if err != nil {
			log.Fatal(err)
		}
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("wdlabels", "en", "wd_id", "wd_label"))
	if err != nil {
		log.Fatal(err)
	}

	c := SafeCounter{v: 0}

	// Setup input, output and business logic.
	p := parallel.NewProcessor(gzipreader, os.Stdout, func(b []byte) ([]byte, error) {

		c.Inc()
		wdid := gjson.GetBytes(b, "id").String()
		wdlabel := gjson.GetBytes(b, "labels.en.value").String()

		if wdlabel == "" {
			wdlabel = wdid
		}
		_, err = stmt.Exec(wdid, wdlabel)
		if err != nil {
			log.Fatal(err)
		}

		if (c.v % 10000) == 0 {
			fmt.Println("..loading:", c.v, "   wdid:", wdid, wdlabel)
		}
		return nil, nil
	})

	// Start processing with parallel workers.
	if err := p.Run(); err != nil {
		log.Fatal(err)
	}

	// Close PG
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	postprocessingStr := []string{
		//"CREATE UNIQUE INDEX wdlabels_en_id ON wdlabels.en(wd_id);",
		"ANALYSE wdlabels.en;",
	}

	for _, str := range postprocessingStr {
		fmt.Println("executing:", str)
		_, err := txn.Exec(str)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("... wdlabels.en Loaded ...")
}
