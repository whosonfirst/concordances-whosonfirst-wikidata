//
// Filter and Load Wikidata JSON dump  to PostgreSQL
//

package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"

	gzip "github.com/klauspost/pgzip"
	"github.com/lib/pq"
	"github.com/miku/parallel"
	"github.com/tidwall/gjson"
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

type wdType map[string]bool

var wofdef map[string]wdType

func init() {

	wofCsvDefinitions := map[string]string{
		"country":    "/wof/code/wikidata_country.csv",
		"county":     "/wof/code/wikidata_county.csv",
		"region":     "/wof/code/wikidata_region.csv",
		"dependency": "/wof/code/wikidata_dependency.csv",
		"locality":   "/wof/code/wikidata_localities.csv",
	}

	wofdef = make(map[string]wdType)
	for k, csvfile := range wofCsvDefinitions {
		fmt.Println(k, csvfile)
		wofdef[k] = readCsvFile(csvfile)
	}

}

func readCsvFile(csvcode string) wdType {
	content, err := ioutil.ReadFile(csvcode)
	if err != nil {
		fmt.Print(err)
	}

	var qcode string
	mapWdType := make(wdType)
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)

		if len(line) >= 2 && line[0] == 'Q' {
			qcode = strings.Split(line, ",")[0]

			// remove spaces
			qcode = strings.Replace(qcode, " ", "", -1)

			// only add 'Q' codes,
			if qcode[0] == 'Q' {
				mapWdType[qcode] = true
			}
		} else {
			// not added , maybe a comment
		}

	}
	fmt.Println("csvread:", len(mapWdType))
	return mapWdType
}

func main() {

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
		"CREATE SCHEMA IF NOT EXISTS wdx;",
		"DROP TABLE IF EXISTS wdx.wd CASCADE;",
		"CREATE TABLE wdx.wd (wd_id TEXT, a_wof_type TEXT[], data JSONB );",
	}

	for _, str := range createTableStr {
		fmt.Println("executing:", str)
		_, err := txn.Exec(str)
		if err != nil {
			log.Fatal(err)
		}
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("wdx", "wd", "wd_id", "a_wof_type", "data"))
	if err != nil {
		log.Fatal(err)
	}

	//csvcode := os.Args[1]

	filename := "/wof/wikidata_dump/latest-all.json.gz"
	fmt.Println("Start .. reading:", filename)
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

	c := SafeCounter{v: 0}

	// Setup input, output and business logic.

	wpp := parallel.NewProcessor(gzipreader, os.Stdout, func(b []byte) ([]byte, error) {
		c.Inc()

		wdid := gjson.GetBytes(b, "id").String()
		if len(wdid) <= 1 {
			return nil, nil
		}
		if wdid[0] != 'Q' {
			return nil, nil
		}

		wdlabel := gjson.GetBytes(b, "labels.en.value").String()
		if wdlabel == "" {
			wdlabel = wdid
		}

		match := pq.StringArray{}

		// check P31claims ...
		p31claims := gjson.GetBytes(b, "claims.P31.#.mainsnak.datavalue.value.id")
		for wofk := range wofdef {
			for _, k := range p31claims.Array() {
				if ok := wofdef[wofk][k.String()]; ok {
					// Check has a qualifiers.P582(end time) ?
					// https://www.wikidata.org/wiki/Property:P582 "indicates the time an item ceases to exist or a statement stops being valid"
					valueP582 := gjson.GetBytes(b, `claims.P17.#[mainsnak.datavalue.value.id="`+k.String()+`"].qualifiers.P582`)
					// No P582(end time)  - so this is valid claims!
					if !valueP582.Exists() {
						match = append(match, wofk)
						break
					}

				}

			}
		}
		// check geonames ...

		// check

		if (c.v % 100000) == 0 {
			fmt.Println("..procesing:", c.v, "   wdid:", wdid, wdlabel)
		}

		if len(match) > 0 {
			// replace last coma to space
			if b[len(b)-2] == 44 {
				b[len(b)-2] = 32
			}

			// write to postgres
			_, err = stmt.Exec(wdid, match, string(b))
			if err != nil {
				log.Fatal(err)
			}
		}
		return nil, nil
	})

	// Start processing with parallel workers.
	if err := wpp.Run(); err != nil {
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
		"CREATE UNIQUE INDEX  ON  wdx.wd(wd_id);",
		"CREATE        INDEX  ON  wdx.wd USING GIN( a_wof_type );",
		"ANALYSE wdx.wd;",
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

	fmt.Println("... wdx.wd  Loaded:", c.v)
}
