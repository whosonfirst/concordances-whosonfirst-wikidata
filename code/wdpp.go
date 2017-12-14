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
	"regexp"
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

var wofwd wdType
var wofredirected wdType

var qre *regexp.Regexp

func init() {
	qre = regexp.MustCompile("^Q[0-9]+$")

	wofCsvDefinitions := map[string]string{
		"country":    "/wof/code/wikidata_country.csv",
		"county":     "/wof/code/wikidata_county.csv",
		"dependency": "/wof/code/wikidata_dependency.csv",
		"locality":   "/wof/code/wikidata_localities.csv",
		"region":     "/wof/code/wikidata_region.csv",

		"borough":       "/wof/code/wikidata_borough.csv",
		"campus":        "/wof/code/wikidata_campus.csv",
		"continent":     "/wof/code/wikidata_continent.csv",
		"localadmin":    "/wof/code/wikidata_localadmin.csv",
		"macrocounty":   "/wof/code/wikidata_macrocounty.csv",
		"marinearea":    "/wof/code/wikidata_marinearea.csv",
		"neighbourhood": "/wof/code/wikidata_neighbourhood.csv",
		"ocean":         "/wof/code/wikidata_ocean.csv",
		"planet":        "/wof/code/wikidata_planet.csv",
		"timezone":      "/wof/code/wikidata_timezone.csv",
	}

	wofdef = make(map[string]wdType, len(wofCsvDefinitions))

	for k, csvfile := range wofCsvDefinitions {
		fmt.Println(k, csvfile)
		wofdef[k] = readCsvFile(csvfile)
	}

	wofwd = readCsvFile("/wof/whosonfirst-data/wd_extended.csv")
	wofredirected = readCsvFile("/wof/whosonfirst-data/wd_redirects.csv")
}

func readCsvFile(csvcode string) wdType {
	content, err := ioutil.ReadFile(csvcode)
	checkErr(err)

	var qcode string
	mapWdType := make(wdType)
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)

		if len(line) >= 2 && line[0] == 'Q' {
			qcode = strings.Split(line, ",")[0]

			// must be valid Q[0-9]+  code
			if !qre.MatchString(qcode) {
				fmt.Println("Error in the csv: ", qcode, " not valid in: ", csvcode)
			}

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
	connStr := "sslmode=disable connect_timeout=10"

	//
	// PG setup  for wd
	//
	db_wd, err := sql.Open("postgres", connStr)
	checkErr(err)
	defer db_wd.Close()
	txn_wd, err := db_wd.Begin()
	checkErr(err)
	createTableStr_wd := []string{
		"CREATE SCHEMA IF NOT EXISTS wd;",
		"DROP TABLE IF EXISTS wd.wdx CASCADE;",
		"CREATE TABLE wd.wdx (wd_id TEXT, a_wof_type TEXT[], data JSONB );",
	}
	for _, str := range createTableStr_wd {
		fmt.Println("executing:", str)
		_, err := txn_wd.Exec(str)
		checkErr(err)
	}
	stmt_wd, err := txn_wd.Prepare(pq.CopyInSchema("wd", "wdx", "wd_id", "a_wof_type", "data"))
	checkErr(err)

	//
	// PG setup  for label
	//
	db_label, err := sql.Open("postgres", connStr)
	checkErr(err)
	defer db_label.Close()
	txn_label, err := db_label.Begin()
	checkErr(err)
	createTableStr_label := []string{
		"CREATE SCHEMA IF NOT EXISTS wdlabels;",
		"DROP TABLE IF EXISTS wdlabels.en CASCADE;",
		"CREATE TABLE wdlabels.en (wd_id TEXT, wd_label TEXT );",
	}
	for _, str := range createTableStr_label {
		fmt.Println("executing:", str)
		_, err := txn_label.Exec(str)
		checkErr(err)
	}
	stmt_label, err := txn_label.Prepare(pq.CopyInSchema("wdlabels", "en", "wd_id", "wd_label"))
	checkErr(err)
	//
	// gz input definition
	//
	filename := "/wof/wikidata_dump/latest-all.json.gz"
	fmt.Println("Start .. reading:", filename)
	file, err := os.Open(filename)
	checkErr(err)
	gz, err := gzip.NewReader(file)
	checkErr(err)
	defer file.Close()
	defer gz.Close()
	gzipreader := bufio.NewReader(gz)

	c := SafeCounter{v: 0}

	//
	// Setup input, output and business logic for parallel processing
	//
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

		_, err = stmt_label.Exec(wdid, wdlabel)
		checkErr(err)

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

		// check if already wof referenced
		if ok := wofwd[wdid]; ok {
			match = append(match, "wof")
		}
		// check if already redirected
		if ok := wofredirected[wdid]; ok {
			match = append(match, "redirected")
		}

		// check GeoNames ID ; P1566
		//if gjson.GetBytes(b, "claims.P1566.#[rank!=deprecated]").Exists() {
		//	match = append(match, "P1566")
		//}

		// check ISO 3166-2 code ;  P300
		if gjson.GetBytes(b, "claims.P300.#[rank!=deprecated]").Exists() {
			match = append(match, "P300")
		}
		// check FIPS 10-4 (countries and regions) :P901
		if gjson.GetBytes(b, "claims.P901.#[rank!=deprecated]").Exists() {
			match = append(match, "P901")
		}
		// check IATA airport code : P238
		if gjson.GetBytes(b, "claims.P238.#[rank!=deprecated]").Exists() {
			match = append(match, "P238")
		}
		// check ICAO airport code : P239
		if gjson.GetBytes(b, "claims.P239.#[rank!=deprecated]").Exists() {
			match = append(match, "P239")
		}
		// check territory claimed by ; P1336
		if gjson.GetBytes(b, "claims.P1336.#[rank!=deprecated]").Exists() {
			match = append(match, "P1336")
		}

		// check territory claimed by ; P1310
		if gjson.GetBytes(b, "claims.P17.#[rank!=deprecated].qualifiers.P1310").Exists() {
			match = append(match, "P1310")
		}

		// log
		if (c.v % 100000) == 0 {
			fmt.Println("..processing:", c.v, "   wdid:", wdid, wdlabel)
		}

		if len(match) > 0 {
			// replace last coma to space
			if b[len(b)-2] == 44 {
				b[len(b)-2] = 32
			}

			// write to postgres
			_, err = stmt_wd.Exec(wdid, match, string(b))
			checkErr(err)
		}
		return nil, nil
	})

	// Start processing with parallel workers.
	err = wpp.Run()
	checkErr(err)

	//
	// Close WD Copy
	//
	_, err = stmt_wd.Exec()
	checkErr(err)
	err = stmt_wd.Close()
	checkErr(err)
	postprocessingStr_wd := []string{
		"CREATE UNIQUE INDEX  ON  wd.wdx(wd_id);",
		"CREATE        INDEX  ON  wd.wdx USING GIN( a_wof_type );",
		"ANALYSE wd.wdx;",
	}
	for _, str := range postprocessingStr_wd {
		fmt.Println("executing:", str)
		_, err := txn_wd.Exec(str)
		checkErr(err)
	}
	err = txn_wd.Commit()
	checkErr(err)
	//fmt.Println("... wd.wdx  Loaded:", c.v)

	//
	// Close Label Copy
	//
	_, err = stmt_label.Exec()
	checkErr(err)
	err = stmt_label.Close()
	checkErr(err)
	postprocessingStr_label := []string{
		"CREATE UNIQUE INDEX wdlabels_en_id ON wdlabels.en(wd_id);",
		"ANALYSE wdlabels.en;",
	}
	for _, str := range postprocessingStr_label {
		fmt.Println("executing:", str)
		_, err := txn_label.Exec(str)
		checkErr(err)
	}
	err = txn_label.Commit()
	checkErr(err)
	fmt.Println("... wdlabels.en Loaded:", c.v)
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
