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
var blacklist wdType

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
		"disputed":      "/wof/code/wikidata_disputed.csv",
		"macroregion":   "/wof/code/wikidata_macroregion.csv",
	}

	wofdef = make(map[string]wdType, len(wofCsvDefinitions))

	for k, csvfile := range wofCsvDefinitions {
		fmt.Println(k, csvfile)
		wofdef[k] = readCsvFile(csvfile)
	}

	wofwd = readCsvFile("/wof/whosonfirst-data/wd_extended.csv")
	wofredirected = readCsvFile("/wof/whosonfirst-data/wd_redirects.csv")
	blacklist = readCsvFile("/wof/code/wikidata_blacklist.csv")
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
		"CREATE UNLOGGED TABLE wd.wdx (wd_id TEXT, a_wof_type TEXT[], data JSONB );",
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
		"CREATE UNLOGGED TABLE wdlabels.en (wd_id TEXT, wd_label TEXT );",
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
		p31claims := gjson.GetBytes(b, "claims.P31.#[rank!=deprecated]#.mainsnak.datavalue.value.id")
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

			// check "blacklist"  ?
			for _, k := range p31claims.Array() {
				if ok := blacklist[k.String()]; ok {
					// Check has a qualifiers.P582(end time) ?
					// https://www.wikidata.org/wiki/Property:P582 "indicates the time an item ceases to exist or a statement stops being valid"
					valueP582 := gjson.GetBytes(b, `claims.P17.#[mainsnak.datavalue.value.id="`+k.String()+`"].qualifiers.P582`)
					// No P582(end time)  - so this is valid claims!
					if !valueP582.Exists() {
						match = append(match, "blacklist")
						break
					}

				}
			}

			// check territory claimed by ; P625
			if gjson.GetBytes(b, "claims.P625.#[rank!=deprecated]").Exists() {
				match = append(match, "hasP625")
				// write to postgres
				_, err = stmt_wd.Exec(wdid, match, string(b))
				checkErr(err)
			} else {
				if len(match) == 1 && (match[0] == "locality" || match[0] == "marinearea" || match[0] == "county") {
					// skip - lot of items without coordinate.
					// -------------------------------------------------
					// {locality} without coordinate 		=1049335
					// {marinearea} without coordinate 		= 282329
					// {county}	without coordinate 			=  12335
				} else {
					// write to postgres
					_, err = stmt_wd.Exec(wdid, match, string(b))
					checkErr(err)
				}
			}

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

	err = txn_wd.Commit()
	checkErr(err)

	//
	// Close Label Copy
	//
	_, err = stmt_label.Exec()
	checkErr(err)
	err = stmt_label.Close()
	checkErr(err)

	err = txn_label.Commit()
	checkErr(err)

	fmt.Println("...  wd.wdx  + wdlabels.en Loaded:", c.v)
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

/*
---  FREQ values :  Wikidata JSON dump  ; 2017nov22
	{locality}	1049335
	{locality,hasP625}	806388
	{marinearea,hasP625}	378402
	{marinearea}	282329
	{locality,wof,hasP625}	88793
	{locality,county,hasP625}	25725
	{county,locality,hasP625}	25584
	{county,locality,wof,hasP625}	14598
	{locality,county,wof,hasP625}	14464
	{county,hasP625}	14434
	{county}	12335
	{campus,hasP625}	12237
	{locality,neighbourhood,hasP625}	12211
	{neighbourhood,locality,hasP625}	12063
	{locality,neighbourhood}	8772
	{neighbourhood,locality}	8702
	{campus,P239,hasP625}	5633
	{campus,wof,P238,P239,hasP625}	4689
	{campus,P238,P239,hasP625}	3237
	{region}	3025
	{county,wof,hasP625}	2276
	{localadmin,locality,wof,hasP625}	1882
	{county,locality}	1415
	{locality,county}	1376
	{neighbourhood,locality,wof,hasP625}	1259
	{locality,neighbourhood,wof,hasP625}	1202
	{region,hasP625}	1149
	{borough,locality,wof,hasP625}	810
	{localadmin,locality,hasP625}	810
	{campus,wof,hasP625}	742
	{locality,localadmin,wof,hasP625}	665
	{marinearea,wof,hasP625}	590
	{region,P300,P901,hasP625}	540
	{P1336,hasP625}	486
	{campus,wof,P239,hasP625}	418
	{macrocounty,hasP625}	390
	{region,P300,hasP625}	381
	{campus}	363
	{campus,P238,hasP625}	343
	{P239,hasP625}	305
	{P300,P901,hasP625}	303
	{P300,hasP625}	303
	{locality,redirected,hasP625}	300
	{locality,borough,wof,hasP625}	286
	{locality,wof}	282
	{locality,localadmin,hasP625}	249
	{dependency}	224
	{campus,wof,P238,hasP625}	195
	{country,wof,P901,hasP625}	181
	{country}	171
	{county,P300,hasP625}	170
	{campus,redirected,P238,P239,hasP625}	165
	{P238,P239,hasP625}	157
	{county,region,hasP625}	129
	{locality,wof,P300,hasP625}	128
	{redirected}	122
	{country,hasP625}	119
	{locality,wof,P300,P901,hasP625}	118
	{macrocounty}	118
	{locality,wof,P901,hasP625}	111
	{dependency,hasP625}	88
	{locality,marinearea,hasP625}	82
	{region,locality,wof,P300,P901,hasP625}	82
	{county,region,wof,P300,P901,hasP625}	78
	{county,wof,P300,hasP625}	78
	{marinearea,locality,hasP625}	68
	{county,region,P300,P901,hasP625}	64
	{county,P300,P901,hasP625}	62
	{locality,neighbourhood,wof}	62
	{campus,P239}	59
	{neighbourhood,locality,wof}	59
	{county,wof}	56
	{county,P901,hasP625}	53
	{county,wof,P300,P901,hasP625}	48
	{borough,hasP625}	47
	{marinearea,wof}	46
	{region,county,hasP625}	46
	{P901,hasP625}	44
	{locality,P901,hasP625}	41
	{locality,P1336,hasP625}	40
	{county,region,P300,hasP625}	39
	{borough,locality,hasP625}	38
	{county,neighbourhood,locality,hasP625}	34
	{county,borough,region,P300,P901,hasP625}	33
	{continent}	29
	{region,county,P300,P901,hasP625}	29
	{locality,P300,P901,hasP625}	27
	{localadmin,locality}	26
	{locality,region,wof,P300,P901,hasP625}	26
	{region,locality,hasP625}	26
	{borough,wof,hasP625}	25
	{county,borough,hasP625}	24
	{county,region}	23
	{disputed}	23
	{locality,wof,P238,P239,hasP625}	23
	{region,county,wof,P300,P901,hasP625}	23
	{locality,county,neighbourhood,hasP625}	22
	{P300}	21
	{locality,P300,hasP625}	21
	{campus,P238}	20
	{county,neighbourhood,locality,wof,hasP625}	20
	{P238,hasP625}	19
	{region,county,P300,hasP625}	17
	{county,region,borough,hasP625}	16
	{P1336,P1310,hasP625}	15
	{campus,redirected,P239,hasP625}	14
	{county,borough,region,hasP625}	14
	{locality,county,neighbourhood,wof,hasP625}	14
	{dependency,wof,P901,hasP625}	13
	{locality,localadmin}	13
	{redirected,hasP625}	13
	{locality,borough,hasP625}	12
	{locality,wof,P1336,hasP625}	12
	{marinearea,locality,wof,hasP625}	12
	{borough,region,county,hasP625}	11
	{county,region,borough,P300,P901,hasP625}	11
	{locality,wof,redirected,hasP625}	11
	{neighbourhood,localadmin,locality,hasP625}	11
	{neighbourhood,localadmin,locality,wof,hasP625}	10
	{borough}	9
	{country,P1336,hasP625}	9
	{county,borough,region}	9
	{county,region,wof,hasP625}	9
	{localadmin,locality,neighbourhood,hasP625}	9
	{locality,P901}	9
	{locality,marinearea,wof,hasP625}	9
	{locality,region,hasP625}	9
	{P1310,hasP625}	8
	{P239}	8
	{campus,wof}	8
	{county,locality,redirected,hasP625}	8
	{dependency,P901,hasP625}	8
	{disputed,hasP625}	8
	{locality,campus,wof,P238,P239,hasP625}	8
	{locality,neighbourhood,county,hasP625}	8
	{locality,neighbourhood,county,wof,hasP625}	8
	{macrocounty,P300,hasP625}	8
	{region,P1336,hasP625}	8
	{region,wof}	8
	{campus,P238,P239}	7
	{country,wof,P901,P1336,hasP625}	7
	{county,locality,wof}	7
	{county,region,locality,wof,hasP625}	7
	{dependency,P300,P901,hasP625}	7
	{locality,county,redirected,hasP625}	7
	{locality,wof,P238,hasP625}	7
	{macrocounty,wof,P300,hasP625}	7
	{macroregion,hasP625}	7
	{region,locality}	7
	{region,locality,P300,hasP625}	7
	{P1336}	6
	{continent,wof,hasP625}	6
	{country,dependency,wof,P901,hasP625}	6
	{locality,county,P300,hasP625}	6
	{locality,neighbourhood,localadmin,wof,hasP625}	6
	{neighbourhood,county,locality,wof,hasP625}	6
	{region,locality,wof,P300,hasP625}	6
	{region,locality,wof,hasP625}	6
	{P238,P239}	5
	{borough,county,hasP625}	5
	{campus,wof,redirected,P238,P239,hasP625}	5
	{county,locality,P300,hasP625}	5
	{dependency,wof,P300,P901,hasP625}	5
	{localadmin,locality,wof}	5
	{locality,county,region,wof,hasP625}	5
	{locality,region,P300,hasP625}	5
	{ocean,wof,hasP625}	5
	{P300,P901}	4
	{borough,region,P300,P901,hasP625}	4
	{country,wof,P300,P901,hasP625}	4
	{country,wof,P300,hasP625}	4
	{county,P300}	4
	{county,borough,region,wof,P300,P901,hasP625}	4
	{county,wof,P901,hasP625}	4
	{dependency,region,wof,P300,P901,hasP625}	4
	{locality,P238,P239,hasP625}	4
	{locality,county,neighbourhood}	4
	{locality,county,wof}	4
	{locality,region,P300,P901,hasP625}	4
	{neighbourhood,county,locality,hasP625}	4
	{neighbourhood,region,locality,P300}	4
	{region,P300}	4
	{region,borough,county,P300,P901,hasP625}	4
	{region,county}	4
	{region,locality,redirected,P300,P901,hasP625}	4
	{P1310}	3
	{P901,P1310,hasP625}	3
	{borough,locality}	3
	{borough,region,county}	3
	{borough,region,county,wof,P300,P901,hasP625}	3
	{borough,region,wof,P300,P901,hasP625}	3
	{country,wof,hasP625}	3
	{county,region,borough}	3
	{county,region,locality,hasP625}	3
	{county,region,locality,wof,P300,P901,hasP625}	3
	{localadmin,locality,neighbourhood,wof,hasP625}	3
	{locality,P1310,hasP625}	3
	{locality,county,borough,wof,hasP625}	3
	{locality,county,wof,P300,P901,hasP625}	3
	{locality,dependency,hasP625}	3
	{locality,localadmin,wof}	3
	{locality,region,wof,P300,hasP625}	3
	{marinearea,P1336,hasP625}	3
	{neighbourhood,hasP625}	3
	{neighbourhood,region,locality,P300,hasP625}	3
	{neighbourhood,region,locality,hasP625}	3
	{planet,wof}	3
	{region,P300,P901,P1336,hasP625}	3
	{region,P901,hasP625}	3
	{region,locality,P300,P901,hasP625}	3
	{region,locality,P901,hasP625}	3
	{region,wof,P300}	3
	{P238}	2
	{P901,P1336,hasP625}	2
	{borough,region,county,P300,P901,hasP625}	2
	{borough,region,hasP625}	2
	{borough,region,wof,P300,hasP625}	2
	{campus,wof,P238}	2
	{campus,wof,P238,P239}	2
	{continent,hasP625}	2
	{country,locality,wof,P901,hasP625}	2
	{county,P300,P901}	2
	{county,region,borough,wof,P300,P901,hasP625}	2
	{county,region,wof,P300,hasP625}	2
	{dependency,P1336,hasP625}	2
	{disputed,P1336,hasP625}	2
	{disputed,region}	2
	{localadmin,region,locality,hasP625}	2
	{locality,P239,hasP625}	2
	{locality,P300}	2
	{locality,county,neighbourhood,P300,hasP625}	2
	{locality,county,region,wof,P300,P901,hasP625}	2
	{locality,county,wof,P300,hasP625}	2
	{locality,localadmin,wof,redirected,hasP625}	2
	{locality,marinearea}	2
	{locality,marinearea,neighbourhood,hasP625}	2
	{locality,neighbourhood,localadmin}	2
	{locality,neighbourhood,localadmin,hasP625}	2
	{locality,neighbourhood,region,P300,hasP625}	2
	{locality,redirected}	2
	{locality,wof,P1310,hasP625}	2
	{locality,wof,P239,hasP625}	2
	{locality,wof,P901}	2
	{macroregion}	2
	{marinearea,locality}	2
	{marinearea,neighbourhood,locality,hasP625}	2
	{neighbourhood}	2
	{neighbourhood,borough,locality,hasP625}	2
	{ocean}	2
	{ocean,hasP625}	2
	{redirected,P238,P239,hasP625}	2
	{region,P1336,P1310,hasP625}	2
	{region,P300,P1336,hasP625}	2
	{region,P300,P901}	2
	{region,borough,county}	2
	{region,borough,county,hasP625}	2
	{region,borough,county,wof,P300,P901,hasP625}	2
	{region,county,wof,hasP625}	2
	{region,locality,county,hasP625}	2
	{region,locality,county,wof,P300,P901,hasP625}	2
	{region,locality,county,wof,hasP625}	2
	{region,locality,neighbourhood,P300}	2
	{region,wof,P300,P901,P1336,hasP625}	2
	{P300,P901,P1336,hasP625}	1
	{P901}	1
	{borough,locality,county,wof,hasP625}	1
	{campus,P239,P1336,hasP625}	1
	{campus,locality,wof,P238,P239,hasP625}	1
	{campus,neighbourhood,locality,P238,P239,hasP625}	1
	{campus,neighbourhood,locality,wof,P238,P239,hasP625}	1
	{campus,redirected,hasP625}	1
	{campus,wof,P239}	1
	{continent,wof,P901,hasP625}	1
	{country,P1310,hasP625}	1
	{country,P1336,P1310,hasP625}	1
	{country,P901,hasP625}	1
	{country,dependency}	1
	{country,dependency,hasP625}	1
	{country,disputed,P901,P1336,hasP625}	1
	{country,locality,hasP625}	1
	{country,wof}	1
	{country,wof,P1336,hasP625}	1
	{county,P1336}	1
	{county,P238,hasP625}	1
	{county,P239,hasP625}	1
	{county,P300,P1336,hasP625}	1
	{county,P901}	1
	{county,borough,locality,wof,hasP625}	1
	{county,borough,region,wof,hasP625}	1
	{county,locality,wof,P1310,hasP625}	1
	{county,locality,wof,P238,P239,hasP625}	1
	{county,locality,wof,P300,P901,hasP625}	1
	{county,locality,wof,P300,hasP625}	1
	{county,locality,wof,P901,hasP625}	1
	{county,marinearea}	1
	{county,marinearea,hasP625}	1
	{county,marinearea,wof,hasP625}	1
	{county,neighbourhood,locality}	1
	{county,redirected}	1
	{county,redirected,hasP625}	1
	{county,region,borough,P300,hasP625}	1
	{county,region,borough,locality,hasP625}	1
	{county,region,locality,P300,P901,hasP625}	1
	{county,region,redirected,P300,hasP625}	1
	{county,wof,P901}	1
	{dependency,P1336}	1
	{dependency,P300}	1
	{dependency,P300,hasP625}	1
	{dependency,country,wof,P901,hasP625}	1
	{dependency,county}	1
	{dependency,county,wof,hasP625}	1
	{dependency,locality}	1
	{dependency,region,P300}	1
	{dependency,region,P300,P901,hasP625}	1
	{dependency,region,P300,hasP625}	1
	{dependency,wof}	1
	{dependency,wof,P300,hasP625}	1
	{dependency,wof,P901,P1336,hasP625}	1
	{dependency,wof,hasP625}	1
	{disputed,P901,P1336,hasP625}	1
	{disputed,country,wof,P901,P1336,hasP625}	1
	{disputed,region,hasP625}	1
	{disputed,region,wof,P1336,hasP625}	1
	{disputed,wof,P1336,hasP625}	1
	{disputed,wof,P300,P901,P1336,P1310,hasP625}	1
	{disputed,wof,P300,P901,hasP625}	1
	{disputed,wof,P901,hasP625}	1
	{localadmin,locality,marinearea,wof,hasP625}	1
	{localadmin,locality,redirected,hasP625}	1
	{localadmin,region,locality,wof,hasP625}	1
	{locality,P1336,P1310,hasP625}	1
	{locality,P238,hasP625}	1
	{locality,P300,P901}	1
	{locality,borough,region,P300,P901,hasP625}	1
	{locality,campus,P238,P239,hasP625}	1
	{locality,campus,hasP625}	1
	{locality,campus,neighbourhood,wof,P238,P239,hasP625}	1
	{locality,campus,wof,hasP625}	1
	{locality,country,wof,P901,hasP625}	1
	{locality,county,P1336,hasP625}	1
	{locality,county,P238,hasP625}	1
	{locality,county,marinearea,hasP625}	1
	{locality,county,marinearea,wof,hasP625}	1
	{locality,county,neighbourhood,borough,region,P300,P901,hasP625}	1
	{locality,county,wof,P1310,hasP625}	1
	{locality,dependency,wof,P300,P901,hasP625}	1
	{locality,dependency,wof,P901,P1336,hasP625}	1
	{locality,dependency,wof,hasP625}	1
	{locality,disputed,hasP625}	1
	{locality,neighbourhood,P901,hasP625}	1
	{locality,neighbourhood,county}	1
	{locality,neighbourhood,localadmin,redirected,hasP625}	1
	{locality,neighbourhood,marinearea,hasP625}	1
	{locality,neighbourhood,region,hasP625}	1
	{locality,neighbourhood,region,wof,P300,hasP625}	1
	{locality,redirected,P901,hasP625}	1
	{locality,region,P901,hasP625}	1
	{locality,region,localadmin,hasP625}	1
	{locality,region,wof,hasP625}	1
	{locality,region,wof,redirected,P300,P901,hasP625}	1
	{macrocounty,wof}	1
	{macrocounty,wof,P300}	1
	{macrocounty,wof,hasP625}	1
	{marinearea,P1310}	1
	{marinearea,county,hasP625}	1
	{marinearea,disputed,hasP625}	1
	{marinearea,localadmin,locality,wof,hasP625}	1
	{marinearea,neighbourhood,locality,wof,hasP625}	1
	{marinearea,redirected}	1
	{marinearea,region,hasP625}	1
	{marinearea,wof,P1336,hasP625}	1
	{marinearea,wof,P300,P901,hasP625}	1
	{neighbourhood,borough,locality,wof,hasP625}	1
	{neighbourhood,county,locality}	1
	{neighbourhood,localadmin,locality}	1
	{neighbourhood,locality,campus,P239,hasP625}	1
	{neighbourhood,locality,wof,P238,P239,hasP625}	1
	{neighbourhood,region,locality,wof,P300,hasP625}	1
	{planet}	1
	{redirected,P300}	1
	{redirected,P300,hasP625}	1
	{region,P1336}	1
	{region,borough}	1
	{region,borough,P300,P901,hasP625}	1
	{region,borough,P300,hasP625}	1
	{region,borough,hasP625}	1
	{region,country,hasP625}	1
	{region,county,P300,P901}	1
	{region,dependency,P1336}	1
	{region,dependency,P1336,hasP625}	1
	{region,dependency,wof,P1336,hasP625}	1
	{region,dependency,wof,P300,P901,hasP625}	1
	{region,disputed,wof,P1336,hasP625}	1
	{region,localadmin,locality,hasP625}	1
	{region,locality,county}	1
	{region,locality,neighbourhood,hasP625}	1
	{region,locality,neighbourhood,wof,P300,hasP625}	1
	{region,locality,wof,P300,P238,hasP625}	1
	{region,locality,wof,redirected,P300,P901,hasP625}	1
	{region,marinearea,hasP625}	1
	{region,wof,P300,P901,hasP625}	1837
	{region,wof,P300,hasP625}	197
	{region,wof,P901,hasP625}	8
	{region,wof,hasP625}	57
	{timezone}	337
	{timezone,hasP625}	10
	{wof}	20704
	{wof,P1310,hasP625}	1
	{wof,P1336,hasP625}	7
	{wof,P238,P239,hasP625}	73
	{wof,P238,hasP625}	2
	{wof,P239,hasP625}	20
	{wof,P300}	1
	{wof,P300,P901,P1336,hasP625}	1
	{wof,P300,P901,hasP625}	296
	{wof,P300,hasP625}	85
	{wof,P901,P1336,hasP625}	1
	{wof,P901,hasP625}	4
	{wof,hasP625}	6983
	{wof,redirected}	1
*/
