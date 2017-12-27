//
// Filter and Load Wikidata JSON dump  to PostgreSQL
//

// TODO:  add https://github.com/mc2soft/pq-types  add PostGISPoint
package main

import (
	"bufio"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
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

const pgoutput = true

type GisPoint struct {
	Lng  float64
	Lat  float64
	Null bool
}

func (p *GisPoint) String() string {
	return fmt.Sprintf("SRID=4326;POINT(%v %v)", p.Lng, p.Lat)
}
func (p GisPoint) Value() (driver.Value, error) {
	if !p.Null {
		return p.String(), nil
	} else {
		return nil, nil
	}
}

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

		// for Natural Earth
		"mountain":  "/wof/code/wikidata_mountain.csv",
		"island":    "/wof/code/wikidata_island.csv",
		"desert":    "/wof/code/wikidata_desert.csv",
		"basin":     "/wof/code/wikidata_basin.csv",
		"cape":      "/wof/code/wikidata_cape.csv",
		"lake":      "/wof/code/wikidata_lake.csv",
		"river":     "/wof/code/wikidata_river.csv",
		"dam":       "/wof/code/wikidata_dam.csv",
		"waterfall": "/wof/code/wikidata_waterfall.csv",
		"pole":      "/wof/code/wikidata_pole.csv",
		"circle":    "/wof/code/wikidata_circle.csv",
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
		"CREATE UNLOGGED TABLE wd.wdx (wd_id TEXT , wd_label TEXT NOT NULL, a_wof_type TEXT[] NOT NULL, geom geometry(Point, 4326) NULL, data JSONB NOT NULL );",
	}
	for _, str := range createTableStr_wd {
		fmt.Println("executing:", str)
		_, err := txn_wd.Exec(str)
		checkErr(err)
	}
	stmt_wd, err := txn_wd.Prepare(pq.CopyInSchema("wd", "wdx", "wd_id", "wd_label", "a_wof_type", "geom", "data"))
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
		"CREATE UNLOGGED TABLE wdlabels.en (wd_id TEXT NOT NULL, wd_label TEXT );",
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
	filename := "/wof/wikidata_dump/latest-all.json.gz" // default filename
	if len(os.Args) > 1 {
		filename = os.Args[1]
	}

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

		if pgoutput {
			_, err = stmt_label.Exec(wdid, wdlabel)
			checkErr(err)
		}

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

		// check statement disputed by ; P1310
		if gjson.GetBytes(b, "claims.P17.#[rank!=deprecated].qualifiers.P1310").Exists() {
			match = append(match, "P1310")
		}

		if pgoutput {
			// log
			if (c.v % 100000) == 0 {
				fmt.Println("..processing:", c.v, "   wdid:", wdid, wdlabel)
			}
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

			// check GeoNames ID ; P1566
			if gjson.GetBytes(b, "claims.P1566.#[rank!=deprecated]").Exists() {
				match = append(match, "hasP1566")
			}

			// check coordinate location ; P625
			p625 := gjson.Get(string(b), "claims.P625.#[rank!=deprecated]#").
				Get("#[mainsnak.datavalue.type==globecoordinate]#").
				Get("#[mainsnak.datavalue.value.globe==http://www.wikidata.org/entity/Q2]#").
				Get("#[mainsnak.snaktype==value]#")

			//p := wkb.Point{geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{0.1275, 51.50722})}

			//2/26 00:48:47 sql: converting argument $3 type: unsupported type geom.Point, a struct
			//p := geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{-122.082506, 37.4249518})
			//loc := geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{0.1275, 51.50722})
			//fmt.Println("loc:::", loc)

			gp := new(GisPoint)
			gp.Null = true

			if p625.Exists() {
				match = append(match, "hasP625")
				//fmt.Println(wdid, `p625`, p625.String())

				p625latitude := p625.Get(`#[rank==preferred].mainsnak.datavalue.value.latitude`)

				if p625latitude.Exists() {
					p625longitude := p625.Get(`#[rank==preferred].mainsnak.datavalue.value.longitude`)

					gp = &GisPoint{Lng: p625longitude.Float(), Lat: p625latitude.Float(), Null: false}
					//fmt.Println(wdid, `rank=preferred`, p625latitude, p625longitude)

				} else {
					p625latitude := p625.Get(`#[rank==normal].mainsnak.datavalue.value.latitude`)

					if p625latitude.Exists() {
						p625longitude := p625.Get(`#[rank==normal].mainsnak.datavalue.value.longitude`)
						gp = &GisPoint{Lng: p625longitude.Float(), Lat: p625latitude.Float(), Null: false}
						//fmt.Println(wdid, `rank=normal`, p625latitude, p625longitude)
					}
				}

				if pgoutput {
					// write to postgres
					_, err = stmt_wd.Exec(wdid, wdlabel, match, gp, string(WikidataJsonClean(b)))
					checkErr(err)
				} else {
					return append(WikidataJsonClean(b), '\n'), nil
				}
			} else {
				if len(match) == 1 && (match[0] == "locality" || match[0] == "marinearea" || match[0] == "county") {
					// skip - lot of items without coordinate.
					// -------------------------------------------------
					// {locality} without coordinate 		=1049335
					// {marinearea} without coordinate 		= 282329
					// {county}	without coordinate 			=  12335
				} else {
					if pgoutput {
						// write to postgres
						_, err = stmt_wd.Exec(wdid, wdlabel, match, gp, string(WikidataJsonClean(b)))
						checkErr(err)
					} else {
						return append(WikidataJsonClean(b), '\n'), nil
					}
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

type LangValue struct {
	Language string `json:"language"`
	Value    string `json:"value"`
}

type AliasList []LangValue

type WikiItems struct {
	ID           string               `json:"id"`
	Claims       map[string]Claim     `json:"claims,omitempty"`
	Labels       map[string]LangValue `json:"labels,omitempty"`
	Descriptions map[string]LangValue `json:"descriptions,omitempty"`
	Aliases      map[string]AliasList `json:"aliases,omitempty"`
	Sitelinks    map[string]SiteList  `json:"sitelinks,omitempty"`
	//Modified     time.Time              `json:"modified"`
	//Type         string                 `json:"type"`
}

type SiteList struct {
	Site   string   `json:"site,omitempty"`
	Title  string   `json:"title,omitempty"`
	Badges []string `json:"badges,omitempty"`
	Url    string   `json:"url,omitempty"`
}

type DataValueObj struct {
	Value json.RawMessage `json:"value,omitempty"`
	Type  string          `json:"type,omitempty"`
}

type EntityID struct {
	EntityType string `json:"entity-type"`
	NumericID  int    `json:"numeric-id"`
	ID         string `json:"id"`
}

type Snak struct {
	Snaktype string `json:"snaktype,omitempty"`
	Property string `json:"property,omitempty"`
	//Hash      string       `json:"hash,omitempty"`
	DataValue DataValueObj `json:"datavalue,omitempty"`
	Datatype  string       `json:"datatype,omitempty"`
}

type Reference struct {
	Hash       string            `json:"hash,omitempty"`
	Snaks      map[string][]Snak `json:"snaks,omitempty"`
	SnaksOrder []string          `json:"snaks-order,omitempty"`
}

type Property struct {
	Mainsnak Snak   `json:"mainsnak,omitempty"`
	Type     string `json:"type,omitempty"`
	//	ID              string            `json:"id,omitempty"`
	Rank string `json:"rank,omitempty"`
	//	References      []Reference       `json:"references,omitempty"`
	Qualifiers map[string][]Snak `json:"qualifiers,omitempty"`
	//	QualifiersOrder []string          `json:"qualifiers-order,omitempty"`
}

type Claim []Property

// -------------------
// Cleaning WikidataJSON
// Removing :  References, QualifiersOrder, ID , Hash
// Removing :  rank=deprecated
// Removing :  Claims  with P582(End time)  -   except P1082:population info
// --------------------
func WikidataJsonClean(content []byte) []byte {

	m := WikiItems{}
	err := json.Unmarshal(content, &m)
	checkErr(err)

	newClaims := map[string]Claim{}

	for propertyName, aProperty := range m.Claims {
		newProperties := []Property{}
		for _, v := range aProperty {
			keep := true
			if v.Mainsnak.Snaktype == "novalue" {
				// some claims has "no value"  - so we don't import them.
				// example:  https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q36823    P36   snaktype": "novalue"  ( No de jure Capital)
				keep = false
			}

			for _, qv := range v.Qualifiers {
				for _, qvi := range qv {
					//  If has "P582 - end time" - we don't keep and don't load to Postgres
					if qvi.Property == "P582" {
						keep = false
						break
					}
				}
			}

			if (keep || propertyName == "P1082") && (v.Rank != "deprecated") {
				newProperties = append(newProperties, v)
				// Keep - Not has an "P582:End Time"   or  Population(P1082)
				// and not Deprecated
			}

		}

		if len(newProperties) > 0 {
			newClaims[propertyName] = newProperties
		}
	}

	newWikiitem := WikiItems{
		ID:           m.ID,
		Claims:       newClaims,
		Labels:       m.Labels,
		Descriptions: m.Descriptions,
		Aliases:      m.Aliases,
		Sitelinks:    m.Sitelinks,
	}

	mj, err := json.Marshal(newWikiitem)
	checkErr(err)

	return mj
}
