//
// Filter and Load Wikidata JSON dump  to PostgreSQL
//

// testdata:
//   const pgoutput = false
//   time go run /wof/code/wdpp.go /wof/wikidata_dump/full-latest-all.json.gz  | grep ^{ | gzip -c > /wof/wikidata_dump/latest-all.json.gz

// TODO:  add https://github.com/mc2soft/pq-types  add PostGISPoint

// Todo:  Demolished test:  https://www.wikidata.org/wiki/Q689409  Weißenstein
// test: https://www.wikidata.org/wiki/Q1088727
//       https://www.wikidata.org/wiki/Q42846054

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
	"sort"
	"strings"
	"sync"

	gzip "github.com/klauspost/pgzip"
	"github.com/lib/pq"
	"github.com/miku/parallel"
	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/gjson"
)

const pgoutput = true
const sample = false

type WikiData struct {
	ID string

	wdqlabel string // wikidata preferred label
	wdqlang  string // wikidata preferred label language
	wdelabel string // wikidata english label

	match pq.StringArray //array of categories

	WikiItems WikiItems
	WikiJson  []byte

	nClaims       int
	nLabels       int
	nDescriptions int
	nAliases      int
	nSitelinks    int

	nCebSitelinks int
	IsCebuano     bool

	// Geometries
	p625          GisPoint
	p625latitude  string
	p625longitude string

	gphash string
	gp     GisPoint
	p1332  GisPoint
	p1333  GisPoint
	p1334  GisPoint
	p1335  GisPoint
}

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

type wiki2Group map[string]pq.StringArray

var wofdef map[string]wdType
var wiki2grp wiki2Group

var wofwd wdType
var wofredirected wdType
var blacklist wdType
var businesslist wdType
var wikimedialist wdType
var duplicatedlist wdType
var disambiguationlist wdType

var adminCodelist wdType

var qre *regexp.Regexp
var reWikivoyage *regexp.Regexp
var err error

var stmt_label *sql.Stmt
var txn_label *sql.Tx
var wdtables []string

func init() {
	wdtables = []string{"wd_nogeom_grp1", "wd_nogeom_grp2", "wd_messy", "wd_ok", "wd_demolished"}
	qre = regexp.MustCompile("^[QP][0-9]+$")

	wofCsvDefinitions := map[string]string{

		// uncategorized - but important !
		"uncategorized": "/wof/code/wikidata_uncategorized.csv",

		// for wof -manual edit
		"locality": "/wof/code/wikidata_localities.csv",
		"region":   "/wof/code/wikidata_region.csv",

		// wof - automatic
		"borough":       "/wof/code/wikidata_borough.csv",
		"campus":        "/wof/code/wikidata_campus.csv",
		"continent":     "/wof/code/wikidata_continent.csv",
		"country":       "/wof/code/wikidata_country.csv",
		"county":        "/wof/code/wikidata_county.csv",
		"dependency":    "/wof/code/wikidata_dependency.csv",
		"disputed":      "/wof/code/wikidata_disputed.csv",
		"localadmin":    "/wof/code/wikidata_localadmin.csv",
		"macrocounty":   "/wof/code/wikidata_macrocounty.csv",
		"macroregion":   "/wof/code/wikidata_macroregion.csv",
		"marinearea":    "/wof/code/wikidata_marinearea.csv",
		"neighbourhood": "/wof/code/wikidata_neighbourhood.csv",
		"ocean":         "/wof/code/wikidata_ocean.csv",
		"planet":        "/wof/code/wikidata_planet.csv",
		"timezone":      "/wof/code/wikidata_timezone.csv",

		// for Natural Earth
		"archipelago":   "/wof/code/wikidata_archipelago.csv",
		"bay":           "/wof/code/wikidata_bay.csv",
		"basin":         "/wof/code/wikidata_basin.csv",
		"canyon":        "/wof/code/wikidata_canyon.csv",
		"cape":          "/wof/code/wikidata_cape.csv",
		"circle":        "/wof/code/wikidata_circle.csv",
		"coast":         "/wof/code/wikidata_coast.csv",
		"dam":           "/wof/code/wikidata_dam.csv",
		"dmz":           "/wof/code/wikidata_dmz.csv",
		"delta":         "/wof/code/wikidata_delta.csv",
		"depression":    "/wof/code/wikidata_depression.csv",
		"desert":        "/wof/code/wikidata_desert.csv",
		"fictional":     "/wof/code/wikidata_fictional.csv",
		"graben":        "/wof/code/wikidata_graben.csv",
		"island":        "/wof/code/wikidata_island.csv",
		"isthmus":       "/wof/code/wikidata_isthmus.csv",
		"lake":          "/wof/code/wikidata_lake.csv",
		"lakegrp":       "/wof/code/wikidata_lakegrp.csv",
		"landform":      "/wof/code/wikidata_landform.csv",
		"mountain":      "/wof/code/wikidata_mountain.csv",
		"pass":          "/wof/code/wikidata_pass.csv",
		"peninsula":     "/wof/code/wikidata_peninsula.csv",
		"plain":         "/wof/code/wikidata_plain.csv",
		"plateau":       "/wof/code/wikidata_plateau.csv",
		"playa":         "/wof/code/wikidata_playa.csv",
		"pole":          "/wof/code/wikidata_pole.csv",
		"port":          "/wof/code/wikidata_port.csv",
		"protectedarea": "/wof/code/wikidata_protectedarea.csv",
		"research":      "/wof/code/wikidata_research.csv",
		"river":         "/wof/code/wikidata_river.csv",
		"tundra":        "/wof/code/wikidata_tundra.csv",
		"valley":        "/wof/code/wikidata_valley.csv",
		"waterbody":     "/wof/code/wikidata_waterbody.csv",
		"waterfall":     "/wof/code/wikidata_waterfall.csv",
		"wetland":       "/wof/code/wikidata_wetland.csv",
	}

	wofdef = make(map[string]wdType, len(wofCsvDefinitions))
	wiki2grp = make(wiki2Group, 0)

	for k, csvfile := range wofCsvDefinitions {
		fmt.Println(k, csvfile)
		wofdef[k] = readCsvFile(csvfile, 'Q')
	}

	for k, qwdtype := range wofdef {
		for qkey, _ := range qwdtype {
			if _, qkeyExist := wiki2grp[qkey]; qkeyExist {
				wiki2grp[qkey] = append(wiki2grp[qkey], k)
			} else {
				wiki2grp[qkey] = pq.StringArray{k}
			}
		}
	}

	//fmt.Println(wiki2grp)
	//os.Exit(0)

	wofwd = readCsvFile("/wof/whosonfirst-data/wd_extended.csv", 'Q')
	wofredirected = readCsvFile("/wof/whosonfirst-data/wd_redirects.csv", 'Q')
	blacklist = readCsvFile("/wof/code/wikidata_blacklist.csv", 'Q')
	businesslist = readCsvFile("/wof/code/wikidata_business.csv", 'Q')
	wikimedialist = readCsvFile("/wof/code/wikidata_wikimedia.csv", 'Q')
	duplicatedlist = readCsvFile("/wof/code/wikidata_duplicated.csv", 'Q')
	disambiguationlist = readCsvFile("/wof/code/wikidata_disambiguation.csv", 'Q')

	adminCodelist = readCsvFile("/wof/code/wikidata_admincode.csv", 'P')

	for blacklistkey, _ := range blacklist {
		if _, qkeyExist := wiki2grp[blacklistkey]; qkeyExist {
			fmt.Println("WARN: blacklist dups in", blacklistkey, wiki2grp[blacklistkey])
		}
	}
}

func readCsvFile(csvcode string, wdFirstChar byte) wdType {
	content, err := ioutil.ReadFile(csvcode)
	checkErr(err)

	var qcode string
	mapWdType := make(wdType)
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)

		if len(line) >= 2 && line[0] == wdFirstChar /* 'Q'/'P' */ {
			qcode = strings.Split(line, ",")[0]

			// must be valid [QP][0-9]+  code
			if !qre.MatchString(qcode) {
				fmt.Println("Error in the csv: ", qcode, " not valid in: ", csvcode)
			}

			// only add 'Q'/'P' codes,
			if qcode[0] == wdFirstChar {
				mapWdType[qcode] = true
			}
		} else {
			// not added , maybe a comment
		}

	}
	fmt.Println("csvread:", csvcode, "     records:", len(mapWdType))
	if len(mapWdType) == 0 {
		panic("Empty csv ???")
	}
	return mapWdType
}

var areaBoxHu geohash.Box
var areaBoxSr geohash.Box
var areaBoxSv geohash.Box
var areaBoxMs geohash.Box
var areaBoxPl geohash.Box
var areaBoxCsSk geohash.Box
var areaBoxRo geohash.Box

var wdStmt map[string]*sql.Stmt
var wdDB map[string]*sql.DB
var wdTX map[string]*sql.Tx

func main() {
	rexpl := regexp.MustCompile(`[,()]`)
	reWikivoyage = regexp.MustCompile("wikivoyage")

	areaBoxHu = CreateAreaBox(13.232, 41.522, 30.041, 50.070)
	areaBoxSr = CreateAreaBox(15.961, 39.555, 23.651, 46.210)
	areaBoxSv = CreateAreaBox(10.37, 54.69, 25.07, 69.38)
	areaBoxMs = CreateAreaBox(93.04, -12.28, 155.60, 8.56)
	areaBoxPl = CreateAreaBox(13.896, 48.853, 24.583, 55.033)
	areaBoxCsSk = CreateAreaBox(11.971, 47.725, 23.005, 51.099)
	areaBoxRo = CreateAreaBox(20.083, 43.485, 30.331, 48.586)

	//  latin letters preferred
	preferredLangSlice := [...]string{
		"en", "es", "pt", "de", "fr", "it",
		"nl", "da", "cs", "sk", "pl",
		"lv", "lt", "et", "hr", "sl", "fi",
		"ca", "sv", "no", "nb", "hu", "ro", "is", "el", "tr", "az", "sq",
		"sr", "ru", "bg", "uk", "be",
		"hi", "ja", "zh", "ko", "vi", "th",
		"ar", "fy", "he",
		"ta"}

	if pgoutput {
		//
		// PG setup  for wd
		//
		connStr := "sslmode=disable connect_timeout=10"
		createTableStr := `
		CREATE SCHEMA IF NOT EXISTS wd;
		 DROP TABLE IF EXISTS wd.{{.tablename}} CASCADE;
		 CREATE UNLOGGED TABLE wd.{{.tablename}} (
			 wd_id         	TEXT NOT NULL
			,wd_label       TEXT NOT NULL
			,wd_lang        TEXT NOT NULL
			,a_wof_type    	TEXT[] NOT NULL
			,nclaims       	Smallint
			,nlabels       	Smallint
			,ndescriptions 	Smallint
			,naliases      	Smallint
			,nsitelinks   	Smallint
			,ncebsitelinks  Smallint
			,iscebuano  	Bool
			,geomhash       TEXT NOT NULL
			,geom           Geometry(Point, 4326) NULL
			,data           JSONB NOT NULL );
			`

		wdStmt = make(map[string]*sql.Stmt)
		wdDB = make(map[string]*sql.DB)
		wdTX = make(map[string]*sql.Tx)
		for _, table := range wdtables {
			wdDB[table], err = sql.Open("postgres", connStr)
			checkErr(err)
			defer wdDB[table].Close()
			wdTX[table], err = wdDB[table].Begin()
			checkErr(err)
		}
		for _, table := range wdtables {
			str := strings.Replace(createTableStr, "{{.tablename}}", table, -1)
			fmt.Println("executing:", str)
			_, err := wdTX[table].Exec(str)
			checkErr(err)
		}
		for _, table := range wdtables {
			wdStmt[table], err = wdTX[table].Prepare(pq.CopyInSchema("wd", table,
				"wd_id",
				"wd_label",
				"wd_lang",
				"a_wof_type",
				"nclaims",
				"nlabels",
				"ndescriptions",
				"naliases",
				"nsitelinks",
				"ncebsitelinks",
				"iscebuano",
				"geomhash",
				"geom",
				"data",
			))
			checkErr(err)
		}

		//
		// PG setup  for label
		//

		db_label, err := sql.Open("postgres", connStr)
		checkErr(err)
		defer db_label.Close()
		txn_label, err = db_label.Begin()
		checkErr(err)
		createTableStr_label := []string{
			"CREATE SCHEMA IF NOT EXISTS wdlabels;",
			"DROP TABLE IF EXISTS wdlabels.qlabel CASCADE;",
			"CREATE UNLOGGED TABLE wdlabels.qlabel (wd_id TEXT NOT NULL, wd_label TEXT, wd_qlabel TEXT ,wd_qlang TEXT );",
		}
		for _, str := range createTableStr_label {
			fmt.Println("executing:", str)
			_, err := txn_label.Exec(str)
			checkErr(err)
		}
		stmt_label, err = txn_label.Prepare(pq.CopyInSchema("wdlabels", "qlabel", "wd_id", "wd_label", "wd_qlabel", "wd_qlang"))
		checkErr(err)

	}

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

		var wikidata WikiData

		wikidata.ID = gjson.GetBytes(b, "id").String()

		if len(wikidata.ID) <= 1 {
			return nil, nil
		}
		if wikidata.ID[0] != 'Q' {
			return nil, nil
		}

		// debug
		//if wikidata.ID != "Q27" {
		//	return nil, nil
		//} else {
		//	fmt.Println(wikidata.ID)
		//}
		// debug

		if sample && len(wikidata.ID) > 5 {
			return nil, nil
		}

		// replace last coma to space
		if b[len(b)-2] == 44 {
			b[len(b)-2] = 32
		}

		wikidata.AddWikidataJsonClean(b)

		if _, ok := wikidata.WikiItems.Labels["en"]; ok {
			wikidata.wdelabel = wikidata.WikiItems.Labels["en"].Value
			wikidata.wdqlabel = wikidata.WikiItems.Labels["en"].Value
			wikidata.wdqlang = "en"
		} else {
			wikidata.wdelabel = wikidata.ID

			wikidata.wdqlabel = ""
			wikidata.wdqlang = ""

			// find first preferred wd Label
			wdqLangFound := false
			for _, pLang := range preferredLangSlice {
				if _, ok := wikidata.WikiItems.Labels[pLang]; ok {
					wikidata.wdqlabel = wikidata.WikiItems.Labels[pLang].Value
					wikidata.wdqlang = wikidata.WikiItems.Labels[pLang].Language
					wdqLangFound = true
					break
				}
			}
			// If not found any label is OK
			if !wdqLangFound && len(wikidata.WikiItems.Labels) > 0 {
				for _, label := range wikidata.WikiItems.Labels {
					wikidata.wdqlabel = label.Value
					wikidata.wdqlang = label.Language
					break
				}
			}

		}

		if wikidata.wdqlabel == "" {
			wikidata.wdqlabel = wikidata.ID
		} else {
			// clean wikidata label : split by ',('
			// 'Eutenhofen (Dietfurt an der Altmühl)'	-> 'Eutenhofen'
			// 'Sussex Corner, New Brunswick' 			-> 'Sussex Corner'
			wikidata.wdqlabel = strings.TrimSpace(rexpl.Split(wikidata.wdqlabel, -1)[0])
		}

		if pgoutput {
			_, err = stmt_label.Exec(wikidata.ID, wikidata.wdelabel, wikidata.wdqlabel, wikidata.wdqlang)
			checkErr(err)
		}

		// Drop  P576: dissolved, demolished
		//if _, p576Exist := wikidata.WikiItems.Claims["P576"]; p576Exist {
		//	return nil, nil
		//}

		p31claimIds := []string{}
		if p31claim, p31Exist := wikidata.WikiItems.Claims["P31"]; p31Exist {

			p31claimgrp := wdType{}
			// check P31claims ...
			for _, dvobject := range p31claim {
				k := gjson.GetBytes(dvobject.Mainsnak.DataValue.Value, "id").String()
				p31claimIds = append(p31claimIds, k)
				if wgrp, ok := wiki2grp[k]; ok {
					for _, grp := range wgrp {
						if _, ok := p31claimgrp[grp]; ok {
							// Do nothing
						} else {
							p31claimgrp[grp] = true
						}

					}
				}
			}

			for grpkey, _ := range p31claimgrp {
				wikidata.match = append(wikidata.match, grpkey)
			}

			// Sort
			sort.Strings(wikidata.match)
		}

		// check if already wof referenced
		if ok := wofwd[wikidata.ID]; ok {
			wikidata.match = append(wikidata.match, "wof")
		}
		// check if already redirected
		if ok := wofredirected[wikidata.ID]; ok {
			wikidata.match = append(wikidata.match, "redirected")
		}

		// Any "admimcode" exist?  ::
		//    ?? has any "Wikidata property for authority control for administrative subdivisions"
		for k := range wikidata.WikiItems.Claims {
			if ok := adminCodelist[k]; ok {
				wikidata.match = append(wikidata.match, "admincode")
				break
			}
		}

		

		wikidata.checkClaims("P1566", "hasP1566") // check GeoNames ID ; P1566
		wikidata.checkClaims("P300", "P300")      // check ISO 3166-2 code ;  P300
		wikidata.checkClaims("P901", "P901")      // check FIPS 10-4 (countries and regions) :P901
		wikidata.checkClaims("P238", "P238")      // check IATA airport code : P238
		wikidata.checkClaims("P239", "P239")      // check ICAO airport code : P239
		wikidata.checkClaims("P240", "P240")      // check FAA airport code : P240
		wikidata.checkClaims("P1336", "P1336")    // check territory claimed by ; P1336
		wikidata.checkClaims("P1624", "P1624")    // check MarineTraffic Port ID  ; P1624

		wikidata.checkClaims("P6766", "P6766")    // check Who's on First ID (P6766)

		// check statement disputed by ; P1310
		p1310v := gjson.GetBytes(wikidata.WikiJson, "claims.P17.#.qualifiers.P1310")
		if len(p1310v.Array()) > 0 {
			wikidata.match = append(wikidata.match, "P1310")
		}

		//fmt.Println(wikidata.ID, wikidata.wdqlabel, wikidata.match)

		if pgoutput {
			// log
			if pgoutput && ((c.v % 100000) == 0) {
				fmt.Println("..processing:", c.v, "   wikidata.ID:", wikidata.ID, wikidata.wdqlabel, wikidata.match)
			}
		}

		// if no any match - we will drop!
		if len(wikidata.match) == 0 {
			return nil, nil
		}

		wikidata.setCoordinates()
		wikidata.setCebuano()

		// ------------------- business --------------------------------------
		// check wikidata.ID  in "business" ?
		if ok := businesslist[wikidata.ID]; ok {
			wikidata.match = append(wikidata.match, "business")
		} else {
			// check P31(instance of) "business"  ?
			for _, k := range p31claimIds {
				if ok := businesslist[k]; ok {
					wikidata.match = append(wikidata.match, "business")
					break
				}
			}
		}

		// ------------------- wikimedia --------------------------------------
		// check wikidata.ID  in "wikimedia" ?
		if ok := wikimedialist[wikidata.ID]; ok {
			wikidata.match = append(wikidata.match, "wikimedia")
		} else {
			// check P31(instance of) "wikimedia"  ?
			for _, k := range p31claimIds {
				if ok := wikimedialist[k]; ok {
					wikidata.match = append(wikidata.match, "wikimedia")
					break
				}
			}
		}

		// -------------------- duplicated ------------------------------------
		// check wikidata.ID  in duplicated ?
		if ok := duplicatedlist[wikidata.ID]; ok {
			wikidata.match = append(wikidata.match, "duplicated")
		} else {
			// check P31(instance of) "duplicated"  ?
			for _, k := range p31claimIds {
				if ok := duplicatedlist[k]; ok {
					wikidata.match = append(wikidata.match, "duplicated")
					break
				}
			}
		}

		// -------------------- disambiguation ------------------------------------
		// check wikidata.ID  in disambiguation ?
		if ok := disambiguationlist[wikidata.ID]; ok {
			wikidata.match = append(wikidata.match, "disambiguation")
		} else {
			// check P31(instance of) "disambiguation"  ?
			for _, k := range p31claimIds {
				if ok := disambiguationlist[k]; ok {
					wikidata.match = append(wikidata.match, "disambiguation")
					break
				}
			}
		}

		// -------------------- blacklist ------------------------------------
		// check wikidata.ID  in blacklist ?
		if ok := blacklist[wikidata.ID]; ok {
			wikidata.match = append(wikidata.match, "blacklist")
		} else {
			// check P31(instance of) "blacklist"  ?
			for _, k := range p31claimIds {
				if ok := blacklist[k]; ok {
					wikidata.match = append(wikidata.match, "blacklist")
					break
				}
			}
		}

		// ---------------------- metalist ------------------------------------
		// check wikidata.ID  in metalist for mathcing ?
		if _, ok := wiki2grp[wikidata.ID]; ok {
			wikidata.match = append(wikidata.match, "metalist")
		}

		// check demolished
		wikidata.checkClaims("P576", "demolished") // check dissolved, demolished ;  P576
		// check P279subclass of
		wikidata.checkClaims("P279", "hasP279") // check "Subclass of" ; P279

		if !pgoutput {
			// Write to text output
			return append(wikidata.WikiJson, '\n'), nil
		} else {
			// Write to Postgres
			if contains(wikidata.match, "demolished") &&
				(!contains(wikidata.match, "admincode")) && (!contains(wikidata.match, "hasP1566")) {
				wikidata.writePG_wd(wdStmt["wd_demolished"])
			} else if !wikidata.p625.Null {
				if wikidata.IsCebuano {
					wikidata.writePG_wd(wdStmt["wd_messy"])
				} else {
					wikidata.writePG_wd(wdStmt["wd_ok"])
				}
			} else {
				if len(wikidata.match) == 1 &&
					(wikidata.match[0] == "locality" ||
						wikidata.match[0] == "marinearea" ||
						wikidata.match[0] == "river" ||
						wikidata.match[0] == "landform" ||
						wikidata.match[0] == "county") {

					wikidata.writePG_wd(wdStmt["wd_nogeom_grp1"])
				} else {
					wikidata.writePG_wd(wdStmt["wd_nogeom_grp2"])
				}
			}
		}
		return nil, nil
	})

	// Start processing with parallel workers.
	// wpp.NumWorkers = 1
	err = wpp.Run()
	checkErr(err)

	if pgoutput {

		//
		// Close WD Copy
		//
		for _, table := range wdtables {
			_, err = wdStmt[table].Exec()
			checkErr(err)
			err = wdStmt[table].Close()
			checkErr(err)
			err = wdTX[table].Commit()
			checkErr(err)
		}

		//
		// Close Label Copy
		//
		_, err = stmt_label.Exec()
		checkErr(err)
		err = stmt_label.Close()
		checkErr(err)

		err = txn_label.Commit()
		checkErr(err)

		fmt.Println("...  wd.wd_*  + wdlabels.qlabel Loaded:", c.v)

	}
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
func (wikidata *WikiData) AddWikidataJsonClean(content []byte) {

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
					// If has "P582 - end time" - we don't keep and don't load to Postgres
					// https://www.wikidata.org/wiki/Property:P582 "indicates the time an item ceases to exist or a statement stops being valid"
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

	wikidata.WikiItems = WikiItems{
		ID:           m.ID,
		Claims:       newClaims,
		Labels:       m.Labels,
		Descriptions: m.Descriptions,
		Aliases:      m.Aliases,
		Sitelinks:    m.Sitelinks,
	}

	mj, err := json.Marshal(wikidata.WikiItems)
	checkErr(err)
	wikidata.WikiJson = mj

	//fmt.Println(string(mj))

	wikidata.nClaims = len(newClaims)
	wikidata.nLabels = len(m.Labels)
	wikidata.nDescriptions = len(m.Descriptions)
	wikidata.nAliases = len(m.Aliases)
	wikidata.nSitelinks = len(m.Sitelinks)
	wikidata.nCebSitelinks = 0
	wikidata.IsCebuano = false

}

func (wikidata *WikiData) setCebuano() {

	if wikidata.nSitelinks > 4 {
		return
	}

	// Strict Cebuano settings

	//{shwiki,srwiki}	49858
	//{shwiki,srwiki,svwiki,cebwiki}	21374
	//{cebwiki}	18888
	//{svwiki,cebwiki}	15741
	//{shwiki}	5033
	//{shwiki,svwiki,cebwiki}	3269

	_, cebExists := wikidata.WikiItems.Sitelinks["cebwiki"]
	_, ladExists := wikidata.WikiItems.Sitelinks["ladwiki"]
	_, svExists := wikidata.WikiItems.Sitelinks["svwiki"]
	_, srExists := wikidata.WikiItems.Sitelinks["srwiki"]
	_, shExists := wikidata.WikiItems.Sitelinks["shwiki"]
	_, iaExists := wikidata.WikiItems.Sitelinks["iawiki"]
	_, msExists := wikidata.WikiItems.Sitelinks["mswiki"]
	_, huExists := wikidata.WikiItems.Sitelinks["huwiki"]
	_, plExists := wikidata.WikiItems.Sitelinks["plwiki"]
	_, csExists := wikidata.WikiItems.Sitelinks["cswiki"]
	_, skExists := wikidata.WikiItems.Sitelinks["skwiki"]
	_, roExists := wikidata.WikiItems.Sitelinks["rowiki"]
	_, simpleExists := wikidata.WikiItems.Sitelinks["simplewiki"]
	_, commonsExists := wikidata.WikiItems.Sitelinks["commonswiki"]

	// set cebuano values
	if cebExists {
		wikidata.nCebSitelinks = 1
	}

	switch wikidata.nSitelinks {

	case 0: // No Site  -- probably imported
		wikidata.IsCebuano = true

	case 1:
		if cebExists { // only 1 cebuano
			wikidata.IsCebuano = true
		} else if iaExists { //  interlingua language  https://en.wikipedia.org/wiki/Interlingua
			wikidata.IsCebuano = true
		} else if shExists { //
			wikidata.IsCebuano = true
		} else if ladExists { //
			wikidata.IsCebuano = true
		} else if msExists && !areaBoxMs.Contains(wikidata.p625.Lat, wikidata.p625.Lng) { //
			wikidata.IsCebuano = true
		} else if huExists && !areaBoxHu.Contains(wikidata.p625.Lat, wikidata.p625.Lng) { //
			wikidata.IsCebuano = true
		} else if roExists && !areaBoxRo.Contains(wikidata.p625.Lat, wikidata.p625.Lng) { //
			wikidata.IsCebuano = true
		} else if plExists && !areaBoxPl.Contains(wikidata.p625.Lat, wikidata.p625.Lng) { //
			wikidata.IsCebuano = true
		} else if csExists && !areaBoxCsSk.Contains(wikidata.p625.Lat, wikidata.p625.Lng) { //
			wikidata.IsCebuano = true
		} else if skExists && !areaBoxCsSk.Contains(wikidata.p625.Lat, wikidata.p625.Lng) { //
			wikidata.IsCebuano = true
		} else if svExists && !areaBoxSv.Contains(wikidata.p625.Lat, wikidata.p625.Lng) { //
			wikidata.IsCebuano = true
		} else if srExists && !areaBoxSr.Contains(wikidata.p625.Lat, wikidata.p625.Lng) { //
			wikidata.IsCebuano = true
		} else if commonsExists {
			wikidata.IsCebuano = true
		} else if simpleExists {
			wikidata.IsCebuano = true
		} else {
			// only  wikivoyage ->  cebuano
			for k := range wikidata.WikiItems.Sitelinks {
				if reWikivoyage.MatchString(k) == true {
					wikidata.IsCebuano = true
				} else if len(k) > 7 {
					wikidata.IsCebuano = true
				} else if k == "cywiki" { // Welsh
					wikidata.IsCebuano = true
				} else if k == "eowiki" { // Esperanto
					wikidata.IsCebuano = true
				} else if k == "euwiki" { // Basque
					wikidata.IsCebuano = true
				}
				break
			}
		}

	case 2:
		if shExists && srExists && (!wikidata.p625.Null) {
			// check geohash - and if it is not serbia - then it is problematic

			if !areaBoxSr.Contains(wikidata.p625.Lat, wikidata.p625.Lng) {
				wikidata.IsCebuano = true
			}

		} else if cebExists && svExists && (!wikidata.p625.Null) { // cebuano + svedish + has Coordinate
			// check geohash - and if it is not svedish - then it is problematic

			if !areaBoxSv.Contains(wikidata.p625.Lat, wikidata.p625.Lng) {
				wikidata.IsCebuano = true
			}

		}

	case 3:
		if shExists && svExists && cebExists && (!wikidata.p625.Null) {
			//  not svedish  and not serbian - then it is problematic
			if (!areaBoxSr.Contains(wikidata.p625.Lat, wikidata.p625.Lng)) &&
				(!areaBoxSv.Contains(wikidata.p625.Lat, wikidata.p625.Lng)) {
				wikidata.IsCebuano = true
			}
		}

	case 4:
		if shExists && svExists && srExists && cebExists && (!wikidata.p625.Null) {
			// check geohash - and if it is not svedish - then it is problematic
			//  not svedish  and not serbian - then it is problematic
			if (!areaBoxSr.Contains(wikidata.p625.Lat, wikidata.p625.Lng)) &&
				(!areaBoxSv.Contains(wikidata.p625.Lat, wikidata.p625.Lng)) {
				wikidata.IsCebuano = true
			}

		}
	}
}

func (wikidata *WikiData) setCoordinates() {

	wikidata.gphash = ""
	wikidata.gp.Null = true
	wikidata.p625.Null = true

	cp625, p625Exist := wikidata.WikiItems.Claims["P625"]
	if p625Exist {
		var lat, lng float64
		null := true
		for _, v := range cp625 {
			if v.Mainsnak.Snaktype == "value" &&
				v.Mainsnak.DataValue.Type == "globecoordinate" &&
				gjson.GetBytes(v.Mainsnak.DataValue.Value, "globe").String() == "http://www.wikidata.org/entity/Q2" {
				// some claims has "no value"  - so we don't import them.
				// example:  https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q36823    P36   snaktype": "novalue"  ( No de jure Capital)

				if v.Rank == "preferred" {
					wikidata.p625.Lat = gjson.GetBytes(v.Mainsnak.DataValue.Value, "latitude").Float()
					wikidata.p625.Lng = gjson.GetBytes(v.Mainsnak.DataValue.Value, "longitude").Float()
					wikidata.p625.Null = false
					wikidata.gp = wikidata.p625
					wikidata.gphash = geohash.Encode(wikidata.p625.Lat, wikidata.p625.Lng)
					wikidata.match = append(wikidata.match, "hasP625")
					return
				} else if v.Rank == "normal" {
					if null {
						lat = gjson.GetBytes(v.Mainsnak.DataValue.Value, "latitude").Float()
						lng = gjson.GetBytes(v.Mainsnak.DataValue.Value, "longitude").Float()
						null = false
					}
				} else {
					panic("Extreeme-Rank-value")
				}
			}
		}
		if !null {
			wikidata.p625.Lat = lat
			wikidata.p625.Lng = lng
			wikidata.p625.Null = null

			wikidata.gp = wikidata.p625
			wikidata.gphash = geohash.Encode(wikidata.p625.Lat, wikidata.p625.Lng)
			wikidata.match = append(wikidata.match, "hasP625")
		}

	}
}

func (wikidata *WikiData) writePG_wd(stmt_wd *sql.Stmt) {
	// write to postgres

	_, err := stmt_wd.Exec(
		wikidata.ID,
		wikidata.wdqlabel,
		wikidata.wdqlang,
		wikidata.match,
		wikidata.nClaims,
		wikidata.nLabels,
		wikidata.nDescriptions,
		wikidata.nAliases,
		wikidata.nSitelinks,
		wikidata.nCebSitelinks,
		wikidata.IsCebuano,
		wikidata.gphash,
		wikidata.gp,
		string(wikidata.WikiJson))
	checkErr(err)
}

func (wikidata *WikiData) checkClaims(claimid string, claimgrp string) {
	if _, pExist := wikidata.WikiItems.Claims[claimid]; pExist {
		wikidata.match = append(wikidata.match, claimgrp)
	}
}

func CreateAreaBox(pMinLng float64, pMinLat float64, pMaxLng float64, pMaxLat float64) geohash.Box {
	return geohash.Box{
		MinLat: pMinLat,
		MaxLat: pMaxLat,
		MinLng: pMinLng,
		MaxLng: pMaxLng,
	}
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}
