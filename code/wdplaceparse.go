// Filter Wikidata JSON dump  Order of lines is not preserved.
//
//       go run ./code/wdplaceparse.go /wof/code/wikidata_county_district.csv
//       go run ./code/wdplaceparse.go /wof/code/wikidata_localities.csv
//  time go run ./code/wdplaceparse.go /wof/code/wikidata_localities.csv > /wof/wikidata_dump/wdplace.json
//  go run ./code/wdplaceparse.go /wof/code/wikidata_localities.csv | split -d --additional-suffix=.json  -n r/4 - /wof/wikidata_dump/wdplace

package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	gzip "github.com/klauspost/pgzip"
	"github.com/miku/parallel"
	"github.com/tidwall/gjson"
)

func main() {

	csvcode := os.Args[1]
	wdtype := make(map[string]bool)
	//content, err := ioutil.ReadFile("/wof/code/wikidata_city_town.csv")
	//content, err := ioutil.ReadFile("/wof/code/wikidata_localities.csv")
	content, err := ioutil.ReadFile(csvcode)
	if err != nil {
		fmt.Print(err)
	}

	var qcode string
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)

		if len(line) >= 2 && line[0] == 'Q' {
			qcode = strings.Split(line, ",")[0]

			// remove spaces
			qcode = strings.Replace(qcode, " ", "", -1)

			// only add 'Q' codes,
			if qcode[0] == 'Q' {
				wdtype[qcode] = true
			}
		} else {
			// not added , maybe a comment
		}

	}

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

	// Setup input, output and business logic.

	p := parallel.NewProcessor(gzipreader, os.Stdout, func(b []byte) ([]byte, error) {
		p31claims := gjson.GetBytes(b, "claims.P31.#.mainsnak.datavalue.value.id")
		for _, k := range p31claims.Array() {
			if ok := wdtype[k.String()]; ok {
				// replace last coma to space
				if b[len(b)-2] == 44 {
					b[len(b)-2] = 32
				}
				return b, nil
			}
		}
		return nil, nil
	})

	// Start processing with parallel workers.
	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
