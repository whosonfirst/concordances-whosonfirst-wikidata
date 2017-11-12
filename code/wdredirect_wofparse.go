// Filter Wikidata redirect csv  ; Order of lines is not preserved.
//
//  cat /wof/wikidata_dump/wikidata_redirects.csv | go run ./code/wdredirect_wofparse.go  > ...
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/miku/parallel"
)

func main() {

	wq := make(map[string]bool)
	content, err := ioutil.ReadFile("/wof/whosonfirst-data/wd.txt")
	if err != nil {
		fmt.Print(err)
	}

	var qcode string
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if len(line) >= 2 && line[0] == 'Q' {
			qcode = line
			wq[qcode] = true
		} else {
		}

	}

	p := parallel.NewProcessor(os.Stdin, os.Stdout, func(b []byte) ([]byte, error) {
		wdid := strings.Split(string(b), ",")[0]
		_, ok := wq[wdid]

		if !ok {
			return nil, nil
		}
		return b, nil
	})

	// Start processing with parallel workers.
	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
