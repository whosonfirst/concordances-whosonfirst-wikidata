// Filter Wikidata JSON dump  Order of lines is not preserved.
//
//  $ head wof_wd_id.json | go run ./code/wdwofparse.go

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/miku/parallel"
	"github.com/tidwall/gjson"
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

	// Setup input, output and business logic.
	p := parallel.NewProcessor(os.Stdin, os.Stdout, func(b []byte) ([]byte, error) {
		wdid := gjson.GetBytes(b, "id").String()
		_, ok := wq[wdid]
		if !ok {
			return nil, nil
		}

		// replace last coma to space
		if b[len(b)-2] == 44 {
			b[len(b)-2] = 32
		}
		return b, nil
	})

	// Start processing with parallel workers.
	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
