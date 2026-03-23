package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

type Record struct {
	Seed string `json:"seed"`
	Hash string `json:"hash"`
}

type DiffResult struct {
	Matches    int
	Mismatches []string
	MissingInA []string
	MissingInB []string
}

func readJSONL(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	result := make(map[string]string)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		var r Record
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			return nil, err
		}
		if r.Seed == "" || r.Hash == "" {
			return nil, fmt.Errorf("invalid record: %+v", r)
		}
		result[r.Seed] = r.Hash
	}
	return result, scanner.Err()
}

func diff(a, b map[string]string) DiffResult {
	res := DiffResult{}

	for seed, hashA := range a {
		hashB, ok := b[seed]
		if !ok {
			res.MissingInB = append(res.MissingInB, seed)
			continue
		}
		if hashA == hashB {
			res.Matches++
		} else {
			res.Mismatches = append(res.Mismatches, seed)
		}
	}

	for seed := range b {
		if _, ok := a[seed]; !ok {
			res.MissingInA = append(res.MissingInA, seed)
		}
	}

	sort.Strings(res.MissingInA)
	sort.Strings(res.MissingInB)
	sort.Strings(res.Mismatches)
	return res
}

func main() {
	map1, err := readJSONL(os.Args[1])
	if err != nil {
		log.Fatalf("could not parse first file %v", err)
	}
	map2, err := readJSONL(os.Args[2])
	if err != nil {
		log.Fatalf("could not parse second file %v", err)
	}

	d := diff(map1, map2)

	fmt.Printf("matches: %d\n", d.Matches)
	fmt.Printf("mismatches: %d\n", len(d.Mismatches))
	fmt.Printf("missing in A: %d\n", len(d.MissingInA))
	fmt.Printf("missing in B: %d\n", len(d.MissingInB))

}
