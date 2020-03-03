package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
)

type PersonJSON struct {
	ID int `json:"id"`
	From string `json:"from"`
	To string `json:"to"`
	Path []Subscriber `json:"path,omitempty"`
}

type Person struct {
	Nick string `json:"Nick"`
	Email string `json:"Email"`
	CreatedAt string `json:"Created_at"`
	Subscriber []Subscriber `json:"Subscribers"`
}

type Subscriber struct {
	Email string `json:"Email"`
	CreatedAt string `json:"Created_at"`
}

//JSON Decoding/Encoding  functions
func decodeJSONBody(filename string) ([]Person, error) {
	file, err := os.Open(filename)
	var personSlice []Person
	if err != nil {
		return nil, fmt.Errorf("error: %s", err)
	}

	defer file.Close()

	val, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("error: %s", err)
	}

	err = json.Unmarshal(val, &personSlice)
	if err != nil {
		return nil, fmt.Errorf("error: %s", err)
	}

	return personSlice, nil
}

func encodeJSONBody(filename string, res []PersonJSON) error {
	ans, err := json.MarshalIndent(res, "", "    ")
	if err != nil {
		return fmt.Errorf("error: %s", err)
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error: %s", err)
	}

	defer file.Close()

	writer := bufio.NewWriter(file)

	defer writer.Flush()

	_, err = writer.Write(ans)
	if err != nil {
		return fmt.Errorf("error: %s", err)
	}

	return nil
}

//Preprocess graph and invert it
func preprocessGetInvertedGraph(persons []Person) (map[string][]string, map[string]string) {
	subscriptionsGraph := make(map[string][]string)
	createdAtGraph := make(map[string]string)
	for _, user := range persons {
		for _, subscriber := range user.Subscriber {
			_, ok := subscriptionsGraph[subscriber.Email]
			if !ok {
				subscriptionsGraph[subscriber.Email] = make([]string, 0)
			}
			subscriptionsGraph[subscriber.Email] = append(subscriptionsGraph[subscriber.Email], user.Email)
			createdAtGraph[subscriber.Email] = subscriber.CreatedAt
		}
	}
	return subscriptionsGraph, createdAtGraph
}

//BFS (Shortest Path)
func bfsGetSubsPathHelper(subsGraph map[string][]string, startVertex string, endVertex string) []string {
	var queue []string
	queue = append(queue, startVertex)
	path := make(map[string][]string)
	path[startVertex] = make([]string, 0)

	for len(queue) > 0 {
		currentPerson := queue[0]
		currentPersonSubs := subsGraph[currentPerson]
		if currentPerson == endVertex {
			return path[currentPerson]
		}
		for _, sub := range currentPersonSubs {
			_, ok := path[sub]
			if !ok {
				path[sub] = make([]string, len(path[currentPerson]))
				copy(path[sub], path[currentPerson])
				if currentPerson != startVertex {
					path[sub] = append(path[sub], currentPerson)
				}
				queue = append(queue, sub)
			}
		}
		queue = append(queue[:0], queue[1:]...)
	}
	return nil
}

//calculate shortest distance using BFS
func calculateShortestPathsBetweenPeople(filename string, subsGraph map[string][]string, createdAtGraph map[string]string) ([]PersonJSON, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error: %s", err)
	}

	defer file.Close()

	read := csv.NewReader(bufio.NewReader(file))
	ans := make([]PersonJSON, 0)

	i := 1
	for {
		row, err := read.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error: %s", err)
		}

		start := row[0]
		end := row[1]
		bfsPath := bfsGetSubsPathHelper(subsGraph, start, end)
		path := make([]Subscriber, len(bfsPath))
		for i, subscriber := range bfsPath {
			path[i] = Subscriber{Email: subscriber, CreatedAt: createdAtGraph[subscriber]}
		}

		res := PersonJSON{ID: i, From: start, To: end, Path: path}
		ans = append(ans, res)
		i++
	}
	return ans, nil
}

func main() {
	personSlice, err  := decodeJSONBody("users.json")
	if err != nil {
		log.Fatal("error: ", err)
	}
	subs, createdAt := preprocessGetInvertedGraph(personSlice)
	res, err  := calculateShortestPathsBetweenPeople("input.csv", subs, createdAt)
	if err != nil {
		log.Fatal("error: ", err)
	}
	err = encodeJSONBody("result.json", res)
	if err != nil {
		log.Fatal("can't write a result:", err)
	}
}
