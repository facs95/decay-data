package query

import (
	"encoding/json"
	"net/http"

	"io/ioutil"
	"log"
)

var client = &http.Client{}
var clientUrl = "https://tendermint.bd.evmos.org:26657/"

func GetBlockResult(height string) (*BlockResult, error) {
	balance_start := "block_results?height="
	url := balance_start + height
	body, err := makeRequest(url, height)
	if err != nil {
		log.Printf("Error making request for height: %v", height)
		return nil, err
	}
	m := &BlockResult{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		log.Printf("Error parsing the body for height %v, with err %v", height, err)
		return nil, err
	}
	return m, nil
}

func makeRequest(endpoint string, height string) ([]byte, error) {
	req, _ := http.NewRequest("GET", clientUrl+endpoint, nil)
	res, err := client.Do(req)
	if err != nil {
		log.Println("Error http request", endpoint, err)
		return nil, err
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println("Error reading the response body", endpoint, err)
		return nil, err
	}
	return body, nil
}
