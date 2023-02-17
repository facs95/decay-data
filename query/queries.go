package query

import (
	"encoding/json"
	"net/http"

	"fmt"
	"io/ioutil"
)

var client = &http.Client{}
var clientUrl = "https://tendermint.bd.evmos.org:26657/"

var eventMergeType = "merge_claims_records"
var eventMigratedType = "claim"
var actionIBCTransfer = "ACTION_IBC_TRANSFER"

func GetBlockResult(height string) (*BlockResult, error) {
	balance_start := "block_results?height="
	url := balance_start + height
	body, err := makeRequest(url, height)
	if err != nil {
		fmt.Println("Error making request for height: ", height)
		return nil, err
	}
	m := &BlockResult{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		fmt.Println("Error parsing the body", body, err)
		return nil, err
	}
	return m, nil
}

func makeRequest(endpoint string, height string) ([]byte, error) {
	req, _ := http.NewRequest("GET", clientUrl+endpoint, nil)
	res, err := client.Do(req)
	if err != nil {
		fmt.Println("Error http request", endpoint, err)
		return nil, err
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println("Error reading the response body", endpoint, err)
		return nil, err
	}
	return body, nil
}
