package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
)

const (
	ShutDownRetries = 5
)

type ESNode struct {
	IP            string `json:"ip"`
	Version       string `json:"port"`
	AdminUserName string `json:"username"`
	AdminPassword string `json:"password"`
	Client        *http.Client
	BaseURL       string
	ESPort        string
}

func (node *ESNode) Init(ip, port, version string) {
	fmt.Printf("Initializing es node info")
	//userinfo := url.UserPassword(node.AdminUserName, node.AdminPassword)

	if ip == "" || port == "" {
		fmt.Printf("IP and port of the es node are needed")
		os.Exit(1)
	}

	url := &url.URL{
		Scheme: "http",
		//	User:   userinfo,
		Host: ip + ":" + port,
	}
	node.BaseURL = url.String()
	node.Client = &http.Client{}
	return
}

func (node *ESNode) CreateIndex(index string) {
	api := fmt.Sprintf("%s/%s", node.BaseURL, index)

	fmt.Printf("api %s", api)
	req, err := http.NewRequest("PUT", api, nil)
	req.Header.Add("Accept", "application/json")
	resp, err := node.Client.Do(req)

	if err != nil {
		fmt.Printf("Unable to create Index")
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Got HTTP response %v on creation", resp.Status)
		os.Exit(1)
	}

	return
}

func (node *ESNode) GetCount(index string) int {
	api := fmt.Sprintf("%s/%s/%s", node.BaseURL, index, "_count")

	resp, err := node.Client.Get(api)
	if err != nil {
		fmt.Printf("Unable to get count")
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Got HTTP Response %v on getting count", resp.Status)
		return 0
	}

	respJson := make(map[string]interface{})

	if body, err := ioutil.ReadAll(resp.Body); err == nil {
		fmt.Printf("val %s", body)
		err = json.Unmarshal(body, &respJson)
		if err != nil {
			fmt.Printf("Unable to parse the response JSON. Error %v", err)
			os.Exit(1)
		}
	}

	count, ok := (respJson["count"]).(float64)
	if !ok {
		fmt.Printf("Unable to convert to int %v %v", ok, count)
		count = 0
	}

	return int(count)
}

/*
func (e *ESNode) GetCouchbaseCheckpointCount(index string) int {
	api := path.Join(e.BaseURI, index, "couchbaseCheckpoint", "_count")

	resp, err := e.Client.Get(api)
	if err != nil {
		fmt.Printf("Unable to get Couchbase checkpoint count")
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Got HTTp Response %v on getting couchbase checkpoint count", resp.Status)
		os.Exit(1)
	}
	var response map[string]interface{}

	err = json.Unmarshal(resp.Body, response)
	if err != nil {
		fmt.Printf("Unable to parse the response JSON")
		os.Exit(1)
	}

	val := response["count"]
	count, ok := val.(int)
	if !ok {
		fmt.Printf("Unable to convert to int")
		count = 0
	}

	return count

}
/*
func (e *ESNode) GetCouchbaseDocument(index string) int {
	api := path.Join(e.BaseURI, index, "couchbaseDocument", "_count")

	resp, err := e.Client.Get(api)
	if err != nil {
		fmt.Printf("Unable to get Couchbase document count")
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Got HTTP Response %v on getting couchbase document count", resp.Status)
		os.Exit(1)
	}
	var response map[string]interface{}

	err = json.Unmarshal(resp.Body, response)
	if err != nil {
		fmt.Printf("Unable to parse the response JSON")
		os.Exit(1)
	}

	count, ok := response["count"].(int)
	if !ok {
		fmt.Printf("Unable to convert to int")
		count = 0
	}

	return count

}

func (e *ESnode) Search(index, query string) {
	values := url.Values{}
	values.Set("pretty", "true")
	values.Set("q", query)

	api := path.Join(e.BaseURI, index, "_search?"+values.Encode())
	resp, err := e.Client.Get(api)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Got HTTP Response %v", resp.Status)
		os.Exit(1)
	}
	var response map[string]interface{}

	err = json.Unmarshal(resp.Body, response)
	if err != nil {
		fmt.Printf("Unable to parse the response JSON")
		os.Exit(1)
	}
	count, ok := response["count"].(int)
	if !ok {
		fmt.Printf("Unable to convert to int")
		count = 0
	}

	return count

}

func (e *ESNode) Shutdown() {
	api := path.Join(e.BaseURI, "_cluster", "nodes", "_local", "shutdown")

	for i := 0; i < ShutDownRetries; i++ {
		resp, err := e.Client.Post(api)
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Got HTTP response %v on trying to shut down a node")
		}
	}

} */
