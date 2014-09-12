package main

import (
	"bytes"
	"code.google.com/p/go.crypto/ssh"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
)

const (
	ShutDownRetries = 5
	documentQuery   = "q=_type:couchbaseDocument"
)

type ESNode struct {
	Ip            string `json:"ip"`
	Port          string `json:"port"`
	AdminUserName string `json:"username"`
	AdminPassword string `json:"password"`
	ConnectorPort string `json:"connector-port"`
	Client        *http.Client
	BaseURL       string
	ESPort        string
}

func (node *ESNode) StartService() (err error) {
	config := &ssh.ClientConfig{
		User: node.AdminUserName,
		Auth: []ssh.AuthMethod{
			ssh.Password(node.AdminPassword),
		},
	}

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:22", node.Ip), config)
	if err != nil {
		return err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	command := "elasticsearch-1.3.2/bin/elasticsearch > out.log 2>&1 &"

	var b bytes.Buffer
	session.Stdout = &b
	if err := session.Run(command); err != nil {
		fmt.Printf("Failed to run command %s", command)
	}
	fmt.Println(b.String())
	return nil
}

func (node *ESNode) Init() {
	//userinfo := url.UserPassword(node.AdminUserName, node.AdminPassword)

	if node.Ip == "" || node.Port == "" {
		fmt.Printf("IP and port of the es node are needed")
		os.Exit(1)
	}

	url := &url.URL{
		Scheme: "http",
		Host:   node.Ip + ":" + node.Port,
	}
	node.BaseURL = url.String()
	node.Client = &http.Client{}
	return
}

func (node *ESNode) CreateIndex(index string) (err error) {
	api := fmt.Sprintf("%s/%s", node.BaseURL, index)

	req, err := http.NewRequest("PUT", api, nil)
	req.Header.Add("Accept", "application/json")

    fmt.Printf("api %s", api)
    fmt.Printf("%v",req)
	resp, err := node.Client.Do(req)

	if err != nil {
		fmt.Printf("\n Unable to create Index: %v ", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		fmt.Printf("Got HTTP response %v on creation", resp.Status)
		return err
	}

	return nil
}

func (node *ESNode) DeleteIndex(index string) (err error) {
	api := fmt.Sprintf("%s/%s", node.BaseURL, index)

	req, err := http.NewRequest("DELETE", api, nil)
	req.Header.Add("Accept", "application/json")

	resp, err := node.Client.Do(req)

	if err != nil {
		fmt.Printf("\n Unable to delete Index: %v ", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Got HTTP response %v on creation", resp.Status)
		return err
	}

	return nil

}

func (node *ESNode) GetCount(index string) (repCount int, err error) {
	query := "_type:couchbaseDocument"

	api := fmt.Sprintf("%s/%s/_count?q=%s", node.BaseURL, index, query)

	resp, err := node.Client.Get(api)
	if err != nil {
        return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = errors.New(fmt.Sprintf("Got HTTP Response %v on getting count", resp.Status))
        return 0, err 
	}

	respJson := make(map[string]interface{})

	if body, err := ioutil.ReadAll(resp.Body); err == nil {
		fmt.Printf("val %s", body)
		err = json.Unmarshal(body, &respJson)
		if err != nil {
			fmt.Printf("Unable to parse the response JSON. Error %v", err)
            return 0, err
		}
	}

	count, ok := (respJson["count"]).(float64)
	if !ok {
		err = errors.New(fmt.Sprintf("Unable to convert to int %v %v", ok, count))
        return 0, err
	}

	return int(count), nil
}

func (node *ESNode) StopService() (err error) {
	config := &ssh.ClientConfig{
		User: node.AdminUserName,
		Auth: []ssh.AuthMethod{
			ssh.Password(node.AdminPassword),
		},
	}

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:22", node.Ip), config)
	if err != nil {
		return err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	command := "pkill -f elasticsearch"

	if err := session.Run(command); err != nil {
		fmt.Printf("Failed to run command %s", command)
	}
	return nil
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
*/

func (node *ESNode) Search(index, query string) (count int, err error) {
	values := url.Values{}
	values.Set("pretty", "true")
	values.Set("q", query)

	api := fmt.Sprintf("%s/%s/%s", node.BaseURL, index, "_search?"+values.Encode())

	req, err := http.NewRequest("GET", api, nil)
	req.Header.Add("Accept", "application/json")

	resp, err := node.Client.Do(req)
	if err != nil {
		fmt.Printf("\n Unable to search for the docs\n")
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Got HTTP Response %v", resp.Status)
		return 0, errors.New("HTTP error")
	}

	var response map[string]interface{}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("\n Error reading the response \n")
		return 0, err
	}

	err = json.Unmarshal(body, response)
	if err != nil {
		fmt.Printf("Unable to parse the response JSON")
		return 0, err
	}

	count, ok := response["count"].(int)
	if !ok {
		count = 0
		err = errors.New("Unable to convert to int")
	}
	return count, nil
}

/*
func (node *ESNode) Shutdown() {
	api := path.Join(node.BaseURI, "_cluster", "nodes", "_local", "shutdown")
	for i := 0; i < ShutDownRetries; i++ {
		resp, err := node.Client.Post(api)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Got HTTP response %v on trying to shut down a node")
		}
	}
}
*/
