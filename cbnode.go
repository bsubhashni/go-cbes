package main

import (
	"code.google.com/p/go.crypto/ssh"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	"io/ioutil"
	"net/http"
	"net/url"
)

const (
	startRebalanceUri    = "/controller/rebalance"
	stopRebalanceUri     = "/controller/StopRebalance"
	addNodeUri           = "/controller/addNode"
	ejectNodeUri         = "/controller/ejectNode"
	rebalanceProgressUri = "/pools/default/rebalanceProgress"
	replicationUri       = "/controller/createReplication"
	failoverNodeUri      = "/controller/failOver"
	createBucketUri      = "/pools/default/buckets"
	flushBucketUri       = "/controller/doFlush"
	remoteClusterUri     = "/pools/default/remoteClusters"
)

type CouchbaseNode struct {
	Ip              string `json:"ip"`
	Port            string `json:"port"`
	BaseURL         string
	BucketDetails   Bucket
	AdminUserName   string `json:"username"`
	AdminPassword   string `json:"password"`
	CouchbaseClient couchbase.Client
	HttpClient      *http.Client
	Bucket          *couchbase.Bucket
	WorkloadCommand chan int
	KnownNodes      map[string]*CouchbaseNode
	EjectNodes      map[string]*CouchbaseNode
}

type Bucket struct {
	Name      string
	Itemcount string
}

type RebalanceStatus struct {
	status string
}

func (node *CouchbaseNode) StartService() (err error) {
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

	command := "/etc/init.d/couchbase-server start"
	if err := session.Run(command); err != nil {
		fmt.Printf("Failed to run command %s", command)
		return err
	}
	return nil
}

func (node *CouchbaseNode) Start() (err error) {
	userinfo := url.UserPassword(node.AdminUserName, node.AdminPassword)

	url := &url.URL{
		Scheme: "http",
		User:   userinfo,
		Host:   node.Ip + ":" + node.Port,
	}
	fmt.Printf("\n url str %s \n", url.String())
	c, err := couchbase.Connect(url.String())
	if err != nil {
		return err
	}
	node.CouchbaseClient = c
	node.HttpClient = &http.Client{}
	node.BaseURL = url.String()
	node.KnownNodes = make(map[string]*CouchbaseNode)
	node.EjectNodes = make(map[string]*CouchbaseNode)
	node.KnownNodes[node.Ip] = node
	return nil
}

func (node *CouchbaseNode) AddNode(n *CouchbaseNode) (err error) {
	values := url.Values{}
	values.Set("hostname", n.Ip)
	values.Set("user", n.AdminUserName)
	values.Set("password", n.AdminPassword)

	api := fmt.Sprintf("%s%s", node.BaseURL, addNodeUri)
	delete(node.KnownNodes, n.Ip)

	resp, err := node.HttpClient.PostForm(api, values)
	if err != nil {
		fmt.Printf("error getting response %v", err)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if body, err := ioutil.ReadAll(resp.Body); err == nil {
			fmt.Printf("\n body of the response with error %s", body)
		}
		return errors.New(fmt.Sprintf("Received a bad status %v", resp.Status))
	}
	node.KnownNodes[n.Ip] = n
	return nil
}

func (node *CouchbaseNode) EjectNode(n *CouchbaseNode) (err error) {
	values := url.Values{}
	values.Set("otpNode", fmt.Sprintf("ns_1@%s", n.Ip))
	api := fmt.Sprintf("%s%s", node.BaseURL, ejectNodeUri)

	node.EjectNodes[n.Ip] = n
	node.KnownNodes[n.Ip] = n

	resp, err := node.HttpClient.PostForm(api, values)
	if err != nil {
		fmt.Printf("Error getting a response")
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if body, err := ioutil.ReadAll(resp.Body); err == nil {
			fmt.Printf("\n body of the response with error %s", body)
		}
		return errors.New(fmt.Sprintf("Received a bad status %v", resp.Status))
	}
	node.EjectNodes[n.Ip] = n
	delete(node.KnownNodes, n.Ip)

	return nil
}

func (node *CouchbaseNode) FailoverNode(n *CouchbaseNode) (err error) {
	values := url.Values{}
	values.Set("otpNode", fmt.Sprintf("ns_2@%s", n.Ip))
	api := fmt.Sprintf("%s%s", node.BaseURL, failoverNodeUri)

	fmt.Printf("failover api %s", api)
	resp, err := node.HttpClient.PostForm(api, values)
	if err != nil {
		fmt.Printf("Error getting a response")
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if body, err := ioutil.ReadAll(resp.Body); err == nil {
			fmt.Printf("\n FailoverNode: body of the response with error %s", body)
		}
		return errors.New(fmt.Sprintf("Received a bad status %v", resp.Status))
	}

	return nil
}

func (node *CouchbaseNode) StartRebalance() (err error) {
	var ejectedNodes, knownNodes string

	values := url.Values{}

	for ip, _ := range node.EjectNodes {
		if len(ejectedNodes) == 0 {
			ejectedNodes = fmt.Sprintf("ns_1@%s", ip)
		} else {
			ejectedNodes = fmt.Sprintf("%s,ns_1@%s", ejectedNodes, ip)
		}
	}

	values.Set("ejectedNodes", ejectedNodes)

	for ip, _ := range node.KnownNodes {
		if len(knownNodes) == 0 {
			knownNodes = fmt.Sprintf("ns_1@%s", ip)
		} else {
			knownNodes = fmt.Sprintf("%s,ns_1@%s", knownNodes, ip)
		}
	}
	values.Set("knownNodes", knownNodes)

	api := fmt.Sprintf("%s%s", node.BaseURL, startRebalanceUri)
	fmt.Printf("\n %v", values)

	resp, err := node.HttpClient.PostForm(api, values)
	if err != nil {
		fmt.Printf("error getting response %v", err)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if body, err := ioutil.ReadAll(resp.Body); err == nil {
			fmt.Printf("\n body of the response with error %s", body)
		}
		return errors.New(fmt.Sprintf("Received a bad status %v", resp.Status))
	}
	return nil
}

func (node *CouchbaseNode) RebalanceProgress() (status string, err error) {
	api := fmt.Sprintf("%s%s", node.BaseURL, rebalanceProgressUri)
	resp, err := node.HttpClient.Get(api)

	if err != nil {
		return status, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(fmt.Sprintf("Received a bad status %v", resp.Status))
	}
	resJson := make(map[string]interface{})

	if body, err := ioutil.ReadAll(resp.Body); err == nil {
		err = json.Unmarshal(body, &resJson)
		if err != nil {
			fmt.Printf("\n error nmarshaling %v", err)
			return status, err
		}
	}

	status = fmt.Sprintf("%v", resJson["status"])
	return status, nil
}

func (node *CouchbaseNode) CreateBucket(bucketname string) (err error) {
	values := url.Values{}

	values.Set("name", bucketname)
	values.Set("ramQuotaMB", "200")
	values.Set("authType", "none")
	values.Set("replicaNumber", "1")

	api := fmt.Sprintf("%s%s", node.BaseURL, createBucketUri)
	resp, err := node.HttpClient.PostForm(api, values)

	if err != nil {
		fmt.Printf("error getting create bucket response %v", err)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if body, err := ioutil.ReadAll(resp.Body); err == nil {
			fmt.Printf("\n error reading create bucket response %s", body)
		}
		return errors.New(fmt.Sprintf("Received a bad status %v", resp.Status))
	}

	return nil
}

func (node *CouchbaseNode) ConnectToBucket() (err error) {
	c := node.CouchbaseClient
	p, err := c.GetPool(node.Bucket.Name)
	if err != nil {
		return err
	}

	b, err := p.GetBucket(node.BucketDetails.Name)
	node.Bucket = b
	return err
}

func (node *CouchbaseNode) DoOp(opName string, key string, doc interface{}) (err error) {
	var dummy interface{}
	switch {
	case opName == "GET":
		err = node.Bucket.Get(key, dummy)
	case opName == "SET":
		err = node.Bucket.Set(key, 0, doc)
	}
	return err
}

func (node *CouchbaseNode) DeleteBucket(bucketname string) (err error) {
	api := fmt.Sprintf("%s%s", node.BaseURL, createBucketUri, bucketname)

	req, err := http.NewRequest("DELETE", api, nil)
	if err != nil {
		fmt.Printf("Error forming the request %v", err)
		return err
	}

	resp, err := node.HttpClient.Do(req)

	if err != nil {
		fmt.Printf("error getting create bucket response %v", err)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if body, err := ioutil.ReadAll(resp.Body); err == nil {
			fmt.Printf("\n error reading create bucket response %s", body)
		}
		return errors.New(fmt.Sprintf("Received a bad status %v", resp.Status))
	}

	return nil
}

func (node *CouchbaseNode) StopService() (err error) {
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

	command := "/etc/init.d/couchbase-server stop"
	if err := session.Run(command); err != nil {
		fmt.Printf("Failed to run command %s", command)
		return err
	}
	return nil
}
