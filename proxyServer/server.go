package main

import (
	//	"bytes"
	//	"encoding/json"
	//	"net"
	"io/ioutil"
	"net/http"
	//	"os"
	"fmt"
	"net/url"
	"strings"
)

const (
	pidFile        = "/var/run/esproxyserver.pid"
	esNodeIP       = "127.0.0.1"
	esNodeCAPIPort = "9091"
	testBucket     = "test"
)

type ProxyServer struct {
	Port               int
	HttpTimeout        int
	CheckpointInterval int
	RetryInterval      int
	Client             *http.Client
}

type Checkpoint struct {
	seqno        int `json:"seqno"`
	failoverID   int `json:"failoverID"`
	commitopaque int `json:"commitopaque"`
	vbopaque     int `json:"vbopaque"`
	bucketUUID   int `json:"bucketUUId"`
}

func (server *ProxyServer) Start() {
	/*	pid := os.Getpid()
		    fmt.Printf("pid of the process %d", pid)

			fp, err := os.Open(pidFile)
			if err != nil {
		        defer fp.Close()
				/*if pidFile, err = ioutil.WriteFile("test"); err != nil {
					//logger.Printf(logger.ERR, "unable to write pid to file")
				}*/
	/*	}
	 */
	http.HandleFunc("/", PoolsHandler)
	http.HandleFunc("/pools", PoolsHandler)
	http.HandleFunc("/_pre_replicate", PreReplicateHttpHandler)
	http.HandleFunc("/_commit_for_checkpoint", CommitForCheckPointHttpHandler)
	http.HandleFunc("/_ensure_full_commit", EnsureFullCommitHandler)
	fmt.Printf("Starting server")
	http.ListenAndServe(":3912", nil)
}

func RootHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Printf("\n Got request for %v \n", req)
	fmt.Printf("\n req url %s", req.URL)
	w.WriteHeader(http.StatusOK)
}

func PoolsHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Printf("\n Root Got request for %v \n", req)
	fmt.Printf("\n Referrer url %s\n", req.Referer())
	client := &http.Client{}
	var path string

	path = fmt.Sprintf("http://localhost:9091%s", req.URL)
	newReq, err := http.NewRequest((*req).Method, path, nil)

	if strings.Contains(path, ";") || strings.Contains(path, "_bulk_doc") {

		path = req.URL.String()
		path = url.QueryEscape(path[1 : len(path)-1])

		if strings.Contains(path, "_bulk_doc") {
			path = strings.Replace(path, "%2F_bulk_doc", "/_bulk_docs", -1)
//			newReq.ContentLength = req.ContentLength
//			newReq.PostForm = req.PostForm
		}

		urlSt := &url.URL{
			Scheme: "http",
			Opaque: fmt.Sprintf("//localhost:9091/%s", path),
			Host:   "localhost:9091",
		}

		newReq.URL = urlSt
		fmt.Printf("%s", newReq.URL.String())
	}

	if err != nil {
		fmt.Printf("Error creating new request %v", err)
		return
	}

	newReq.SetBasicAuth("root", "password")

	resp, err := client.Do(newReq)

	if err != nil {
		fmt.Printf("%v", err)
	}
	if resp.Body != nil {
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Printf("\n %s \n", body)
		newBody := fmt.Sprintf("%s", body)
		newBody = strings.Replace(newBody, "9091", "3912", -1)
		fmt.Printf("%s", newBody)
		w.Write([]byte(newBody))

	}
	w.WriteHeader(http.StatusOK)
	return
}

func PreReplicateHttpHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Printf("\n pre replicate Got request for %v \n", req)
	client := &http.Client{}
	urlStr := fmt.Sprintf("http://localhost:9091/_pre_replicate%s", req.URL)
	newReq, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		fmt.Printf("Error creating new request")
		return
	}

	newReq.SetBasicAuth("root", "password")

	resp, err := client.Do(newReq)
	defer resp.Body.Close()

	if err != nil {
		fmt.Printf("%v", err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Printf("%s", body)
	//dumb forward to capi server
	/*resp, err := client.Do(req)
	if err != nil {
	//	logger.Printf(logger.ERR, "ES node failed to reply")
	} else {
	//	logger.Printf(logger.INFO, "Got response header as %v", resp.Status)
		(*w.WriteHeader(resp.Status)
		(*w.Write(resp.Body)
	}*/

	w.WriteHeader(http.StatusOK)
	w.Write(body)
	return
}

func CommitForCheckPointHttpHandler(w http.ResponseWriter, req *http.Request) {
	//	logger.Printf(logger.INFO, "Handling checkpointing call")

	w.WriteHeader(http.StatusNotFound)
	return
}

func BulkDocsHandler(w http.ResponseWriter, req *http.Request) {
	//	logger.Printf(logger.INFO, "Got bulk docs call")

	/*
	   client = &http.Client {
	       httpTimeout : httpTimeout
	   }

	   resp, err := client.Get(fmt.Sprintf("%s"))

	   if err != nil {
	       logger.Printf(logger.ERR, "Got error %v", err)
	   } else {
	       if resp.Status
	   }
	*/
	w.WriteHeader(http.StatusOK)
	return
}

func EnsureFullCommitHandler(w http.ResponseWriter, req *http.Request) {
	//    logger.Printf(logger.INFO, "Got full commit call")
	w.WriteHeader(http.StatusNotFound)
	return
}
