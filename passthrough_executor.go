package main

import (
	"errors"
	"fmt"
	"log"
	"time"
)

type PassthroughExecutor struct {
	activeCBNodes      []*CouchbaseNode
	activeESNodes      []*ESNode
	replicationMapping map[string]string
	count              int
	document           string
}

func (ex *PassthroughExecutor) Setup(config *Config) (err error) {	
	for index, _ := range config.CBNodes {
		node := &config.CBNodes[index]
		fmt.Printf("\n Starting the couchbase on node %s", node.Ip)
		if err = node.Init(); err != nil {
			fmt.Printf("Error initializing couchbase node %v", err)
			return err
		}
		ex.activeCBNodes = append(ex.activeCBNodes, node)
	}

	for index, _ := range config.ESNodes {
		node := &config.ESNodes[index]
		fmt.Printf("\n Initializing elastic search on node %s", node.Ip)
		node.Init()
		ex.activeESNodes = append(ex.activeESNodes, node)
	} 

	//create bucket
	if len(ex.activeCBNodes) > 0 {
		bucketname := "NewBucket"
		node := ex.activeCBNodes[0]
		if err = node.CreateBucket(bucketname); err != nil {
			fmt.Printf("\n Error creating bucket %v \n", err)
			return err
		} else {
			fmt.Printf("\n Created bucket %s\n", bucketname)
			time.Sleep(time.Second)
			if err = node.ConnectToBucket(bucketname); err != nil {
				log.Fatalf("%v", err)
			}
		}
	} else {
		return errors.New("No couchbase nodes initialized")
	}
	time.Sleep(1 * time.Second)

	//create index
	if len(ex.activeESNodes) > 0 {
		indexname := "newindex"
		esNode := ex.activeESNodes[0]
		if err = esNode.CreateIndex(indexname); err != nil {
			fmt.Printf("\n Error creating index %v \n", err)
			return err
		} else {
			fmt.Printf("\n Created index %s\n", indexname)
		}
	} else {
		return errors.New("No elastic search node initialized")
	}

	//Add to mapping - now done here but should be done prior to this
	ex.replicationMapping = make(map[string]string)
	ex.replicationMapping["NewBucket"] = "newindex"

	ex.count = config.Replications[0].ItemCount

	return nil
}

func (ex *PassthroughExecutor) TearDown() (err error) {
	//delete buckets
	/*if len(ex.activeCBNodes) > 0 {
			bucketname := "NewBucket"
			couchbaseNode := ex.activeCBNodes[0]
			if err = couchbaseNode.DeleteBucket(bucketname); err != nil {
				fmt.Printf("\n Error deleting bucket %v", err)
	            return err
			} else {
				fmt.Printf("\n Deleted bucket %s \n", bucketname)
			}
		}*/
	if len(ex.activeESNodes) > 0 {
		indexname := "newindex"
		esNode := ex.activeESNodes[0]
		if err = esNode.DeleteIndex(indexname); err != nil {
			fmt.Printf("\n Error deleting index %v \n", err)
			return err
		} else {
			fmt.Printf("\n Deleted index %s \n", indexname)
		}
	}

	/*for _, node := range ex.activeCBNodes {
		fmt.Printf("\n Stopping the service on node %s", node.Ip)
		if err = node.StopService(); err != nil {
			fmt.Printf("Error stopping the service: %v", err)
			return err
		}
		ex.activeCBNodes = append(ex.activeCBNodes, node)
	}*/
	/*
			for _, node := range ex.activeESNodes {
				fmt.Printf("\n Stopping the elastic search service on node %s", node.Ip)
				if err = node.StopService(); err != nil {
					fmt.Printf("Error stopping the elastic search service: %v", err)
		            return err
				}
			} */
	return nil
}

func (ex *PassthroughExecutor) Run() time.Duration {
	couchbaseNode := ex.activeCBNodes[0]

	for i := 0; i < ex.count; i++ {
		if err := couchbaseNode.DoOp("SET", fmt.Sprintf("%s_%d", "key", i), nil); err != nil {
			fmt.Printf("error %v", err)
			break
		}
	}

	//Create Replication between NewBucket and TestIndex
	esNode := ex.activeESNodes[0]
	if err := couchbaseNode.CreateRemoteClusterReference(esNode); err != nil {
		fmt.Printf("Error creating remote cluster reference %v", err)
	}

	//Start Replication
	for bucket, index := range ex.replicationMapping {
		if err := couchbaseNode.CreateReplication(bucket, index); err != nil {
			fmt.Printf("Error starting the replication %v", err)
		}
	}
	time.Sleep(1 * time.Minute)

	//Verify Results
	/*if esNode.GetCount("newindex") == ex.count {
		fmt.Printf("\n Success !!\n")
	}*/

	return 0
}
