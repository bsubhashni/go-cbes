package main

import (
	"fmt"
	"log"
	"time"
)

const (
	maxWaitTimeForReplication = 10 * time.Second
)

type AddRbExecutor struct {
	activeCBNodes      []*CouchbaseNode
	pendingCBNodes     []*CouchbaseNode
	activeESNodes      []*ESNode
	replicationMapping map[string]string
	count              int
	document           string
	eptCB              *CouchbaseNode
	eptES              *ESNode
}

func (ex *AddRbExecutor) Setup(config *Config) (err error) {
	for index, _ := range config.CBNodes {
		node := &config.CBNodes[index]
		fmt.Printf("\nStarting the couchbase service on node %s", node.Ip)
		if err = node.Init(); err != nil {
			fmt.Printf("\nError initializing couchbase node %v", err)
			return err
		}
		ex.activeCBNodes = append(ex.activeCBNodes, node)
	}
	ex.eptCB = ex.activeCBNodes[0]

	for index, _ := range config.ESNodes {
		node := &config.ESNodes[index]
		fmt.Printf("\nStarting the elastic search service on node %s", node.Ip)
		node.Init()
		ex.activeESNodes = append(ex.activeESNodes, node)
	}
	ex.eptES = ex.activeESNodes[0]

	/*
	       //Failover
	   	var fnodes []*CouchbaseNode
	   	for _, node := range ex.activeCBNodes {
	   		if node.Ip != ex.eptCB.Ip {
	   			fnodes = append(fnodes, node)
	   		}
	   	}
	   	FailoverAndRebalance(ex.eptCB, fnodes)

	   			//Remove form cluster
	   		    var nodes []*CouchbaseNode
	   			for _, node := range ex.activeCBNodes {
	   				if node.Ip != ex.eptCB.Ip {
	   					nodes = append(nodes, node)
	   				}
	   			}
	   			RemoveAndRebalance(ex.eptCB, nodes)
	*/

    
	//Add  to cluster
	var anodes []*CouchbaseNode
	for _, node := range ex.activeCBNodes {
		if node.Ip != ex.eptCB.Ip {
			anodes = append(anodes, node)
		}
	}

	AddAndRebalance(ex.eptCB, anodes)
	//RemoveAndRebalance(ex.eptCB, nodes)

	//create mapping
	ex.replicationMapping = make(map[string]string)
	for index := range config.Replications {
		bucketname := fmt.Sprintf("%s-%d", CouchbaseBucketSeed, index)
		indexname := fmt.Sprintf("%s-%d", IndexSeed, index)
		ex.replicationMapping[bucketname] = indexname
		//create bucket and index
		if err = ex.eptCB.CreateBucket(bucketname); err != nil {
			fmt.Printf("\nError creating bucket %v %s\n", err, ex.eptCB.Ip)
			return err
		} else {
			fmt.Printf("\nCreated bucket %s\n", bucketname)
			time.Sleep(time.Second)
			if err = ex.eptCB.ConnectToBucket(bucketname); err != nil {
				log.Fatalf("%v", err)
			}
		}
		if err = ex.eptES.CreateIndex(indexname); err != nil {
			fmt.Printf("\nError creating index %v\n", err)
			return err
		} else {
			fmt.Printf("\nCreated index %s\n", indexname)
		}
	}
	time.Sleep(30 * time.Second)

	return nil
}

func (ex *AddRbExecutor) TearDown() (err error) {
	for bucketname, indexname := range ex.replicationMapping {
		if err = ex.eptCB.DeleteBucket(bucketname); err != nil {
			fmt.Printf("\n Error deleting bucket %v", err)
			return err
		} else {
			fmt.Printf("\n Deleted bucket %s \n", bucketname)
		}
		if err = ex.eptES.DeleteIndex(indexname); err != nil {
			fmt.Printf("\n Error deleting index %v \n", err)
			return err
		} else {
			fmt.Printf("\n Deleted index %s \n", indexname)
		}
	}

	//Failover
	var fnodes []*CouchbaseNode
	for _, node := range ex.activeCBNodes {
		if node.Ip != ex.eptCB.Ip {
			fnodes = append(fnodes, node)
		}
	}
	FailoverAndRebalance(ex.eptCB, fnodes)
	var nodes []*CouchbaseNode

	for _, node := range ex.activeCBNodes {
		if ex.eptCB.Ip != node.Ip {
			nodes = append(nodes, node)
		}
	}
	RemoveAndRebalance(ex.eptCB, nodes)

	for _, node := range ex.activeCBNodes {
		if err = node.StopService(); err != nil {
			log.Fatalf("\nUnable to stop couchbase service on node %s", node.Ip)
		} else {
			fmt.Printf("\nStopping Service on node %s", node.Ip)
		}
	}

	return nil
}

func (ex *AddRbExecutor) doOps(stopOpsChan <-chan bool, opCountChan chan<- int) {
	couchbaseNode := ex.activeCBNodes[0]
	count := 0
	for {
		select {
		case <-stopOpsChan:
			time.Sleep(1 * time.Second)
			opCountChan <- count
			return
		default:
			if err := couchbaseNode.DoOp("SET", fmt.Sprintf("%s_%d", "key", count), nil); err != nil {
				log.Fatalf("unable do the op %v", err)
			} else {
				count++
			}
		}
	}
}

func (ex *AddRbExecutor) doSituation(stopOp chan<- bool) {
	AddAndRebalance(ex.eptCB, ex.pendingCBNodes)
	time.Sleep(1 * time.Minute)
	stopOp <- true
}

func (ex *AddRbExecutor) Run() time.Duration {
	//Create Replication between NewBucket and TestIndex
	esNode := ex.activeESNodes[0]
	couchbaseNode := ex.activeCBNodes[0]
	if err := couchbaseNode.CreateRemoteClusterReference(esNode); err != nil {
		fmt.Printf("Error creating remote cluster reference %v", err)
	}

	//Start Replication
	for bucket, index := range ex.replicationMapping {
		if err := couchbaseNode.CreateReplication(bucket, index); err != nil {
			fmt.Printf("Error starting the replication %v", err)
		}
	}

	opCountChan := make(chan int, 1)
	stopOpsChan := make(chan bool, 1)

	go ex.doOps(stopOpsChan, opCountChan)
	go ex.doSituation(stopOpsChan)

	opCount := <-opCountChan
	startTime := time.Now()

check:
	//verify the number of docs on the es index
	replicatedCount, err := esNode.GetCount("index-0")
	if err != nil {
		fmt.Printf("\nError getting count %v", err)
		goto done
	}
	if replicatedCount < opCount {
		wait := time.Since(startTime)
		if wait < maxWaitTimeForReplication {
			time.Sleep(10 * time.Millisecond)
			goto check
		}
	}

done:
	fmt.Printf("\n Op Count %d replicated Count %d \n", opCount, replicatedCount)
	if opCount == replicatedCount {
		fmt.Printf("Passed addrb test!!!")
	}
	fmt.Printf("%d %v", opCount, startTime)
	return 0
}
