package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
)

const (
	CouchbaseBucketSeed = "CBucket"
	MemcachedBucketSeed = "MemdBucket"
	IndexSeed           = "Index"
	KeySeed             = "SimpleKey"
)

type Replication struct {
	BucketType string `json:"bucket-type"`
	ItemCount  int    `json:"item-count"`
	ItemSize   int    `json:"item-size"`
}

type Config struct {
	Replications []Replication   `json:"replication"`
	CBnodes      []CouchbaseNode `json:"cb-nodes"`
	ESnodes      []ESNode        `json:"es-nodes"`
	SituationId  string          `json:"cluster-situation"`
	ActionId     string          `json:"data-manipulation"`
	situation    []Situation
	action       *Action
    executors    []Executor
}

type Action struct {
	Id          string `json:"id"`
	Description string `json:"description"`
}

type Situation struct {
	Id            string `json:"id"`
	Description   string `json:"description"`
	NodeCount     int    `json:"node-count"`
	ReplicaCount  int    `json:"replication-count"`
	FailoverCount int    `json:"failover-count"`
	AddCount      int    `json:"add-count"`
	RemoveCount   int    `json:"remove-count"`
}

func readSituationOptions(situationsStandard string, situations *[]Situation) (err error) {
	bytes, err := ioutil.ReadFile(situationsStandard)
	if err != nil {
		fmt.Printf("\n Error reading file %s %v", situationsStandard, err)
	}

	if err := json.Unmarshal(bytes, situations); err != nil {
		fmt.Printf("\nError unmarshaling situation options %v", err)
		return err
	} else {
		fmt.Printf("\n%v", *situations)
	}
	return nil
}

func readActionOptions(dmStandard string, actions *[]Action) (err error) {
	bytes, err := ioutil.ReadFile(dmStandard)
	if err != nil {
		fmt.Printf("\n Error reading file %s %v", dmStandard, err)
		return err
	}

	if err := json.Unmarshal(bytes, actions); err != nil {
		fmt.Printf("\nError unmarshaling data manipulation options %v", err)
		return err
	} else {
		fmt.Printf("\n%v", *actions)
	}
	return nil
}

func mapSituation(config *Config, situations *[]Situation) (err error) {
	for _, situation := range *situations {
		if strings.EqualFold(config.SituationId, situation.Id) {
			config.situation = append(config.situation,situation)
			fmt.Printf("\n \n Copying situation %v", config)
		}
	}
    if (len(config.situation) == 0) {
	    return errors.New("Cannot map data-manipulation action with the standards")
    } else {
        return nil
    }
}

func mapAction(config *Config, actions *[]Action) (err error) {
	for _, action := range *actions {
		if strings.EqualFold(config.ActionId, action.Id) {
			config.action = &action
			fmt.Printf("\n \n Copying action %v", config)
			return nil
		}
	}
	return errors.New("Cannot map data-manipulation action with the standards")
}

func LoadConfig(fileName string, dmStandard string, situationsStandard string) (config Config) {
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatalf("Error reading file %v", err)
	}

	//Read config
	if err := json.Unmarshal(bytes, &config); err != nil {
		log.Fatalf("Error unmarshaling config %v", err)
	} else {
		fmt.Printf("%v", config)
	}

	//Read Standards
	var actions []Action
	err = readActionOptions(dmStandard, &actions)
	if err != nil {
		log.Fatalf("%v", err)
	}

	var situations []Situation
	err = readSituationOptions(situationsStandard, &situations)
	if err != nil {
		log.Fatalf("%v", err)
	}

	//Map to standard
	err = mapAction(&config, &actions)

	if err != nil {
		log.Fatalf("%v", err)
	}

	err = mapSituation(&config, &situations)

	if err != nil {
		log.Fatalf("%v", err)
	}
	return config
}
