package main

import (
	"time"
)

type AddRbExecutor struct {
	startupNodes []CouchbaseNode
	AddNodes     []CouchbaseNode
}

func (ex *AddRbExecutor) Setup(config *Config) (err error) {
	//Start ES
	return nil
}

func (ex *AddRbExecutor) TearDown() (err error) {
	//delete bucket
	//delete index
	//Stop the nodes
	return nil

}

func (ex *AddRbExecutor) Run() time.Duration {
	return 0
}
