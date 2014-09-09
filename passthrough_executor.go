package main

import (
    "time"
)

type PassthroughExecutor struct {
    cbnodes []CouchbaseNode
    esNodes []ESNode
}

func (ex *PassthroughExecutor) Setup(config *Config) (err error) {
    for _, node := range config.CBnodes {
        if err = node.StartService(); err != nil {
            return err;
        }
        ex.cbnodes = append(ex.cbnodes, node)
    }

    //Create
    return nil
}

func (ex *PassthroughExecutor) TearDown() (err error) {
    for _, node := range ex.cbnodes {
        if err = node.StopService(); err != nil {
            return err;
        }
    }

    return nil
}

func (ex *PassthroughExecutor) Run() (time.Duration) {
    return 0;
}
