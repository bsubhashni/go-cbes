package main

import (
    "time"
)

type Executor interface {
    Setup(config *Config) error
    TearDown() error
    Run() (time.Duration)
}
