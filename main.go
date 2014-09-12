package main

import (
	"fmt"
	"strings"
	"time"
)

func mapExecutors(config *Config) {
	for _, situation := range config.situation {
		if strings.EqualFold(situation.Id, "AddRb") {
			executor := &AddRbExecutor{}
			config.executors = append(config.executors, executor)
		}
		if strings.EqualFold(situation.Id, "passthrough") {
			executor := &PassthroughExecutor{}
			config.executors = append(config.executors, executor)
		}
		//Do map more
	}
}

func main() {
	start := time.Now()
	config := LoadConfig("config.json",
		"resources/data-manipulation-options.json",
		"resources/situation-options.json")

	//Map executors to the config
	mapExecutors(&config)

	for description, executor := range config.executors {
		var duration time.Duration
		fmt.Printf("\n %v", description)
		if err := executor.Setup(&config); err == nil {
			duration = executor.Run()
		}

		executor.TearDown()
		fmt.Printf("\n Completed in %v", duration.String)
		fmt.Printf("\n----------------------------------\n")
	}

	duration := time.Since(start)
	fmt.Printf("\n Time taken for Execution of the tests %s\n", duration.String())

}
