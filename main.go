package main

import (
	"fmt"
	"strings"
	"time"
)

func mapExecutors(config *Config) {
	for _, situation := range config.situation {
		if strings.EqualFold(situation.Id, "AddRb") {
			var executor *AddRbExecutor
			config.executors = append(config.executors, executor)
		}
		if strings.EqualFold(situation.Id, "passthrough") {
			var executor *PassthroughExecutor
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
	fmt.Printf("%v", config)

	//Map executors to the config
	mapExecutors(&config)

	for description, executor := range config.executors {
		fmt.Printf("\n %v", description)
		executor.Setup(&config)
		duration := executor.Run()
		executor.TearDown()
		fmt.Printf("\n Completed in %v", duration.String)
		fmt.Printf("\n----------------------------------\n")
	}

	duration := time.Since(start)
	fmt.Printf("\n Time taken for Execution of the tests %s\n", duration.String())

}
