package main

import (
    "fmt"
    //"logger"
    "flag"
)


func main() {
    port := flag.Int("proxy-port", 3612, "Port to start the proxy server on")
    flag.Parse()

    server := &ProxyServer {
        Port: *port,
    }
    
    //logger.Printf(logger.INFO, "Starting up the server")
    fmt.Printf("Starting up the server")
    server.Start() 
}
