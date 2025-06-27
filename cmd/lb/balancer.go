package main

import (
	"context"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var (
	port         = flag.Int("port", 8090, "load balancer port")
	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")

	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	healthyServers []string
	mu             sync.RWMutex
)

func healthChecker(servers []string) {
	newHealthyServers := make([]string, 0)
	var wg sync.WaitGroup
	for _, server := range servers {
		wg.Add(1)
		go func(serverURL string) {
			defer wg.Done()
			if health(serverURL) {
				func() {
					mu.Lock()
					defer mu.Unlock()
					newHealthyServers = append(newHealthyServers, serverURL)
				}()
			}
		}(server)
	}
	wg.Wait()

	mu.Lock()
	healthyServers = newHealthyServers
	mu.Unlock()
}

func scheme() string {
	return "http"
}

func health(dst string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err != nil {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
	defer resp.Body.Close()

	for k, values := range resp.Header {
		for _, v := range values {
			rw.Header().Add(k, v)
		}
	}

	if *traceEnabled {
		rw.Header().Set("lb-from", dst)
	}
	log.Printf("fwd: %s %s", resp.Status, fwdRequest.URL)
	rw.WriteHeader(resp.StatusCode)
	io.Copy(rw, resp.Body)
	return nil
}

func chooseServerByPath(path string, servers []string) (string, error) {
	if len(servers) == 0 {
		return "", fmt.Errorf("no healthy servers available")
	}
	hash := crc32.ChecksumIEEE([]byte(path))
	index := int(hash % uint32(len(servers)))
	return servers[index], nil
}

func main() {
	flag.Parse()

	go func() {
		for {
			healthChecker(serversPool)
			time.Sleep(10 * time.Second)
		}
	}()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		mu.RLock()
		currentHealthyServers := healthyServers
		mu.RUnlock()

		server, err := chooseServerByPath(r.URL.Path, currentHealthyServers)
		if err != nil {
			log.Println(err)
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		log.Printf("routing request for %s to server %s", r.URL.Path, server)
		forward(server, rw, r)
	}))

	log.Printf("Starting load balancer at :%d", *port)
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
