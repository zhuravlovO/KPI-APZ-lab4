package integration

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}
	numRequests := 10
	testPath := "/api/v1/some-data"
	var firstServer string
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(numRequests)
	for i := 0; i < numRequests; i++ {
		go func() {
			defer wg.Done()
			url := fmt.Sprintf("%s%s?req_id=%d", baseAddress, testPath, i)
			resp, err := client.Get(url)
			if err != nil {
				t.Errorf("Failed to get response: %v", err)
				return
			}
			defer resp.Body.Close()

			serverFromHeader := resp.Header.Get("lb-from")
			if serverFromHeader == "" {
				t.Error("lb-from header is missing or empty")
				return
			}
			t.Logf("Request routed to: %s", serverFromHeader)

			mu.Lock()
			if firstServer == "" {
				firstServer = serverFromHeader
			}
			mu.Unlock()
			if serverFromHeader != firstServer {
				t.Errorf("Mismatched servers! Expected %s but got %s", firstServer, serverFromHeader)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration test is not enabled")
	}
	for i := 0; i < b.N; i++ {
		url := fmt.Sprintf("%s/api/v1/some-data?bench_id=%d", baseAddress, i)
		resp, err := client.Get(url)
		if err != nil {
			b.Errorf("Request failed: %v", err)
			continue
		}
		resp.Body.Close()
	}
}
