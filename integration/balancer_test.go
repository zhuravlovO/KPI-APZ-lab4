package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

type dbResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func TestBalancerIntegration(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	time.Sleep(2 * time.Second)

	keyToTest := "testrnasap"
	expectedValue := time.Now().Format("2006-01-02")

	url := fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, keyToTest)
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("Failed to send request to %v, url, err")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 OK, but got %d", resp.StatusCode)
	}

	var dbResponse dbResponse
	if err := json.NewDecoder(resp.Body).Decode(&dbResponse); err != nil {
		t.Fatalf("Failed to decode response body: %v", err)
	}

	if dbResponse.Key != keyToTest {
		t.Errorf("Expected key '%s', but got '%s'", keyToTest, dbResponse.Key)
	}
	if dbResponse.Value != expectedValue {
		t.Errorf("Expected value '%s', but got '%s'", expectedValue, dbResponse.Value)
	}
	t.Logf("Successfully got data for key '%s', value is '%s'", dbResponse.Key, dbResponse.Value)

	resp, err = client.Get(fmt.Sprintf("%s/api/v1/some-data?key=non-existent-key", baseAddress))
	if err != nil {
		t.Fatalf("Request for non-existent key failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404 Not Found for non-existent key, but got %d", resp.StatusCode)
	}
	t.Logf("Successfully got 404 for a non-existent key")
}
