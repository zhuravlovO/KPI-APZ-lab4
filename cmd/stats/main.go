package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

func main() {
	log.Println("=== server version from LAB 5 ===")

	team := "testrnasap"
	date := time.Now().Format("2006-01-02")

	go func() {
		body := fmt.Sprintf(`{"value": "%s"}`, date)
		_, err := http.Post("http://dbserver:8083/db/"+team, "application/json", strings.NewReader(body))
		if err != nil {
			log.Printf("failed to POST to db: %v", err)
		}
	}()

	http.HandleFunc("/api/v1/some-data", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		resp, err := http.Get("http://dbserver:8083/db/" + key)
		if err != nil || resp.StatusCode != http.StatusOK {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		io.Copy(w, resp.Body)
	})
	http.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string][]string{
			"requests": {"1", "2"},
		})
	})

	log.Println("Staring the HTTP server on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
