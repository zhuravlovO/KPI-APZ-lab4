package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
	port := 8080

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)
	h.Handle("/report", report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(rw, "Key parameter is missing", http.StatusBadRequest)
			return
		}

		dbUrl := fmt.Sprintf("http://dbserver:8083/db/%s", key)

		resp, err := http.Get(dbUrl)
		if err != nil {
			http.Error(rw, "Failed to connect to db server: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		rw.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		rw.WriteHeader(resp.StatusCode)
		io.Copy(rw, resp.Body)
	})

	go func() {
		time.Sleep(2 * time.Second)

		hostname := os.Getenv("SERVER_NAME")
		if hostname == "" {
			var err error
			hostname, err = os.Hostname()
			if err != nil {
				log.Printf("Error getting hostname: %s", err)
				return
			}
		}

		value := time.Now().Format("2006-01-02")
		body, _ := json.Marshal(map[string]string{"value": value})
		dbUrl := fmt.Sprintf("http://dbserver:8083/db/%s", hostname)

		resp, err := http.Post(dbUrl, "application/json", strings.NewReader(string(body)))
		if err != nil {
			log.Printf("Failed to register server %s in db: %s", hostname, err)
		} else {
			log.Printf("Server %s registered in db with value %s", hostname, value)
			resp.Body.Close()
		}
	}()

	server := httptools.CreateServer(port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

type Report map[string][]string

const reportMaxLen = 100

func (r Report) Process(req *http.Request) {
	author := req.Header.Get("lb-author")
	counter := req.Header.Get("lb-req-cnt")
	if author != "" {
		list := r[author]
		if len(list) > reportMaxLen {
			list = list[len(list)-reportMaxLen:]
		}
		r[author] = append(list, counter)
	}
}

func (r Report) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("content-type", "application/json")
	rw.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(rw).Encode(r)
}
