package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/roman-mazur/architecture-practice-4-template/datastore"
)

var db *datastore.Db

func main() {
	var err error
	db, err = datastore.Open("db-files")
	if err != nil {
		log.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	http.HandleFunc("/db/", dbHandler)

	log.Println("DB HTTP API started on :8083")
	log.Fatal(http.ListenAndServe(":8083", nil))
}

func dbHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		val, err := db.Get(key)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{
			"key":   key,
			"value": val,
		})
	case "POST":
		var body struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		err := db.Put(key, body.Value)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
