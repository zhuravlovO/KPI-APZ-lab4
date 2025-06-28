package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/roman-mazur/architecture-practice-4-template/datastore"
)

type DbServer struct {
	db *datastore.Db
}

func (ds *DbServer) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	value, err := ds.db.Get(key)
	if err == datastore.ErrNotFound {
		http.Error(w, "Key not found in DB", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := map[string]string{"key": key, "value": value}
	json.NewEncoder(w).Encode(resp)
}

func (ds *DbServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var payload map[string]string
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := ds.db.Put(key, payload["value"]); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (ds *DbServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	if err := ds.db.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	db, err := datastore.NewDb("./data", 10*1024*1024)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	ds := &DbServer{db: db}

	http.HandleFunc("/db/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			ds.GetHandler(w, r)
		case http.MethodPost:
			ds.PostHandler(w, r)
		case http.MethodDelete:
			ds.DeleteHandler(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Println("DB HTTP API started on :8083")
	log.Fatal(http.ListenAndServe(":8083", nil))
}
