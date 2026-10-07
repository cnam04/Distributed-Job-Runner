package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	// var err error
	// database, err = openDatabase(defaultDatabasePath)
	// if err != nil {
	// 	log.Fatalf("Database initialization failed: %v", err)
	// }
	// defer database.Close()

	// 1. Create a new ServeMux (Multiplexer)
	mux := http.NewServeMux()

	// 2. Register routes using Go 1.22+ method matching and path wildcards
	// example handlers:
	mux.HandleFunc("GET /example", exampleHandler)
	// 3. Configure the HTTP Server for production readiness
	server := &http.Server{
		Addr:         ":3000",
		Handler:      loggingMiddleware(mux), // Wrap multiplexer with middleware
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	fmt.Println("Server running on http://localhost:3000")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

// log http requests
// example middleware
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

type example struct {
	Field1 string `json:"field1"`
	Field2 int    `json:"field2"`
}

func newExample(word string, num int) (example, error) {
	if num == 3 {
		return example{}, fmt.Errorf("Can't be 3")
	}
	return example{
		word,
		num,
	}, nil
}

func (e *example) printExample() {
	log.Printf("%s, %d", e.Field1, e.Field2)
}

// example handler func
func exampleHandler(w http.ResponseWriter, r *http.Request) {
	ex, err := newExample(
		"meow",
		4,
	)
	if err != nil {
		respondWithError(w, http.StatusBadRequest, err.Error())
	}

	ex.printExample()

	res, err := json.Marshal(ex)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
	}

	w.Write(res)
}

func respondWithError(w http.ResponseWriter, statusCode int, message string) {
	w.WriteHeader(statusCode)
	_, err := w.Write([]byte(message))
	if err != nil {
		panic(err)
	}
}
