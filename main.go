package main

import (
	"database/sql"
	"os"

	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/postgres"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

func main() {
	log.Namespace = "dp-dataset-api"

	cfg, err := config.Get()
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	db, err := sql.Open("postgres", cfg.PostgresDatasetsURL)
	if err != nil {
		log.ErrorC("DB open error", err, nil)
		os.Exit(1)
	}

	dataStore, err := postgres.NewDatastore(db)
	if err != nil {
		log.ErrorC("Create postgres error", err, nil)
		os.Exit(1)
	}

	router := mux.NewRouter()

	s := server.New(cfg.BindAddr, router)

	log.Debug("listening...", log.Data{
		"bind_address": cfg.BindAddr,
	})

	_ = api.CreateDatasetAPI(cfg.SecretKey, router, dataStore)

	if err = s.ListenAndServe(); err != nil {
		log.Error(err, nil)
	}
}
