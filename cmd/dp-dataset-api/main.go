package main

import (
	"database/sql"
	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/postgres"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"os"
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



	dataStore, _ := postgres.NewDatastore(db)

	router := mux.NewRouter()

	_ = api.CreateDatasetAPI("", router, dataStore)

	s := server.New(cfg.BindAddr, router)

	log.Debug("listening...", log.Data{
		"bind_address": cfg.BindAddr,
	})

	if err = s.ListenAndServe(); err != nil {
		log.Error(err, nil)
	}

}
