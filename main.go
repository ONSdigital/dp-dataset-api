package main

import (
	"os"

	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/datastore"
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

	mongo := &datastore.Mongo{
		Collection: cfg.MongoConfig.Collection,
		Database:   cfg.MongoConfig.Database,
		URI:        cfg.MongoConfig.BindAddr,
	}

	if err := mongo.Init(); err != nil {
		log.ErrorC("Failed to initialise mongo", err, nil)
		os.Exit(1)
	}

	datastore := api.DataStore{Backend: mongo}

	router := mux.NewRouter()

	s := server.New(cfg.BindAddr, router)

	log.Debug("listening...", log.Data{
		"bind_address": cfg.BindAddr,
	})

	_ = api.CreateDatasetAPI(cfg.SecretKey, router, datastore)

	if err = s.ListenAndServe(); err != nil {
		log.Error(err, nil)
	}
}
