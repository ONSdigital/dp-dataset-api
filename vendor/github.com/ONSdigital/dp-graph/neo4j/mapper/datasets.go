package mapper

import (
	"github.com/ONSdigital/golang-neo4j-bolt-driver/structures/graph"
)

// Datasets maps datasetIDs to dataset data
type Datasets map[string]DatasetData

// DatasetEditinos maps edition labels to lists of version IDs
type DatasetEditions map[string]Versions

// Versions contains a list of version IDs
type Versions []int

// DatasetData contains information specific to how this code is represented or
// present in a particular dataset
type DatasetData struct {
	DimensionLabel string
	Editions       DatasetEditions
}

// CodesDatasets returns a dpbolt.ResultMapper which converts dpbolt.Result to Datasets
func CodesDatasets(datasets Datasets) ResultMapper {
	return func(r *Result) error {
		var err error

		var node graph.Node
		if node, err = getNode(r.Data[0]); err != nil {
			return err
		}

		var relationship graph.Relationship
		if relationship, err = getRelationship(r.Data[1]); err != nil {
			return err
		}

		var datasetID string
		if datasetID, err = getStringProperty("dataset_id", node.Properties); err != nil {
			return err
		}

		var datasetEdition string
		if datasetEdition, err = getStringProperty("edition", node.Properties); err != nil {
			return err
		}

		var version int64
		if version, err = getint64Property("version", node.Properties); err != nil {
			return err
		}

		var dimensionLabel string
		if dimensionLabel, err = getStringProperty("label", relationship.Properties); err != nil {
			return err
		}

		dataset, ok := datasets[datasetID]
		if !ok {
			dataset = DatasetData{
				DimensionLabel: dimensionLabel,
				Editions:       make(DatasetEditions, 0),
			}
		}

		if dataset.Editions[datasetEdition] == nil {
			dataset.Editions[datasetEdition] = make(Versions, 0)
		}

		dataset.Editions[datasetEdition] = append(dataset.Editions[datasetEdition], int(version))

		datasets[datasetID] = dataset

		return nil
	}
}
