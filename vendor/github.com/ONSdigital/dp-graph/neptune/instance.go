package neptune

import (
	"context"
	"fmt"
	"strings"

	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-graph/neptune/query"
	"github.com/ONSdigital/go-ns/log"
	gremgo "github.com/ONSdigital/gremgo-neptune"
	"github.com/pkg/errors"
)

const codeListNotFoundFmt = "VertexStep(OUT,[usedBy],vertex), HasStep([~label.eq(_code_list_%s)"

// CountInsertedObservations returns the current number of observations relating to a given instance
func (n *NeptuneDB) CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error) {
	return n.getNumber(fmt.Sprintf(query.CountObservations, instanceID))
}

// AddVersionDetailsToInstance updates an instance node to contain details of which
// dataset, edition and version the instance will also be known by
func (n *NeptuneDB) AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
	data := log.Data{
		"instance_id": instanceID,
		"dataset_id":  datasetID,
		"edition":     edition,
		"version":     version,
	}

	q := fmt.Sprintf(query.AddVersionDetailsToInstance, instanceID, datasetID, edition, version)

	if _, err := n.exec(q); err != nil {
		log.ErrorC("neptune exec failed on AddVersionDetailsToInstance", err, data)
		return err
	}
	return nil
}

// SetInstanceIsPublished sets a flag on an instance node to indicate the published state
func (n *NeptuneDB) SetInstanceIsPublished(ctx context.Context, instanceID string) error {
	data := log.Data{
		"instance_id": instanceID,
	}

	q := fmt.Sprintf(query.SetInstanceIsPublished, instanceID)

	if _, err := n.exec(q); err != nil {
		log.ErrorC("neptune exec failed on SetInstanceIsPublished", err, data)
		return err
	}
	return nil
}

// CreateInstanceConstraint is not needed for the neptune implementation, as constraints are
// not a neptune construct
func (n *NeptuneDB) CreateInstanceConstraint(ctx context.Context, i *model.Instance) error {
	return errors.New("method not supported: CreateInstanceConstraint")
}

// CreateInstance will check if an instance node already exists and create one from
// the provided details if one does not exist
func (n *NeptuneDB) CreateInstance(ctx context.Context, i *model.Instance) error {
	if err := i.Validate(); err != nil {
		return err
	}

	data := log.Data{
		"instance_id": i.InstanceID,
	}

	exists, err := n.InstanceExists(ctx, i)
	if err != nil {
		return err
	}

	if exists {
		log.Info("instance already exists in neptune", data)
		return nil
	}

	create := fmt.Sprintf(query.CreateInstance, i.InstanceID, strings.Join(i.CSVHeader, ","))
	if _, err := n.exec(create); err != nil {
		log.ErrorC("neptune exec failed on CreateInstance", err, data)
		return err
	}
	return nil
}

// AddDimensions list to the specified instance node
func (n *NeptuneDB) AddDimensions(ctx context.Context, i *model.Instance) error {
	if err := i.Validate(); err != nil {
		return err
	}

	data := log.Data{
		"instance_id": i.InstanceID,
	}

	q := fmt.Sprintf(query.AddInstanceDimensionsPart, i.InstanceID)
	for _, d := range i.Dimensions {
		q += fmt.Sprintf(query.AddInstanceDimensionsPropertyPart, d.(string))
	}

	if _, err := n.exec(q); err != nil {
		log.ErrorC("neptune exec failed on AddDimensions", err, data)
		return err
	}

	return nil
}

// CreateCodeRelationship links an instance to a code for the given dimension option
func (n *NeptuneDB) CreateCodeRelationship(ctx context.Context, i *model.Instance, codeListID, code string) error {
	if err := i.Validate(); err != nil {
		return err
	}

	if len(code) == 0 {
		return errors.New("error creating relationship from instance to code: code is required but was empty")
	}

	data := log.Data{
		"instance_id": i.InstanceID,
		"code_list":   codeListID,
		"code":        code,
	}

	createRelationships := fmt.Sprintf(query.CreateInstanceToCodeRelationship, i.InstanceID, code, codeListID)
	if res, err := n.exec(createRelationships); err != nil {
		if len(res) > 0 && res[0].Status.Code == gremgo.StatusScriptEvaluationError &&
			strings.Contains(res[0].Status.Message, fmt.Sprintf(codeListNotFoundFmt, codeListID)) {

			return errors.Wrapf(err, "error creating relationship from instance to code: code or code list not found", data)
		}
		log.ErrorC("neptune exec failed on CreateCodeRelationship", err, data)
		return err
	}

	return nil
}

// InstanceExists returns true if an instance already exists with the provided id
func (n *NeptuneDB) InstanceExists(ctx context.Context, i *model.Instance) (bool, error) {
	data := log.Data{
		"instance_id": i.InstanceID,
	}

	exists := fmt.Sprintf(query.CheckInstance, i.InstanceID)
	count, err := n.getNumber(exists)
	if err != nil {
		log.ErrorC("neptune getNumber failed to check if instance exists", err, data)
		return false, err
	}

	if count == 0 {
		return false, nil
	}

	return true, nil
}
