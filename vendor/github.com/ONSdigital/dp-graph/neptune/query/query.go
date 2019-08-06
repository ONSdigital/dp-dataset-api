package query

const (
	// codelists
	GetCodeLists          = "g.V().hasLabel('_code_list')"
	GetCodeListsFiltered  = "g.V().hasLabel('_code_list').has('%s', true)"
	GetCodeList           = "g.V().hasLabel('_code_list').has('listID', '%s')"
	CodeListExists        = "g.V().hasLabel('_code_list').has('listID', '%s').count()"
	CodeListEditionExists = "g.V().hasLabel('_code_list').has('listID', '%s').has('edition', '%s').count()"
	GetCodes              = "g.V().hasLabel('_code_list')" +
		".has('listID', '%s').has('edition', '%s')" +
		".in('usedBy').hasLabel('_code')"
	CodeExists = "g.V().hasLabel('_code_list')" +
		".has('listID', '%s').has('edition', '%s')" +
		".in('usedBy').has('value', '%s').count()"

	/*
		This query harvests data from both edges and nodes, so we collapse
		the response to contain only strings - to make it parse-able with
		the graphson string-list method.

		%s Parameters: codeListID, codeListEdition, codeValue

		Naming:

			r: usedBy relation
			rl: usedBy.label
			c: code node
			d: dataset
			de: dataset.edition
			dv: dataset.version
	*/
	GetCodeDatasets = `g.V().hasLabel('_code_list').has('listID', '%s').
		has('edition','%s').
		inE('usedBy').as('r').values('label').as('rl').select('r').
		match(
			__.as('r').outV().has('value','%s').as('c'),
			__.as('c').out('inDataset').as('d').
				select('d').values('edition').as('de').
				select('d').values('version').as('dv'),
				select('d').values('dataset_id').as('did').
			__.as('d').has('is_published',true)).
		union(select('rl', 'de', 'dv', 'did')).unfold().select(values)
	`

	// hierarchy write
	CloneHierarchyNodes = "g.V().hasLabel('_generic_hierarchy_node_%s').as('old')" +
		".addV('_hierarchy_node_%s_%s')" +
		".property('code',select('old').values('code'))" +
		".property('label',select('old').values('label'))" +
		".property(single, 'hasData', false)" +
		".property('code_list','%s').as('new')" +
		".addE('clone_of').to('old').select('new')"
	CountHierarchyNodes         = "g.V().hasLabel('_hierarchy_node_%s_%s').count()"
	CloneHierarchyRelationships = "g.V().hasLabel('_generic_hierarchy_node_%s').as('oc')" +
		".out('hasParent')" +
		".in('clone_of').hasLabel('_hierarchy_node_%s_%s')" +
		".addE('hasParent').from(select('oc').in('clone_of').hasLabel('_hierarchy_node_%s_%s'))"
	RemoveCloneMarkers  = "g.V().hasLabel('_hierarchy_node_%s_%s').outE('clone_of').drop()"
	SetNumberOfChildren = "g.V().hasLabel('_hierarchy_node_%s_%s').property(single,'numberOfChildren',__.in('hasParent').count())"
	SetHasData          = "g.V().hasLabel('_hierarchy_node_%s_%s').as('v')" +
		`.V().hasLabel('_%s_%s').as('c').where('v',eq('c')).by('code').by('value').` +
		`select('v').property('hasData',true)`
	MarkNodesToRemain = "g.V().hasLabel('_hierarchy_node_%s_%s').has('hasData').property('remain',true)" +
		".repeat(out('hasParent')).emit().property('remain',true)"
	RemoveNodesNotMarkedToRemain = "g.V().hasLabel('_hierarchy_node_%s_%s').not(has('remain',true)).drop()"
	RemoveRemainMarker           = "g.V().hasLabel('_hierarchy_node_%s_%s').has('remain').properties('remain').drop()"

	// hierarchy read
	HierarchyExists     = "g.V().hasLabel('_hierarchy_node_%s_%s').limit(1)"
	GetHierarchyRoot    = "g.V().hasLabel('_hierarchy_node_%s_%s').not(outE('hasParent'))"
	GetHierarchyElement = "g.V().hasLabel('_hierarchy_node_%s_%s').has('code','%s')"
	GetChildren         = "g.V().hasLabel('_hierarchy_node_%s_%s').has('code','%s').in('hasParent').order().by('label')"
	// Note this query is recursive
	GetAncestry = "g.V().hasLabel('_hierarchy_node_%s_%s').has('code', '%s').repeat(out('hasParent')).emit()"

	// instance - import process
	CreateInstance                   = "g.addV('_%s_Instance').property(single,'header','%s')"
	CheckInstance                    = "g.V().hasLabel('_%s_Instance').count()"
	CreateInstanceToCodeRelationship = "g.V().hasLabel('_%s_Instance').as('i').addE('inDataset').from(" +
		"V().hasLabel('_code').has('value','%s').where(out('usedBy').hasLabel('_code_list').has('listID','%s'))" +
		")"
	AddVersionDetailsToInstance = "g.V().hasLabel('_%s_Instance').property(single,'dataset_id','%s')." +
		"property(single,'edition','%s').property(single,'version','%s')"
	SetInstanceIsPublished = "g.V().hasLabel('_%s_Instance').property(single,'is_published',true)"
	CountObservations      = "g.V().hasLabel('_%s_observation').count()"

	//instance - parts
	AddInstanceDimensionsPart         = "g.V().hasLabel('_%s_Instance')"
	AddInstanceDimensionsPropertyPart = ".property(list, 'dimensions', '%s')"

	// dimension
	CreateDimensionToInstanceRelationship = "g.V().hasLabel('_%s_%s').has('value', '%s').fold().coalesce(unfold(), " +
		"addV('_%s_%s').as('d').property('value','%s').addE('HAS_DIMENSION').from(V().hasLabel('_%s_Instance')).select('d'))"

	// observation
	DropObservationRelationships   = "g.V().hasLabel('_%s_observation').has('value', '%s').bothE().drop().iterate()"
	DropObservation                = "g.V().hasLabel('_%s_observation').has('value', '%s').drop().iterate()"
	CreateObservationPart          = "g.addV('_%s_observation').property(single, 'value', '%s').property(single, 'rowIndex', '%d')"
	AddObservationRelationshipPart = ".addE('isValueOf').to(V().hasId('%s').hasLabel('_%s_%s').where(values('value').is('%s'))).outV()"

	GetInstanceHeaderPart  = "g.V().hasLabel('_%s_Instance').as('instance')"
	GetAllObservationsPart = ".V().hasLabel('_%s_observation').values('row')"

	GetObservationsPart         = ".V().hasLabel('_%s_observation').match("
	GetObservationDimensionPart = "__.as('row').out('isValueOf').hasLabel('_%s_%s').where(values('value').is(within(%s)))"
	GetObservationSelectRowPart = ".select('instance', 'row').by('header').by('row').unfold().dedup().select(values)"
	LimitPart                   = ".limit(%d)"
)
