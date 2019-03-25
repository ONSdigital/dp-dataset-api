package query

const (
	// codelists
	GetCodeLists       = "MATCH (i) WHERE i:_code_list%s RETURN distinct labels(i) as labels"
	GetCodeList        = "MATCH (i:_code_list:`_code_list_%s`) RETURN i"
	CodeListExists     = "MATCH (cl:_code_list:`_code_list_%s`) RETURN count(*)"
	GetCodeListEdition = "MATCH (i:_code_list:`_code_list_%s` {edition:" + `"%s"` + "}) RETURN i"
	CountEditions      = "MATCH (cl:_code_list:`_code_list_%s`) WHERE cl.edition = %q RETURN count(*)"
	GetCodes           = "MATCH (c:_code) -[r:usedBy]->(cl:_code_list: `_code_list_%s`) WHERE cl.edition = %q RETURN c, r"
	GetCode            = "MATCH (c:_code) -[r:usedBy]->(cl:_code_list: `_code_list_%s`) WHERE cl.edition = %q AND c.value = %q RETURN c, r"
	GetCodeDatasets    = "MATCH (d)<-[inDataset]-(c:_code)-[r:usedBy]->(cl:_code_list:`_code_list_%s`) WHERE (cl.edition=" + `"%s"` + ") AND (c.value=" + `"%s"` + ") AND (d.is_published=true) RETURN d,r"

	// hierarchy write
	CreateHierarchyConstraint    = "CREATE CONSTRAINT ON (n:`_hierarchy_node_%s_%s`) ASSERT n.code IS UNIQUE;"
	CloneHierarchyNodes          = "MATCH (n:`_generic_hierarchy_node_%s`) WITH n MERGE (:`_hierarchy_node_%s_%s` { code:n.code,label:n.label,code_list:{code_list}, hasData:false });"
	CountHierarchyNodes          = "MATCH (n:`_hierarchy_node_%s_%s`) RETURN COUNT(n);"
	CloneHierarchyRelationships  = "MATCH (genericNode:`_generic_hierarchy_node_%s`)-[r:hasParent]->(genericParent:`_generic_hierarchy_node_%s`) WITH genericNode, genericParent MATCH (node:`_hierarchy_node_%s_%s` { code:genericNode.code }), (parent:`_hierarchy_node_%s_%s` { code:genericParent.code }) MERGE (node)-[r:hasParent]->(parent);"
	SetNumberOfChildren          = "MATCH (n:`_hierarchy_node_%s_%s`) with n SET n.numberOfChildren = size((n)<-[:hasParent]-(:`_hierarchy_node_%s_%s`))"
	SetHasData                   = "MATCH (n:`_hierarchy_node_%s_%s`), (p:`_%s_%s`) WHERE n.code = p.value SET n.hasData=true"
	MarkNodesToRemain            = "MATCH (parent:`_hierarchy_node_%s_%s`)<-[:hasParent*]-(child:`_hierarchy_node_%s_%s`) WHERE child.hasData=true set parent.remain=true set child.remain=true"
	RemoveNodesNotMarkedToRemain = "MATCH (node:`_hierarchy_node_%s_%s`) WHERE NOT EXISTS(node.remain) DETACH DELETE node"
	RemoveRemainMarker           = "MATCH (node:`_hierarchy_node_%s_%s`) REMOVE node.remain"

	// hierarchy read
	HierarchyExists     = "MATCH (i:`_hierarchy_node_%s_%s`) RETURN i LIMIT 1"
	GetHierarchyRoot    = "MATCH (i:`_hierarchy_node_%s_%s`) WHERE NOT (i)-[:hasParent]->() RETURN i LIMIT 1"
	GetHierarchyElement = "MATCH (i:`_hierarchy_node_%s_%s` {code:{code}}) RETURN i"
	GetChildren         = "MATCH (i:`_hierarchy_node_%s_%s` {code:{code}})<-[r:hasParent]-(child) RETURN child ORDER BY child.label"
	GetAncestry         = "MATCH (i:`_hierarchy_node_%s_%s` {code:{code}})-[r:hasParent *]->(parent) RETURN parent"

	// instance - import process
	CreateInstanceObservationConstraint = "CREATE CONSTRAINT ON (o:`_%s_observation`) ASSERT o.rowIndex IS UNIQUE"
	CreateInstance                      = "CREATE (i:`_%s_Instance` { header:'%s'}) RETURN i"
	CountInstance                       = "MATCH (i: `_%s_Instance`) RETURN COUNT(*)"
	AddInstanceDimensions               = "MATCH (i:`_%s_Instance`) SET i.dimensions = {dimensions_list}"
	CreateInstanceToCodeRelationship    = "MATCH (i:`_%s_Instance`), (c:_code {value:{code}})-[:usedBy]->(cl:`_code_list_%s`) CREATE (c)-[:inDataset]->(i)"
	AddVersionDetailsToInstance         = "MATCH (i:`_%s_Instance`) SET i.dataset_id = {dataset_id}, i.edition = {edition}, i.version = {version} RETURN i"
	SetInstanceIsPublished              = "MATCH (i:`_%s_Instance`) SET i.is_published = true"
	CountObservations                   = "MATCH (o:`_%s_observation`) RETURN COUNT(o)"

	// dimension
	CreateDimensionConstraint             = "CREATE CONSTRAINT ON (d:`_%s_%s`) ASSERT d.value IS UNIQUE" //dimensionLabel "_%s_%s" = i.InstanceID, d.DimensionID
	CreateDimensionToInstanceRelationship = "MATCH (i:`_%s_Instance`) CREATE (d:`_%s_%s` {value: {value}}) CREATE (i)-[:HAS_DIMENSION]->(d) RETURN ID(d)"
)
