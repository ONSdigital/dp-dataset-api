package driver

const serviceName = "neo4j"
const pingStmt = "MATCH (i) RETURN i LIMIT 1"

// Healthcheck calls neo4j to check its health status.
func (n *NeoDriver) Healthcheck() (string, error) {
	conn, err := n.pool.OpenPool()
	if err != nil {
		return serviceName, err
	}
	defer conn.Close()

	rows, err := conn.QueryNeo(pingStmt, nil)
	if err != nil {
		return serviceName, err
	}
	defer rows.Close()

	return serviceName, nil
}
