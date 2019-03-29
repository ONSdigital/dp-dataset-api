/*
Package mock contains functions to satisfy all interfaces defined by the graph/driver
package.

Each function can return one of 3 errors depending on how the mock is setup or a
positive response for the function - either a stubbed response or nil error.

In order to create a mock instance of graph.DB the relevant environment config
should be set, and then graph.Test() should be called specifiying which errors,
if any, should be returned by calls to the mock DB's functions.

Calls to graph.Test() will return a mock which implements all possible interfaces
defined in the graph.DB struct, rather than requested Subsets as with real implementations
*/

package mock
