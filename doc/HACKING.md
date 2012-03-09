The starting point (void main) is com.turbulence.Turbulence.

This initializes some things (TurbulenceDriver) and then starts the HTTP
server.  The API URLs are defined in RESTv1. Each API call maps to a class in
com.turbulence.core.actions. All actions inherit from Action and are to be
created using ActionFactory. They all return Result.

Each client (HTTP API call) is run in a separate thread!

RegisterSchemaAction
--------------------

This is in charge of creating the clusterspace. The clusterspace is a neo4j
database. For every ontology registered, the action goes and fetches the
ontology. It then adds a node for the ontology so that all concepts in the
ontology have an edge to this node (simply for tracking ownership). The
clusterspace creation algorithm is best read from RegisterSchemaAction.link
code. Each concept gets link called on it.

StoreDataAction
---------------

Stores XML data based on concepts in known ontologies. Each primary XML element
is stored, followed by individual children being stored. This is stored in
Cassandra, with the Row ID being unique for every primary element and the
column names being the fully-qualified Class IRI and also a duplicate with the
column name as `<fully qualified Class IRI>|/location/in/primary/element` where
location is a XPath path expression.

QueryAction
-----------

Runs a SPARQL query over the clusterspace. Is supposed to extract data from
Cassandra too, but is not implemented fully yet.

TurbulenceDriver
----------------

sets up certain constants, and utility methods to access them.

com.turbulence.util.*
---------------------

Config - JSON config parser
OntologySaver - save an ontology OWL file locally (cached using SHA1 of IRI as
filename)
OntologyMapper - try and load an ontology from local cache
