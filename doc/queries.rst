Queries
=======

Queries are performed using a subset of SPARQL_.

.. TODO:: Rationale for using SPARQL

.. _SPARQL: http://www.w3.org/TR/rdf-sparql-query/

SPARQL subset we support for BTP

1) ONLY SELECT statements
2) Support FILTER only for specifying where in the XML that concept should
   be present
3) All UNIONs, GROUPs, BASIC supported

The RESULT of a SELECT is NOT a set of triples
The intermediate result is a set of triples whose bound variables give
us what data to extract
Run that over the data set and extract the relevant XML and dump this
to the client.

Predicates
--------------
Predicates may be either rdfs:subClassOf (http://www.w3.org/2000/01/rdf-schema)
In addition we define the following relationship types:

* hasProperty (e.g. `{ mo:Track hasProperty ?name . FILTER(?name ==
  "Masti ki Basti") }` )

Finally predicates which are fully qualified rdf:Property's are
allowed and may be used for selective filtering on the client's
own datasets or for further culling
We define a filter Expression - `LOCATION(Node, "<XPath expression>")`.
The LOCATION filter is not run on the cluster space, but on the data space,
returning only XML fragments where Node (literal) lies at <XPath expression>
depth/location in the XML data

NOTE: Only a strict subset of XPath is supported, namely Path expressions
- http://www.w3.org/TR/xpath20/#id-path-expressions

