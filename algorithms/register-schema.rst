`REGISTER-SCHEMA(ONTOLOGY, CS, GO)`
===================================

Ontology - An XML Schema or RDF/OWL document
CS - Cluster Space graph
GO - Global ontology

For XML Schema::

    TODO

For RDF/OWL::

    For each ontology that the Ontology imports
        REGISTER-SCHEMA(ontology)

    // we are not concerned about Instances
    // in CS, but the reasoner will want it so we insert it into GO
    
    For class C in Ontology
        LINK-CLUSTERSPACE(C)

    If consistency checks and link succeed
        Save the ontology modification
        Add Schema to known ontologies table with IRI as the key
        Add Schema as a SchemaNode to CS
        For all C in Ontology
            NODE(C) = Add a node for C in CS
            Store complete IRI of the class as an attribute in NODE(C)
            Add edge between SchemaNode and NODE(C)
            LINK-CLUSTERSPACE(NODE(C))
        Reply with success
    Else
        Undo all Class additions
        Reply with error


