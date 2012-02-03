`STORE-ELEMENT(E, SchemaIRI)`
==============================

Preconditions
-------------

- The SchemaIRI is known
- The Element is an instance of a class defined in the Schema at SchemaIRI
- Element is a top-level element

::
    Follow SchemaIRI in CS to get node identifying CLASS(E)
    
    Store E in data database created for this SchemaIRI
    LINK-ELEMENT(LOC(E), CLASS(E), SchemaIRI)
    For every sub-element SE in E
        Add edge from CLASS(E) to CLASS(SE) identified by this SchemaIRI
        LINK-ELEMENT(LOC(SE), CLASS(SE), SchemaIRI)
        Recurse to all levels
