`STORE-DATA(Document)`
====================

`Document` - A well formed XML Document with schema

Preconditions: The schema is registered with the database
Postconditions: The data is stored in the database

::
    Schema = null
    Extract Schema from Document or if explicitly mentioned in the request
    If the Schema is not registered with the database (using REGISTER-SCHEMA(Schema))
    then
        error "Unknown schema"
    else
        For element E in Document
            STORE-ELEMENT(E, SchemaIRI)
