API Reference
=============

All interactions with the Turbulence database are done via a REST API. The REST
API operates over HTTP.

Parameters are always sent as a GET request. The special `body` parameter
indicates a POST body (which should obviously be sent as a POST request). All
parameters as **required** unless stated otherwise.

Responses are delivered as XML documents. On success an HTTP 200 OK response is
sent with any extra information specific to the API call. HTTP error codes vary
per API call, depending on what kind of error it was. The response body
contains more information about the `Error codes`_.

API Endpoint
------------

.. |endpoint| replace:: http://domain:port/api/1

The REST API can be accessed at

    |endpoint|/<action>?<params>

where port is defined by the :ref:`http.port`.

Actions
-------

Register schema
^^^^^^^^^^^^^^^

Registers a new schema (XML Schema or OWL/RDFS Ontology file) with Turbulence.
The schema can be submitted in the POST body as an URL. Although this is not
absolutely necessary to be called before `Post data`_, it is RECOMMENDED to
speed up `Post data`_.

    |endpoint|/**register_schema**

Options
~~~~~~~

url : string
    URL to fetch the schema from.

Response
~~~~~~~~

.. code-block:: xml

    <response>
        <success/>
    </response>

or

.. code-block:: xml

    <response>
        <error code="error code">message</error>
    </response>

Possible errors
"""""""""""""""

Post data
^^^^^^^^^

Every fragment sent from a remote endpoint should contain the schema URL that
the data fragment conforms to. Alternatively the schema URL may be sent as
a parameter.

    |endpoint|/**store_data**

Options
~~~~~~~

body : data
    XML document

url : string (optional)
    The URL specifying the schema for the document. This *always* over-rides the
    schema defined in the document itself, if any.

Response
~~~~~~~~

On success:

.. code-block:: xml

    <response>
        <success/>
    </response>

Query
^^^^^

Run a query against the data set in the database. Returns XML data which
matches the `query <queries>`_

    |endpoint|/**query**

**METHOD**: POST

The POST body is the SPARQL query.

Response
~~~~~~~~

On success:

.. code-block:: xml

    <response>
        <data>
            <xmlFragment1>
                ...
            </xmlFragment1>
            <xmlFragment2>
                ...
            </xmlFragment2>
            <xmlFragment3>
                ...
            </xmlFragment3>
            ...
        </data>
    </response>

On error:

.. code-block:: xml

    <response>
        <error>message</error>
    </response>

Error codes
-----------

The number is the number in the :token:`code` attribute of an :token:`error`
tag. The message is the tag contents.

1. Invalid Schema :
    The schema was invalid

2. Schema retrieval failure :
    The schema could not be fetched from its destination
