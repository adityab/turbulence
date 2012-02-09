Configuration
=============

Turbulence is configured via a JSON_ file. By default Turbulence will look for
a file called `config.json` in the *current working directory*. This can be
overruled by the command line option :option:`config <turbulence -c>`.

.. _JSON: http://www.json.org

The top level braces de-limit the JSON object. Each top level *key* is
a section header. For example, the HTTP port would be specified as

.. code-block:: js

    {
        "http" : {
            "port" : 8080
        },

        ...
    }

Core options
------------

core.data_dir : string
    Directory in which all data is stored by this instance of Turbulence.

core.log_level : string
    One of `debug`, `warn`, `info`.

HTTP options
------------

http.port : integer
    Port on which to listen for :doc:`REST requests <apiref>`.

