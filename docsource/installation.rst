Installation Instructions
=========================

Local Deployment
-----------------
Follow these instructions for a local deployment once the :ref:`System Requirements` have been satisfied. These instructions are known to work on Ubuntu 16.04 LTS. The rest of this playbook assumes that you have all dependency binaries in your system environment path variable. At a high level, the steps are -

.. code-block:: text

    1. Start ElasticSearch
    2. Start Kibana
    3. Configure Workspace listeners to write events to Search mongo db
    4. Restart Workspace service
    5. Create a Workspace data type
    6. Configure KBaseSearchEngine
    7. Start worker
    8. Start coordinator
    9. Verify ElasticSearch index

1. Open a new terminal and start ElasticSearch.

.. note::

    Elastic Search can only be started up by a non-root user

.. code-block:: bash

    $ elasticsearch

2. Open a new terminal and start Kibana. Then open http://localhost:5601 in a
   browser tab to view the initial state of ElasticSearch.

.. code-block:: bash

   $ kibana

3. Configure the Workspace listeners to write events to the Search mongodb.

.. code-block:: bash

    $ gedit [PATH_TO_YOUR_WORKSPACE_DIR]/deploy.cfg

Add the following lines under the listener configuration section -

.. code-block:: cfg

    listeners = Search
    listener-Search-class = us.kbase.workspace.modules.SearchPrototypeEventHandlerFactory
    listener-Search-config-mongohost = localhost
    listener-Search-config-mongodatabase = Search_test
    listener-Search-config-mongouser = ""
    listener-Search-config-mongopwd = ""

4. Restart the Workspace Service. (See section on `Deploying the Workspace Service locally <https://github.com/kbase/workspace_deluxe/blob/dev-candidate/docsource/developers.rst>`_)

5. Open a new terminal and save the following document as Empty.spec. Then load into ipython, register the spec and save an object of this type to the Workspace. Saving a new object will cause the Workspace listener to write a new event to the mongo instance. Note that the ws.administer() command below requires administration privileges on the workspace.

.. code-block:: javascript

    module Empty {

        /* @optional foo */
        typedef structure {
            int foo;
        } AType;
    };

.. code-block:: bash

    $ ipython

    In [1]: spec = open("[PATH_TO_SPEC]/Empty.spec").read()
    In [2]: ws.request_module_ownership('Empty')
    In [3]: ws.administer({'command': 'listModRequests'})
    Out[4]:
    [{u'moduleName': u'Empty', ...}]
    In [5]: ws.administer({'command': 'approveModRequest', 'module': 'Empty'})
    In [6]: ws.register_typespec({'spec': spec, 'new_types': ['AType'], 'dryrun': 0})
    Out[7]: {u'Empty.Atype-0.1': ....}
    In [8]: ws.release_module('Empty')
    Out[9]: [u'Empty.AType-1.0']
    In [10]: ws.save_objects({'id': 1, 'objects': [{'type': 'Empty.AType', 'data': {'bar': 'baz'}, 'name': 'myobj'}]})
    Out[11]:
    [[1,
    u'myobj',
    ...
    ]]


Create a new terminal and start mongo to check to make sure the event has been written. Note that the status is UNPROC (unprocessed event).

.. code-block:: bash

    $ mongo
    > show dbs
    Search_test
    admin
    local
    workspace
    ws_types
    > use Search_test
    switched to db Search_test
    > db.getCollectionNames()
    ["searchEvents"]
    > db.searchEvents.findOne()
    {
          "_id": ...,
          "strcde": "WS",
          "accgrp": 1,
          ...
          "status": "UNPROC"
    }

6. Create a new terminal and edit search_tools.cfg, create a test data type and build the executable script.

.. code-block:: bash

    $ cd [PATH_TO_YOUR_KBaseSearchEngine_DIR]
    $ git checkout master
    $ git pull
    $ cp search_tools.cfg.example search_tools.cfg
    $ gedit search_tools.cfg

Make the following edits. Note: the user for the token used below must have workspace admin privileges.

.. code-block:: cfg

    search-mongo-host=localhost
    search-mongo-db=Search_test
    elastic-host=localhost
    elastic-port=9200
    scratch=[PATH_TO_DIR_WHERE_TEMP_FILES_CAN_BE_STORED_BY_APP]
    workspace-url=http://localhost:7058
    auth-service-url=https://ci.kbase.us/services/auth/api/legacy/KBase/Sessions/Login
    indexer-token=[YOUR_CI_TOKEN]
    types-dir=[PATH_TO_YOUR_KBaseSearchEngine_DIR]/KBaseSearchEngine/test_types
    type-mappings-dir=[PATH_TO_YOUR_KBaseSearchEngine_DIR]/KBaseSearchEngine/test_type_mappings
    workspace-mongo-host=fake
    workspace-mongo-db=fake

.. code-block:: bash

    $ mkdir test_types
    $ cd test_types
    $ gedit Empty.json

.. code-block:: json

    {
        "global-object-type": "EmptyAType2",
        "ui-type-name": "A Type",
        "storage-type": "WS",
        "storage-object-type": "Empty.AType",
        "indexing-rules": [
            {
                "path": "whee",
                "keyword-type": "string"
            },
            {
                "path": "whee2",
                "keyword-type": "string"
            }
        ]
    }

.. code-block:: bash

    $ cd ..
    $ mkdir test_type_mappings
    $ make build-executable-script JARS_DIR=[ABSOLUTE_PATH_TO_KBASE_JARS_DIR] KB_RUNTIME=[PATH_TO_YOUR_ANT_INSTALL_DIR (example /usr/share)]

8. Start a worker

.. code-block:: bash

    $ bin/search_tools.sh -c search_tools.cfg -k myworker
    Press return to shut down process

9. Start the coordinator. Note that the event is processed and data has been indexed.

.. code-block:: bash

    $ bin/search_tools.sh -c search_tools.cfg -s
    Press return to shut down process
    Moved event xxx NEW_VERSION WS:1/1/1 from UNPROC to READY
    Event xxx NEW_VERSION WS:1/1/1 completed processing with state INDX on myworker

10. Open Kibana in browser with url localhost:/5601/app/kibana#/dev_tools/console?_g=()

On Kibana console, make the following query

.. code-block:: rest

    GET _search
    {
     "query": {
        "match_all": {}
     }
    }

    GET _cat/indices

    GET kbase.1.emptytype2/data/_search

The results for the query should appear on the right panel.


Production Deployment
---------------------