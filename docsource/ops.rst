Operations
==============

[TODO: A future PR will implement a CLI for trickle updates from a user specified time]

[TODO: A future PR will include a CLI to automate step 5b by creating an index based on a search type. This would reduce potential errors. This command will also take care of step 11.]

[TODO: Add CLI command for step 13 & 14 for generating events and running trickle updates once the CLI has been implemented]

Reindexing Naming Conventions
------------------------------

For each type of index, "genome" for example, we may have multiple indices in ElasticSearch with the naming convention "kbase.genome_1", "kbase.genome_2" etc. where the namespace "kbase" comes from the "elastic-namespace" key-value pair in search_tools.cfg and each numeric suffix comes from the search type version (See resources/typemappings/GenomeAndAssembly.yaml.example).

In order to make it easy for the client to search across all indices of a certain type, aliases are applied with the following naming convention.

.. code-block:: text

 alias -> [indices]
 genome -> [genome_1, genome_2 ... genome_n]

When reindexing is necessary, the index is reindexed to a new index with a new search type version. The first reindexing operation performed on genome_1 will result in a new index genome_2 that replaces genome_1 in the genome alias.


Reindexing from source
-----------------------
1. Note current time.

2. Create a new set of indexing rules for the 'DATA_TYPE' in the resources/types/DATA_TYPE.yaml and a corresponding mapping for the new rules in resources/typeMappings/DATA_TYPE.yaml

3. Stop all workers with ctrl+c or kill command.

4. Generate events for the DATA_TYPE

.. code-block:: bash

    $ bin/search_tools.sh -c search_tools.cfg -t DATA_TYPE

5. When new index has been created, test new index by running a few queries.

.. code-block:: bash

    GET kbase.genome_2/_search

   OR

    GET kbase.genome_2/_search
    {
     "query": {
       "match": {
         "FIELD": "VALUE"
       }
     }
    }

   OR

    https://www.elastic.co/guide/en/elasticsearch/reference/5.5/search-request-body.html

6. If the new index looks good, update index alias and delete current index.

.. note::

    If you want the current index to linger for a day or two to serve a rollback option, reindex the current index into another new index called kbase.genome_1_backup and then delete the current index. This is one of two ways of renaming an index in ElasticSearch. The other way is to use the snapshot API.

.. code-block:: bash

    POST _aliases
    {
     "actions": [
     {
       "add": {
         "index": "kbase.genome_2",
         "alias": "kbase.genome"
         }
       },
       {
         "remove": {
         "index": "kbase.genome_1",
         "alias": "kbase.genome"
       }
     }
     ]
    }

    DELETE kbase.genome_1

7. List all available indexes for the genome alias and all available genome indexes to ensure consistency across the alias map. Verify that all genome indexes that are present (except for backups) are referenced by the alias. Also verify that the alias does not contain an index reference for which no index exists.

.. code-block:: bash

    GET /_cat/aliases/kbase.genome

    GET /_cat/indices/kbase.genome_*

8. Restart workers

.. code-block:: bash

    $ bin/search_tools.sh -c search_tools.cfg -k myworker

9. Restart trickle updates from the current time noted in step 1.


Reindexing from an existing index
----------------------------------
For the sake of simplicity and for the reasons described below, a single process has been defined for all reindexing cases -

a) change field value
b) add field type
c) change field type
d) remove field type

The process involves reindexing an existing index into a new index for all of these cases. i.e. in-place reindexing is discouraged because the system may not have a recent snapshot/backup or any snapshot for recovery purposes should anything go wrong with the reindexing process. It is recommended that all of these steps are performed atomically for one index at a time (avoid reindexing multiple indexes in parallel) since it will be difficult to track through hundreds to indexes to find out which indexes are being reindexed.

In addition, given that there can be as many as a thousand indices for KBase data, maintenance can become a challenge if the process is not simple. Some level of simplicity has been achieved here by defining a single process that covers all the reindexing cases. If necessary, the process may be further simplified through some level of automation as it matures over time.

.. note::

    The commands below can be copy-pasted into Kibana and executed against the index. The corresponding curl commands can be obtained from Kibana by clicking on the little wrench icon that appears next to the pasted command.

.. note::

    If any of the steps below fail, don't proceed until the issue is resolved by referring to the ElasticSearch documentation.

1. Note current time

2. Stop any workers that are performing trickle updates on the index that needs to be reindexed. Note that this will stop the trickle updates to all indexes.

3. Refresh the index that needs reindexing to make sure it has been brought to a consistent state.

.. code-block:: bash

    POST /kbase.genome_1/_refresh

4. Get a checksum for the index and record it in a separate file for later verification.

.. code-block:: bash

    GET /kbase.genome_1/_stats/docs,store

5a. If mapping needs to be changed (for cases b, c, d above), get current index mapping.

.. code-block:: bash

    GET kbase.genome_1/_mapping

5b. Copy-paste the mapping from the current index into the body section of the PUT command below and make the necessary field change (preferably one change per complete reindexing operation).

It is a good practice to make the mapping strict ("dynamic": "strict") for each type (data and access) in the index. Strict mappings prevent the mapping from being modified dynamically during ingest time.

Update the settings section below the mapping. The number of shards and replicas must be decided on based on your capacity planning rules. It is costly to change the number of shards, as this will require another reindexing operation. In general, follow these rules and limits.

increase write speed => more shards

increase read speed (availability) => more replicas

max shards per node = 600

max shard size = ~50GB

.. code-block:: bash

    PUT kbase.genome_2
    {
      "mappings": {
        "data": {
          "dynamic": "strict",
          "_parent": {
            "type": "access"
          },
          "_routing": {
            "required": true
          },
          "properties": {
            "accgrp": {
              "type": "integer"
            },
            . . .
          }
        },
        "access": {
          "dynamic": "strict",
          "properties": {
            "extpub": {
            "type": "integer"
            },
            . . .
          }
        }
      },
      "settings": {
        "index": {
          "number_of_shards": "5",
          "number_of_replicas": "1"
        }
      }
    }

5c. If the mapping does not require any change but the documents' field values (not including meta-data fields whose key names start with underscore) need to be changed, use the `Painless <https://www.elastic.co/guide/en/elasticsearch/reference/5.4/modules-scripting-painless-syntax.html>`_ script to modify metadata. Setting version_type to external will cause Elasticsearch to preserve the version from the source index, create any documents that are missing, and update any documents that have an older version in the destination index than they do in the source index.

.. code-block:: bash

    POST _reindex
    {
      "source": {
        "index": "kbase.genome_1"
      },
      "dest": {
        "index": "kbase.genome_2",
        "version_type": "external"
      },
      "script": {
        "lang": "painless",
        "inline": "if (ctx._source.foo == 'bar') {ctx._version++; ctx._source.remove('foo')}"
      }
    }

6. Now, reindex the entire data from current index to new index. Alternately, use a query to reindex only a subset of the current index.

.. code-block:: bash

    POST _reindex
    {
      "source": {
        "index": "kbase.genome_1"
      },
      "dest": {
        "index": "kbase.genome_2"
      }
    }

        OR

    POST _reindex
    {
      "source": {
        "index": "kbase.genome_1",
        "query": {
          ...
        }
      },
      "dest": {
        "index": "kbase.genome_2"
      }
    }

7. Run a checksum on the new index to make sure the numbers line up with the numbers of the current index.

.. code-block:: bash

    GET /kbase.genome_2/_stats/docs,store

8. Run a query to specifically check the change that was applied.

.. code-block:: bash

    GET kbase.genome_2/_search

   OR

    GET kbase.genome_2/_search
    {
     "query": {
       "match": {
         "FIELD": "VALUE"
       }
     }
    }

   OR

    https://www.elastic.co/guide/en/elasticsearch/reference/5.5/search-request-body.html

9. If the new index looks good, update index alias and delete current index.

.. note::

    If you want the current index to linger for a day or two to serve a rollback option, reindex the current index into another new index called kbase.genome_1_backup and then delete the current index. This is one of two ways of renaming an index in ElasticSearch. The other way is to use the snapshot API.

.. code-block:: bash

    POST _aliases
    {
     "actions": [
     {
       "add": {
         "index": "kbase.genome_2",
         "alias": "kbase.genome"
         }
       },
       {
         "remove": {
         "index": "kbase.genome_1",
         "alias": "kbase.genome"
       }
     }
     ]
    }

    DELETE kbase.genome_1

10. List all available indexes for the genome alias and all available genome indexes to ensure consistency across the alias map. Verify that all genome indexes that are present (except for backups) are referenced by the alias. Also verify that the alias does not contain an index reference for which no index exists.

.. code-block:: bash

    GET /_cat/aliases/kbase.genome

    GET /_cat/indices/kbase.genome_*

11. If the change involved in the reindexing operation also requires a corresponding search type spec change (located in resources/types/genome.yml for example), then this change must be applied to the spec as well.

12. Change mapping version from "1" to "2" in the resources/types/genome.yml search type spec and add a comment (for future reference) that describes the change that took place in the reindexing operation.

13. Generate new events for the time range for which the change was made to the data source.

14. Restart trickle updates from the current time noted in step 1.
