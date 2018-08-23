Operations
===========
Working with Spec files:

Indexing is done with reference to spec files. The spec files contain the extraction and transformation information required by the indexer for each object type. The application maintains some spec files in the resources directory. It is up to the operations person to provide these specs to the object types that need to be indexed. In addition, type mappings for the specs may also be provided (see resources/type_mappings/GenomeAndAssembly.yaml.example).

Note: If a spec references dependent objects, then specs for the dependent objects must also be specified. If specs of dependent objects are not found, the parent objects will not be indexed and the indexing operation will log a non-fatal error.


System Requirements:

The following cache values have been hard-coded in kbasesearchengine/KBaseSearchEngineServer.java and memory for these caches need to be allocated.

 AccessGroupCache => 200mb (50k simultaneous users * 1000 group integer ids)

 NarrativeInfoCache => 6Gb (50k simultaneous users * 1000 NarrativeInfo objects of approx 60bytes*2FactorOfSafety)

 AuthInfoCache => 2Gb (50k simultaneous users * 1000 AuthInfo objects of approx 20bytes*2FactorOfSafety)

Detecting Service Status:

curl -d '{"method": "KBaseSearchEngine.status", "version": "1.1", "id": 1, "params": []}' https://ci.kbase.us/services/searchapi  | python -mjson.tool
{
    "result": [
        {
            "git_commit_hash": "da56aaf594088a985757f8cd26a778d53807e506",
            "git_url": "https://github.com/kbase/KBaseSearchEngine.git",
            "message": "",
            "state": "OK",
            "version": "0.1.1"
        }
    ],
    "version": "1.1"
}

ElasticSearch Setup:

Unless required, it is recommended to prevent dynamic index creation.

PUT _cluster/settings
{
 "persistent": {
   "action.auto_create_index" : false
 }
}


Backup and Restoration of index
Deploying multiple indexes

