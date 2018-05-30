Operations
===========
System Requirements:

The following cache values have been hard-coded in kbasesearchengine/KBaseSearchEngineServer.java and memory for these caches need to me allocated.

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

Backup and Restoration of index
Deploying multiple indexes

