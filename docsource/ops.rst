Operations
===========
System Requirements:

The following cache values have been hard-coded in kbasesearchengine/KBaseSearchEngineServer.java and memory for these caches need to me allocated.

 AccessGroupCache => 200mb (50k simultaneous users * 1000 group integer ids)

 NarrativeInfoCache => 6Gb (50k simultaneous users * 1000 NarrativeInfo objects of approx 60bytes*2FactorOfSafety)

 AuthInfoCache => 2Gb (50k simultaneous users * 1000 AuthInfo objects of approx 20bytes*2FactorOfSafety)

Backup and Restoration of index
Deploying multiple indexes

