Search Service MKII release notes
=================================
0.2.4
---
* Remove objects in deleted/inaccessible workspaces from search results. This should be a rare event that, 
  due to time delay in updating the searchvindex from  changes in the source data,
  objects returned from the search index may not be accessible at the data source. 

0.2.3
----
* Upgrade ES from 5.5 to 5.6.11
* Fix bugs in error handling: 
    - Information requested of deleted objects returns nulls instead of throwing errors. 
    - Errors casting bugs fixed


0.2.2-dev1
-----
* Changed search_objects and search_types to optionally require auth. Unauthorized calls will only
see results from public data. Will throw exception if with_public and with_private are both false.

* searchapi method "search_types" now returns up to 1000 types and document counts; was limited to 
  10 previously. This is hardcoded in ElasticIndexingStorage.java, and can be overriden in the 
  constructor for testing.

0.2.1
-----
****  Made the following changes that affect the interface.

* In KBaseSearchEngine.spec, renamed the usage of workspace_info to access_group_info,
  in the output from searchobjects and getobjects.
  * Renamed the usage of workspace_info with access_group_info in variable, method and
    class in the module.

* In KBaseSearchEngine.spec, moved the flags addNarrativeInfo and addAccessGroupInfo
  from match filter parameter to the postprocessing parameter provided to the search API.
  
* If index records for corresponding events already exist in the system, the events are ignored.
  The new CLI option `overwriteExistingData` is added to overwrite existing index records. When 
  generating events, tag events to overwrite the corresponding records in the indexes if these
  records already exist. Default value is false.

0.1.3
-----

* Added a Workspace Info decorator, which adds a map of workspaces info (key: workspace id and
  value: the Tuple from get_workspace_info()), and a map of objects info (key: object_ref
  value: the Tuple from get_object_info3) to the search results.

* The narrative info decorator and workspace info decorator can be enabled by setting the flags,
  addNarrativeInfo, and addWorkspaceInfo respectively in the match filter parameter provided to the
  search API.

0.1.2
-----

* Added a Narrative Info decorator, which adds a map of narrative info (containing narrative id,
  narrative name, date created, workspace username, workspace display name), to the search results.
  The narrative info and auth info (containing workspace user display name) are cached.

0.1.1
-----

1. Fixed a bug where an empty list of subobjects in a parent object would cause a general (e.g.
  no `key.*` fields) parent record to be stored in the subobject index in ElasticSearch. 
2. Adds more information to the error thrown when encountering an unexpected type in a recursive
  index
3. Removed `data_includes` from the API, as it was unimplemented.
4. Changed `lookupInKeys` to lookup\_in_keys to be consistent with other fields.
5. Removed `skip_info` from the API, as it is unused and provides little to no benefit in
  transport costs.
6. Removed `object_props` from the API and moved its contents into top level fields in the
  `ObjectData` structure.
7. Added CLI for re-running failed events.
8. All workspace handler events are now updated to reflect the latest state of objects before
   the objects are indexed. 

0.1.0
-----

1. Initial release
