Search Service MKII release notes
=================================

0.2.0
-----
1. Removed recursive indexing. 

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