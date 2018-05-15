Search Service MKII release notes
=================================

0.1.2
-----

* Added a map of narrative info (containing narrative id, narrative name, date created,
  workspace username, workspace display name), to the search results. The narrative info and
  auth info (containing workspace user display name) are cached.

0.1.1
-----

* Fixed a bug where an empty list of subobjects in a parent object would cause a general (e.g.
  no `key.*` fields) parent record to be stored in the subobject index in ElasticSearch. 
* Adds more information to the error thrown when encountering an unexpected type in a recursive
  index
* Removed `data_includes` from the API, as it was unimplemented.
* Changed `lookupInKeys` to lookup\_in_keys to be consistent with other fields.
* Removed `skip_info` from the API, as it is unused and provides little to no benefit in
  transport costs.
* Removed `object_props` from the API and moved its contents into top level fields in the
  `ObjectData` structure.

0.1.0
-----

* Initial release