Search Service MKII release notes
=================================

0.1.1
-----

* Fixed a bug where an empty list of subobjects in a parent object would cause a general (e.g.
  no `key.*` fields) parent record to be stored in the subobject index in ElasticSearch. 
* Removed `data_includes` from the API, as it was unimplemented.
* Changed `lookupInKeys` to lookup\_in_keys to be consistent with other fields.
* Removed `skip_info` from the API, as it is unused and provides little to no benefit in
  transport costs.
* Removed `object_props` from the API and moved its contents into top level fields in the
  `ObjectData` structure.

0.1.0
-----

* Initial release