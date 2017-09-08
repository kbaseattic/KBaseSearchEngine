
KBaseSearchEngine
=================

Powers KBase Search MKII.

Type Mapping
------------

### Type transformation specifications

Example type transformation `json` files are in `resources/types`.

TODO documentation of the transformation spec.

### Type mapping specifications

Type mappings are optional `yaml` files that specify how to map data source types to search types.
If provided for a source type, they override the mapping provided in the type transformation
file(s) (in the `source-type` and `source-object-type` fields). In particular, the mapping
files are aware of the source type version while the transformation files are not.

Mapping files also allow using the same set of transformation files for multiple environments
where the source types may not have equivalent names by providing environment-specific mapping
files. 

There is an example mapping file in `resources/typemappings` that explains the structure and
how the mappings work.


