Operations
===========
Working with Spec files:

Indexing is done with reference to spec files. The spec files contain the extraction and transformation information required by the indexer for each object type. The application maintains some spec files in the resources directory. It is up to the operations person to provide these specs to the object types that need to be indexed. In addition, type mappings for the specs may also be provided (see resources/type_mappings/GenomeAndAssembly.yaml.example).

Note: If a spec references dependent objects, then specs for the dependent objects must also be specified. If specs of dependent objects are not found, the parent objects will not be indexed and the indexing operation will log a non-fatal error.

Backup and Restoration of index
Deploying multiple indexes

