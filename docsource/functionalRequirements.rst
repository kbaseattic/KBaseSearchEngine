Functional Requirements
========================

1. Support the indexing of many data types (Genome, Assembly, Narrative etc). It is expected that there will be about 100 types

2. Support the indexing of versions of types. It is expected that each object can have on average about 100 versions.

3. Support the indexing of instance versions. A specific version of a specific object may have mutable data that me require index updates.

4. In general flatten or denormalize data to improve search performance. But, where applicable, use the parent-child relationship to minimize load on indexing when parent data changes and this change can affect upwards of thousands of children.

5. Data object sizes can be as big as 1Gb.

6. Allow for searching only the last version (can be done with painless script) of an object. Also allow for the ability to search any version of an object.

7. Include access control information in the index records to allow results to be filtered based on user credentials.

8. While indexing is based on events written to a mongodb, workspace requests are made to get the actual data. The events must be stateless.

9. A retry mechanism is required for indexing to compensate for drops in network connection.

10. Ability to make trickle updates to the indexes with changes made to the workspace for specific time ranges or from a given time to current time.

11. Wrap ElasticSearch API with a generalized API that includes access control information.