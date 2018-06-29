Functional Requirements
========================

1. Support the indexing of many data types (Genome, Assembly, Narrative etc). It is expected that there will be about 1000 types

2. Support the indexing of versions of object types. It is expected that each object can have at most about 100 versions.

3. Support the indexing of multiple versions of an object. A specific version of a specific object may have mutable data that may require index updates.

4. In general flatten or denormalize data to improve search performance. Properly balance search performance and update times for mutable data.

5. Data object sizes can be as big as 1Gb.

6. Allow for searching only the last version of an object. Also allow for the ability to search any version of an object.

7. Include access control information in the index records to allow results to be filtered based on user credentials.

8. While indexing is based on events written to a mongodb, workspace requests are made to get the actual data. The events must be stateless.

9. A retry mechanism is required for indexing to compensate for drops in network connection.

10. Ability to make trickle updates to the indexes with changes made to the workspace for specific time ranges or from a given time to current time.

11. Wrap ElasticSearch API with a generalized API that includes access control information.