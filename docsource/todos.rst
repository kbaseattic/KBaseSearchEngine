TODOs (ideas for improvement)
=====

1. After the first release, check if it makes sense to make global-object-type the same as the storage-object-type. This can help simplify the type spec by getting rid of one of the key-value pairs. It is also nice to keep types consistent across systems. The benefit that should be sought is if it will allow users to search specifically within the kbaseGenomes set, someOtherGenomes sets, and all genomes in general. Currently, it is not clear what the final indexed genome object looks like and what its searchability is like.

2. It seems like it will be better to completely remove "full-text" as a key in the type specs and instead use "keyword-type: text". It simplifies the spec. If "keyword-type: text" is currently substitutable for "full-text: true" then I think we should make this change and remove the "full-text" as a valid key. It seems like the option "full-text: false" and "keyword-type: string" (for structured content) is supported in ESv5 with just "keyword-type: keyword".

3. Make all type specs YAML and escape all json files on loading type specs.

4. Must write a formal spec for type spec