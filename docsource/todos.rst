TODOs (ideas for improvement)
=====


1. It seems like it will be better to completely remove "full-text" as a key in the type specs and instead use "keyword-type: text". It simplifies the spec. If "keyword-type: text" is currently substitutable for "full-text: true" then I think we should make this change and remove the "full-text" as a valid key. It seems like the option "full-text: false" and "keyword-type: string" (for structured content) is supported in ESv5 with just "keyword-type: keyword".

2. Make all type specs YAML and skip any json files when loading type specs.

3. Must write a formal spec for type spec