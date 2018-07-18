Known Issues
=============

1. A sequence of decarotors make calls to the workspace to collect narrative and other information. The reasoning behind this is because the narrative info that is desired by the front is mutable, and so indexing it is expensive in that a change in the narrative (a name change for example) could require updating millions, or even billions, of records, since every subobject of every version of every object in the workspace would need to be updated. There is room here to simplify design by eliminating these decorators and using parent-child relationships.