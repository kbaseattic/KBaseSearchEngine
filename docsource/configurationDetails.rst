Configuration Details
======================
When mapping storage object type versions to search object type versions, the storage object types must be integers. This means that versions that use the format major#.minor# (as in the case of KBase Workspace types) cannot be mapped. But since  Search only cares about backwards incompatible changes it therefore expects only the major# in this case.


search_tools.cfg is for the search_tools.sh script, deploy.cfg is for the service. deploy.cfg is the standard name we use for all services. so we need to populate both cfgs with the same key-values where there are duplicates!

but they're for separate applications. if they were merged them there'd be a ton of stuff in there that the service doesn't care about and the same for search_tools

Also the deploy.cfg has all those templated variables that kb-sdk fills in
Which search_tools won't do