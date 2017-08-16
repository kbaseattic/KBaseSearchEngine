package kbaserelationengine.events;

public enum ObjectStatusEventType {
	CREATED,
	NEW_VERSION,
	DELETED,
	SHARED,
	UNSHARED,
	PUBLISHED,
	UNPUBLISHED,
	NEW_ALL_VERSIONS,
	COPY_ACCESS_GROUP; //TODO ACL need to add the access group details to mongo (maybe)
}
