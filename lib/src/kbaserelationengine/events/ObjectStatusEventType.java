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
	RENAME_ALL_VERSIONS,
	DELETE_ALL_VERSIONS,
	UNDELETE_ALL_VERSIONS,
	DELETE_ACCESS_GROUP, //TODO NOW need to remove access group from all access docs
	COPY_ACCESS_GROUP;
}
