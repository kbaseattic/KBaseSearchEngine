package kbasesearchengine.events;

public enum StatusEventType {
	NEW_VERSION,
	DELETED,
	SHARED,
	UNSHARED,
	PUBLISH_ALL_VERSIONS,
	PUBLISH_ACCESS_GROUP, //TODO NOW need to add access group to access docs public list where access group in groups list
	UNPUBLISH_ALL_VERSIONS,
	UNPUBLISH_ACCESS_GROUP, //TODO NOW need to remove access group from all access docs public list
	NEW_ALL_VERSIONS,
	RENAME_ALL_VERSIONS,
	DELETE_ALL_VERSIONS,
	UNDELETE_ALL_VERSIONS,
	DELETE_ACCESS_GROUP, //TODO NOW need to remove access group from all access docs
	COPY_ACCESS_GROUP; // TODO DP need to handle data palette
}
