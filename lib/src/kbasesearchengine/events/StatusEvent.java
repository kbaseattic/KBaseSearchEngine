package kbasesearchengine.events;

import java.time.Instant;

import com.google.common.base.Optional;

import kbasesearchengine.common.GUID;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.tools.Utils;

public class StatusEvent {

    //TODO JAVADOC
    //TODO TEST
    //TODO add some validation that the available fields match the requirement for the event type
    
    private final Instant time;
    private final StatusEventType eventType;
    private final String storageCode;
    private final Optional<StorageObjectType> storageObjectType;
    private final String id; // TODO NOW remove
    private final Optional<Integer> accessGroupID;
    private final Optional<String> objectID;
    private final Optional<Integer> version;
    private final Optional<Boolean> isPublic;
    private final Optional<String> newName;
    
    private StatusEvent(
            final String id,
            final StatusEventType eventType,
            final String storageCode,
            final Optional<Integer> accessGroupID,
            final Optional<String> objectID,
            final Optional<Integer> version,
            final Optional<StorageObjectType> storageObjectType,
            final Instant time,
            final Optional<Boolean> isPublic,
            final Optional<String> newName) {
        this.id = id;
        this.eventType = eventType;
        this.storageCode = storageCode;
        this.accessGroupID = accessGroupID;
        this.objectID = objectID;
        this.version = version;
        this.storageObjectType = storageObjectType;
        this.time = time;
        this.isPublic = isPublic;
        this.newName = newName;
    }

    public GUID toGUID(){
        // TODO NOW should this throw an error if some or all of the ids are null?
        return new GUID(
                getStorageCode(),
                accessGroupID.orNull(),
                objectID.orNull(),
                version.orNull(),
                null,
                null);
    }

    public String getId() {
        return id;
    }

    public String getStorageCode() {
        return storageCode;
    }

    public Optional<Integer> getAccessGroupId() {
        return accessGroupID;
    }

    public Optional<String> getAccessGroupObjectId() {
        return objectID;
    }

    public Optional<Integer> getVersion() {
        return version;
    }

    public Optional<StorageObjectType> getStorageObjectType() {
        return storageObjectType;
    }

    public StatusEventType getEventType() {
        return eventType;
    }

    public Instant getTimestamp() {
        return time;
    }

    public Optional<Boolean> isGlobalAccessed(){
        return isPublic;
    }

    public Optional<String> getNewName() {
        return newName;
    }
    
    @Override
    public String toString() {
        StringBuilder builder2 = new StringBuilder();
        builder2.append("StatusEvent [time=");
        builder2.append(time);
        builder2.append(", eventType=");
        builder2.append(eventType);
        builder2.append(", storageCode=");
        builder2.append(storageCode);
        builder2.append(", storageObjectType=");
        builder2.append(storageObjectType);
        builder2.append(", id=");
        builder2.append(id);
        builder2.append(", accessGroupID=");
        builder2.append(accessGroupID);
        builder2.append(", objectID=");
        builder2.append(objectID);
        builder2.append(", version=");
        builder2.append(version);
        builder2.append(", isPublic=");
        builder2.append(isPublic);
        builder2.append(", newName=");
        builder2.append(newName);
        builder2.append("]");
        return builder2.toString();
    }
    
    public static Builder getBuilder(
            final String storageCode,
            final Instant time,
            final StatusEventType eventType) {
        return new Builder(storageCode, time, eventType);
    }
    
    public static Builder getBuilder(
            final StorageObjectType storageType,
            final Instant time,
            final StatusEventType eventType) {
        return new Builder(storageType, time, eventType);
    }
    
    public static class Builder {
        
        private final Instant time;
        private final StatusEventType eventType;
        private final String storageCode;
        private final Optional<StorageObjectType> storageObjectType;
        private String id = null; // TODO NOW remove
        private Optional<Integer> accessGroupID = Optional.absent();
        private Optional<String> objectID = Optional.absent();
        private Optional<Integer> version = Optional.absent();
        private Optional<Boolean> isPublic = Optional.absent();
        private Optional<String> newName = Optional.absent();
        
        private Builder(
                final String storageCode,
                final Instant time,
                final StatusEventType eventType) {
            Utils.notNullOrEmpty(storageCode, "storageCode cannot be null or the empty string");
            Utils.nonNull(time, "time");
            Utils.nonNull(eventType, "eventType");
            this.time = time;
            this.storageCode = storageCode;
            this.eventType = eventType;
            this.storageObjectType = Optional.absent();
        }
        
        private Builder(
                final StorageObjectType storageType,
                final Instant time,
                final StatusEventType eventType) {
            Utils.nonNull(time, "time");
            Utils.nonNull(eventType, "eventType");
            Utils.nonNull(storageType, "storageType");
            this.time = time;
            this.storageCode = storageType.getStorageCode();
            this.eventType = eventType;
            this.storageObjectType = Optional.of(storageType);
        }
        
        //TODO NOW remove. Use StatusEventWithID
        public Builder withID(final String id) {
            this.id = id;
            return this;
        }
        
        public Builder withNullableAccessGroupID(final Integer accessGroupID) {
            this.accessGroupID = Optional.fromNullable(accessGroupID);
            return this;
        }
        
        public Builder withNullableObjectID(final String objectID) {
            if (Utils.isNullOrEmpty(objectID)) {
                this.objectID = Optional.absent();
            } else {
                this.objectID = Optional.of(objectID);
            }
            return this;
        }
        
        public Builder withNullableVersion(final Integer version) {
            this.version = Optional.fromNullable(version);
            return this;
        }
        
        public Builder withNullableisPublic(final Boolean isPublic) {
            this.isPublic = Optional.fromNullable(isPublic);
            return this;
        }
        
        public Builder withNullableNewName(final String newName) {
            if (Utils.isNullOrEmpty(newName)) {
                this.newName = Optional.absent();
            } else {
                this.newName = Optional.of(newName);
            }
            return this;
        }
        
        public StatusEvent build() {
            return new StatusEvent(id, eventType, storageCode, accessGroupID, objectID,
                    version, storageObjectType, time, isPublic, newName);
        }
    }
}
