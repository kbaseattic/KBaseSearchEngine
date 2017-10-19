package kbasesearchengine.events;

import java.time.Instant;

import com.google.common.base.Optional;

import kbasesearchengine.common.GUID;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.tools.Utils;

/** An event representing a change in status to which the indexer should respond.
 * @author gaprice@lbl.gov
 *
 */
public class StatusEvent {

    //TODO CODE add some validation that the available fields match the requirement for the event type
    
    private final Instant time;
    private final StatusEventType eventType;
    private final String storageCode;
    private final Optional<StorageObjectType> storageObjectType;
    private final Optional<Integer> accessGroupID;
    private final Optional<String> objectID;
    private final Optional<Integer> version;
    private final Optional<Boolean> isPublic;
    private final Optional<String> newName;
    private final StatusEventProcessingState processingState;
    
    private StatusEvent(
            final StatusEventType eventType,
            final String storageCode,
            final Optional<Integer> accessGroupID,
            final Optional<String> objectID,
            final Optional<Integer> version,
            final Optional<StorageObjectType> storageObjectType,
            final Instant time,
            final Optional<Boolean> isPublic,
            final Optional<String> newName,
            final StatusEventProcessingState processingState) {
        this.eventType = eventType;
        this.storageCode = storageCode;
        this.accessGroupID = accessGroupID;
        this.objectID = objectID;
        this.version = version;
        this.storageObjectType = storageObjectType;
        this.time = time;
        this.isPublic = isPublic;
        this.newName = newName;
        this.processingState = processingState;
    }

    /** The GUID of the data involved in the event. All fields except the storage code may be null.
     * @return the GUID.
     */
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

    /** Get the storage code for the storage system in which the event occurred.
     * @return
     */
    public String getStorageCode() {
        return storageCode;
    }

    /** Get the id of the access group containing the data involved in the event.
     * @return the access group id if available.
     */
    public Optional<Integer> getAccessGroupId() {
        return accessGroupID;
    }

    /** Get the object id of the data involved in the event.
     * @return the object id if available.
     */
    public Optional<String> getAccessGroupObjectId() {
        return objectID;
    }

    /** Get the version of the data involved in the event.
     * @return the version if available.
     */
    public Optional<Integer> getVersion() {
        return version;
    }

    /** Get the object type of the data involved in the event.
     * @return the object type if available.
     */
    public Optional<StorageObjectType> getStorageObjectType() {
        return storageObjectType;
    }

    /** Get the type of this event.
     * @return the event type.
     */
    public StatusEventType getEventType() {
        return eventType;
    }

    /** Get the time the event occurred.
     * @return the event timestamp.
     */
    public Instant getTimestamp() {
        return time;
    }

    /** Get whether the data involved in the event is publicly accessible.
     * @return whether the data is publicly accessible, if known.
     */
    public Optional<Boolean> isPublic(){
        return isPublic;
    }

    /** Get the new name for the data involved in the event.
     * @return the new name if available.
     */
    public Optional<String> getNewName() {
        return newName;
    }
    
    /** Get the processing state of the event.
     * @return the event's processing state.
     */
    public StatusEventProcessingState getProcessingState() {
        return processingState;
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
    
    /** Get a builder for an event when the object type is not known.
     * @param storageCode the storage code for the storage system where the event occurred.
     * @param time the time the event occurred.
     * @param eventType the type of the event.
     * @return a builder for the event.
     */
    public static Builder getBuilder(
            final String storageCode,
            final Instant time,
            final StatusEventType eventType) {
        return new Builder(storageCode, time, eventType);
    }
    
    /** Get a builder for an event.
     * @param storageType the object type of the data involved in the event.
     * @param time the time the event occurred.
     * @param eventType the type of the event.
     * @return a builder for the event.
     */
    public static Builder getBuilder(
            final StorageObjectType storageType,
            final Instant time,
            final StatusEventType eventType) {
        return new Builder(storageType, time, eventType);
    }
    
    /** A builder for a status event.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {
        
        private final Instant time;
        private final StatusEventType eventType;
        private final String storageCode;
        private final Optional<StorageObjectType> storageObjectType;
        private Optional<Integer> accessGroupID = Optional.absent();
        private Optional<String> objectID = Optional.absent();
        private Optional<Integer> version = Optional.absent();
        private Optional<Boolean> isPublic = Optional.absent();
        private Optional<String> newName = Optional.absent();
        private StatusEventProcessingState processingState = StatusEventProcessingState.UNPROC;
        
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
        
        /** Add an access group ID to the event. Null values will remove any previously set
         * access group IDs in the builder.
         * @param accessGroupID the access group ID to add to the builder.
         * @return this builder.
         */
        public Builder withNullableAccessGroupID(final Integer accessGroupID) {
            this.accessGroupID = Optional.fromNullable(accessGroupID);
            return this;
        }
        
        /** Add an access group object ID to the event. Null values will remove any previously set
         * access group object IDs in the builder.
         * @param objectID the access group object ID to add to the builder.
         * @return this builder.
         */
        public Builder withNullableObjectID(final String objectID) {
            if (Utils.isNullOrEmpty(objectID)) {
                this.objectID = Optional.absent();
            } else {
                this.objectID = Optional.of(objectID);
            }
            return this;
        }
        
        /** Add an object version to the event. Null values will remove any previously set
         * version in the builder.
         * @param version the version to add to the builder.
         * @return this builder.
         */
        public Builder withNullableVersion(final Integer version) {
            this.version = Optional.fromNullable(version);
            return this;
        }
        
        /** Add public flag to the event. Null values will remove any previously set
         * public flag in the builder.
         * @param isPublic the public flag to add to the builder.
         * @return this builder.
         */
        public Builder withNullableisPublic(final Boolean isPublic) {
            this.isPublic = Optional.fromNullable(isPublic);
            return this;
        }
        
        /** Add a new object name to the event. Null values will remove any previously set
         * name in the builder.
         * @param accessGroupID the name to add to the builder.
         * @return this builder.
         */
        public Builder withNullableNewName(final String newName) {
            if (Utils.isNullOrEmpty(newName)) {
                this.newName = Optional.absent();
            } else {
                this.newName = Optional.of(newName);
            }
            return this;
        }
        
        /** Set the processing state of this event. Defaults to unprocessed.
         * @param state the event's processing state.
         * @return this builder.
         */
        // note default is unprocessed
        public Builder withProcessingState(final StatusEventProcessingState state) {
            Utils.nonNull(state, "state");
            this.processingState = state;
            return this;
        }
        
        /** Build the status event.
         * @return the new event.
         */
        public StatusEvent build() {
            return new StatusEvent(eventType, storageCode, accessGroupID, objectID,
                    version, storageObjectType, time, isPublic, newName, processingState);
        }
    }
}
