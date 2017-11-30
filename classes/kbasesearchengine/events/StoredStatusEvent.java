package kbasesearchengine.events;

import java.time.Instant;

import com.google.common.base.Optional;

import kbasesearchengine.tools.Utils;

/** A status event stored in the storage system and therefore associated with an id
 * and a processing state.
 * @author gaprice@lbl.gov
 *
 */
public class StoredStatusEvent {
    
    private final StatusEvent event;
    private final StatusEventID id;
    private final StatusEventProcessingState state;
    private final Optional<Instant> updateTime;
    private final Optional<String> updater;
    
    // right on the edge of needing to convert to a builder...
    
    /** Create a stored event.
     * @param event the event.
     * @param id the event ID.
     * @param state the processing state of the event.
     * @param updateTime the time of the last update to the processing state. Optional and
     * nullable.
     * @param updater the id of the operator that last updated the processing state. Optional and
     * nullable.
     */
    public StoredStatusEvent(
            final StatusEvent event,
            final StatusEventID id,
            final StatusEventProcessingState state,
            final Instant updateTime,
            final String updater) {
        Utils.nonNull(event, "event");
        Utils.nonNull(id, "id");
        Utils.nonNull(state, "state");
        this.event = event;
        this.id = id;
        this.state = state;
        this.updateTime = Optional.fromNullable(updateTime);
        if (Utils.isNullOrEmpty(updater)) {
            this.updater = Optional.absent();
        } else {
            this.updater = Optional.of(updater);
        }
    }

    /** Get the status event.
     * @return the id.
     */
    public StatusEvent getEvent() {
        return event;
    }

    /** Get the id.
     * @return the id.
     */
    public StatusEventID getId() {
        return id;
    }

    /** Return the state of the stored event the last time it was accessed.
     * @return the event processing state.
     */
    public StatusEventProcessingState getState() {
        return state;
    }
    
    /** Get the time at which the event was updated.
     * @return the update time, or absent if missing.
     */
    public Optional<Instant> getUpdateTime() {
        return updateTime;
    }

    /** Return the ID of the operator that last updated the processing state.
     * @return the operator ID, or absent if missing.
     */
    public Optional<String> getUpdater() {
        return updater;
    }

    // will add equals & hashcode if needed.
    
}
