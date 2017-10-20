package kbasesearchengine.events;

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
    
    /** Create a stored event.
     * @param event the event.
     * @param id the event ID.
     * @param state the processing state of the event.
     */
    public StoredStatusEvent(
            final StatusEvent event,
            final StatusEventID id,
            final StatusEventProcessingState state) {
        Utils.nonNull(event, "event");
        Utils.nonNull(id, "id");
        Utils.nonNull(state, "state");
        this.event = event;
        this.id = id;
        this.state = state;
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

    // will add equals & hashcode if needed.
    
}
