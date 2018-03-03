package kbasesearchengine.events;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import kbasesearchengine.tools.Utils;

/** A {@link ChildStatusEvent} that is stored in a storage system, and therefore has additional
 * attributes associated with that storage - an ID, the final state of the event, and the time
 * the event was stored.
 * @author gaprice@lbl.gov
 *
 */
public class StoredChildStatusEvent {
    
    private final static Set<StatusEventProcessingState> ALLOWED_STATES =
            new HashSet<>(Arrays.asList(StatusEventProcessingState.FAIL,
                    StatusEventProcessingState.INDX, StatusEventProcessingState.UNINDX));

    // this is on the edge of needing a builder, but all the fields are required, so...
    private final ChildStatusEvent childEvent;
    private final StatusEventProcessingState state;
    private final StatusEventID id;
    private final Instant storeTime;
    
    /** Create a stored child status event.
     * @param childEvent the child event.
     * @param state the final state of the event.
     * @param id 
     * @param storeTime
     */
    public StoredChildStatusEvent(
            final ChildStatusEvent childEvent,
            final StatusEventProcessingState state,
            final StatusEventID id,
            final Instant storeTime) {
        Utils.nonNull(childEvent, "childEvent");
        Utils.nonNull(state, "state");
        Utils.nonNull(id, "id");
        Utils.nonNull(storeTime, "storeTime");
        if (!isAllowedState(state)) {
            throw new IllegalArgumentException("Child events may only have terminal states");
        }
        this.childEvent = childEvent;
        this.state = state;
        this.id = id;
        this.storeTime = storeTime;
    }

    public static boolean isAllowedState(final StatusEventProcessingState state) {
        return ALLOWED_STATES.contains(state);
    }
    
    /** Get the child event.
     * @return the child event.
     */
    public ChildStatusEvent getChildEvent() {
        return childEvent;
    }

    /** The final state of the child event - one of {@link StatusEventProcessingState#INDX},
     * {@link StatusEventProcessingState#UNINDX}, or {@link StatusEventProcessingState#FAIL}.
     * @return the event state.
     */
    public StatusEventProcessingState getState() {
        return state;
    }

    /** Get the child event's ID in the storage system.
     * @return the ID.
     */
    public StatusEventID getID() {
        return id;
    }

    /** Get the time the child event was stored in the storage system.
     * @return the stored time.
     */
    public Instant getStoreTime() {
        return storeTime;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((childEvent == null) ? 0 : childEvent.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((state == null) ? 0 : state.hashCode());
        result = prime * result
                + ((storeTime == null) ? 0 : storeTime.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        StoredChildStatusEvent other = (StoredChildStatusEvent) obj;
        if (childEvent == null) {
            if (other.childEvent != null) {
                return false;
            }
        } else if (!childEvent.equals(other.childEvent)) {
            return false;
        }
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (state != other.state) {
            return false;
        }
        if (storeTime == null) {
            if (other.storeTime != null) {
                return false;
            }
        } else if (!storeTime.equals(other.storeTime)) {
            return false;
        }
        return true;
    }
}
