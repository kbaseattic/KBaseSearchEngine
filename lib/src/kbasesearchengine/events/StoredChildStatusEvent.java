package kbasesearchengine.events;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Optional;

import kbasesearchengine.tools.Utils;

/** A {@link ChildStatusEvent} that is stored in a storage system, and therefore has additional
 * attributes associated with that storage - an ID, the final state of the event, the time
 * the event was stored, and possibly error information.
 * @author gaprice@lbl.gov
 *
 */
public class StoredChildStatusEvent {
    
    private final static Set<StatusEventProcessingState> ALLOWED_STATES =
            new HashSet<>(Arrays.asList(StatusEventProcessingState.FAIL,
                    StatusEventProcessingState.INDX, StatusEventProcessingState.UNINDX));

    private final ChildStatusEvent childEvent;
    private final StatusEventProcessingState state;
    private final StatusEventID id;
    private final Instant storeTime;
    private final Optional<String> errorCode;
    private final Optional<String> errorMessage;
    private final Optional<String> errorStackTrace;
    
    private StoredChildStatusEvent(
            final ChildStatusEvent childEvent,
            final StatusEventProcessingState state,
            final StatusEventID id,
            final Instant storeTime,
            final String errorCode,
            final String errorMessage,
            final String errorStackTrace) {
        this.childEvent = childEvent;
        this.state = state;
        this.id = id;
        this.storeTime = storeTime;
        this.errorCode = Optional.fromNullable(errorCode);
        this.errorMessage = Optional.fromNullable(errorMessage);
        this.errorStackTrace = Optional.fromNullable(errorStackTrace);
    }

    /** Check if an event processing state is allowed as a state for a child event. Child
     * events can only possess terminal states - one of {@link StatusEventProcessingState#INDX},
     * {@link StatusEventProcessingState#UNINDX}, or {@link StatusEventProcessingState#FAIL}.
     * @param state the state to check.
     * @return true if the state is allowed.
     */
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
    
    /** Get the error code for the error associated with this event, if any.
     * @return the error code.
     */
    public Optional<String> getErrorCode() {
        return errorCode;
    }

    /** Get the error message for the error associated with this event, if any.
     * @return the error message.
     */
    public Optional<String> getErrorMessage() {
        return errorMessage;
    }

    /** Get the error stack trace for the error associated with this event, if any.
     * @return the error stack trace.
     */
    public Optional<String> getErrorStackTrace() {
        return errorStackTrace;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((childEvent == null) ? 0 : childEvent.hashCode());
        result = prime * result
                + ((errorCode == null) ? 0 : errorCode.hashCode());
        result = prime * result
                + ((errorMessage == null) ? 0 : errorMessage.hashCode());
        result = prime * result
                + ((errorStackTrace == null) ? 0 : errorStackTrace.hashCode());
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
        if (errorCode == null) {
            if (other.errorCode != null) {
                return false;
            }
        } else if (!errorCode.equals(other.errorCode)) {
            return false;
        }
        if (errorMessage == null) {
            if (other.errorMessage != null) {
                return false;
            }
        } else if (!errorMessage.equals(other.errorMessage)) {
            return false;
        }
        if (errorStackTrace == null) {
            if (other.errorStackTrace != null) {
                return false;
            }
        } else if (!errorStackTrace.equals(other.errorStackTrace)) {
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
    
    /** Get a builder for a {@link StoredChildStatusEvent}.
     * @param childEvent the child event.
     * @param id the id of the child event in the storage system.
     * @param storeTime the time the event was stored.
     * @return a new builder.
     */
    public static Builder getBuilder(
            final ChildStatusEvent childEvent,
            final StatusEventID id,
            final Instant storeTime) {
        return new Builder(childEvent, id, storeTime);
    }
    
    /** A builder for a {@link StoredChildStatusEvent}.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {
        
        private final ChildStatusEvent childEvent;
        private final StatusEventID id;
        private final Instant storeTime;
        private StatusEventProcessingState state = StatusEventProcessingState.FAIL;
        private String errorCode = null;
        private String errorMessage = null;
        private String errorStackTrace = null;
        
        private Builder(
                final ChildStatusEvent childEvent,
                final StatusEventID id,
                final Instant storeTime) {
            Utils.nonNull(childEvent, "childEvent");
            Utils.nonNull(id, "id");
            Utils.nonNull(storeTime, "storeTime");
            this.childEvent = childEvent;
            this.id = id;
            this.storeTime = storeTime;
        }
        
        /** Change the state of the child event. By default the event state is set as 
         * {@link StatusEventProcessingState#FAIL}. Allowed states are
         * {@link StatusEventProcessingState#INDX}, {@link StatusEventProcessingState#UNINDX},
         * and {@link StatusEventProcessingState#FAIL}.
         * @param state the new state.
         * @return this builder.
         */
        public Builder withState(final StatusEventProcessingState state) {
            Utils.nonNull(state, "state");
            if (!isAllowedState(state)) {
                throw new IllegalArgumentException("Child events may only have terminal states");
            }
            this.state = state;
            return this;
        }
        
        /** Associate an error to this event. If errorCode is null, the error information will
         * not be changed.
         * @param errorCode a short code for the error.
         * @param errorMessage a free text error message.
         * @param errorStackTrace the error stack trace.
         * @return this builder.
         */
        public Builder withNullableError(
                final String errorCode,
                final String errorMessage,
                final String errorStackTrace) {
            if (errorCode == null) {
                return this;
            }
            Utils.notNullOrEmpty(errorCode, "errorCode cannot be null or whitespace only");
            Utils.notNullOrEmpty(errorMessage, "errorMessage cannot be null or whitespace only");
            Utils.notNullOrEmpty(errorStackTrace,
                    "errorStackTrace cannot be null or whitespace only");
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
            this.errorStackTrace = errorStackTrace;
            return this;
        }
        
        /** Build the stored child event.
         * @return the event.
         */
        public StoredChildStatusEvent build() {
            return new StoredChildStatusEvent(childEvent, state, id, storeTime,
                    errorCode, errorMessage, errorStackTrace);
        }
    }
}
