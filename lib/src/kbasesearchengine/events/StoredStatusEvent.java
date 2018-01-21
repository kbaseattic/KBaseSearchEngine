package kbasesearchengine.events;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Optional;

import kbasesearchengine.tools.Utils;

/** A status event stored in the storage system and therefore associated with an id
 * and a processing state.
 * @author gaprice@lbl.gov
 *
 */
public class StoredStatusEvent implements StatusEventWithId {
    
    private final StatusEvent event;
    private final StatusEventID id;
    private final StatusEventProcessingState state;
    private final Optional<Instant> updateTime;
    private final Optional<String> updater;
    private final Set<String> tags;
    
    private StoredStatusEvent(
            final StatusEvent event,
            final StatusEventID id,
            final StatusEventProcessingState state,
            final Optional<Instant> updateTime,
            final Optional<String> updater,
            final Set<String> tags) {
        this.event = event;
        this.id = id;
        this.state = state;
        this.updateTime = updateTime;
        this.updater = updater;
        this.tags = Collections.unmodifiableSet(tags);
    }

    @Override
    public StatusEvent getEvent() {
        return event;
    }

    @Override
    public StatusEventID getId() {
        return id;
    }
    
    @Override
    public boolean isParentId() {
        return false;
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
    
    /** Return the tags that specify which workers can process the event.
     * @return the tags.
     */
    public Set<String> getTags() {
        return tags;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((event == null) ? 0 : event.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((state == null) ? 0 : state.hashCode());
        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        result = prime * result
                + ((updateTime == null) ? 0 : updateTime.hashCode());
        result = prime * result + ((updater == null) ? 0 : updater.hashCode());
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
        StoredStatusEvent other = (StoredStatusEvent) obj;
        if (event == null) {
            if (other.event != null) {
                return false;
            }
        } else if (!event.equals(other.event)) {
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
        if (tags == null) {
            if (other.tags != null) {
                return false;
            }
        } else if (!tags.equals(other.tags)) {
            return false;
        }
        if (updateTime == null) {
            if (other.updateTime != null) {
                return false;
            }
        } else if (!updateTime.equals(other.updateTime)) {
            return false;
        }
        if (updater == null) {
            if (other.updater != null) {
                return false;
            }
        } else if (!updater.equals(other.updater)) {
            return false;
        }
        return true;
    }

    /** Get a builder for a {@link StoredStatusEvent}.
     * @param event the stored event.
     * @param id the ID of the event.
     * @param state the state of the event.
     * @return a new builder.
     */
    public static Builder getBuilder(
            final StatusEvent event,
            final StatusEventID id,
            final StatusEventProcessingState state) {
        return new Builder(event, id, state);
    }
    
    /** A builder for {@link StoredStatusEvent}s.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {

        private final StatusEvent event;
        private final StatusEventID id;
        private final StatusEventProcessingState state;
        private Optional<Instant> updateTime = Optional.absent();
        private Optional<String> updater = Optional.absent();
        private Set<String> tags = new HashSet<>();
        
        private Builder(
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
        
        /** Add a tag that specifies which workers can process this event to the builder.
         * @param tag the tag.
         * @return this builder.
         */
        public Builder withTag(final String tag) {
            Utils.notNullOrEmpty(tag, "tag cannot be null or whitespace");
            tags.add(tag);
            return this;
        }
        
        /** Add an update time and optional updater id to the event.
         * @param updateTime the update time. If null, the update is wholly ignored.
         * @param updater the id of the updater. If null or whitespace the id is ignored.
         * @return this builder.
         */
        public Builder withNullableUpdate(final Instant updateTime, final String updater) {
            if (updateTime == null) {
                return this;
            }
            this.updateTime = Optional.of(updateTime);
            if (!Utils.isNullOrEmpty(updater)) {
                this.updater = Optional.of(updater);
            } else {
                this.updater = Optional.absent();
            }
            return this;
        }
        
        /** Build the {@link StoredStatusEvent}.
         * @return the event.
         */
        public StoredStatusEvent build() {
            return new StoredStatusEvent(event, id, state, updateTime, updater, tags);
        }
    }
}
