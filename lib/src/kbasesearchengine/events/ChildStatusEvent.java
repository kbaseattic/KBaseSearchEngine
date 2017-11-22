package kbasesearchengine.events;

import kbasesearchengine.tools.Utils;

/** An event resulting from the expansion of a single event into two or more sub events.
 * @author gaprice@lbl.gov
 *
 */
public class ChildStatusEvent implements StatusEventWithId {
    
    private final StatusEvent event;
    private final StatusEventID parentId;
    
    /** Create a child event.
     * @param event the child status event.
     * @param parentId the ID of the parent status event.
     */
    public ChildStatusEvent(final StatusEvent event, final StatusEventID parentId) {
        Utils.nonNull(event, "event");
        Utils.nonNull(parentId, "parentId");
        this.event = event;
        this.parentId = parentId;
    }

    @Override
    public StatusEventID getId() {
        return parentId;
    }

    @Override
    public boolean isParentId() {
        return true;
    }
    
    @Override
    public StatusEvent getEvent() {
        return event;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((event == null) ? 0 : event.hashCode());
        result = prime * result
                + ((parentId == null) ? 0 : parentId.hashCode());
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
        ChildStatusEvent other = (ChildStatusEvent) obj;
        if (event == null) {
            if (other.event != null) {
                return false;
            }
        } else if (!event.equals(other.event)) {
            return false;
        }
        if (parentId == null) {
            if (other.parentId != null) {
                return false;
            }
        } else if (!parentId.equals(other.parentId)) {
            return false;
        }
        return true;
    }
}
