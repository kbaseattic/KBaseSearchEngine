package kbasesearchengine.events;

/** A status event associated with a unique ID, which may be of the event itself or the event's
 * parent in the case of expandable events.
 * @author gaprice@lbl.gov
 *
 */
public interface StatusEventWithId {

    /** Returns the event associated with the ID.
     * @return the event.
     */
    StatusEvent getEvent();
    
    /** Get the id of the status event or the id of the status event's parent event.
     * @return the id.
     */
    StatusEventID getID();
    
    /** Get whether the ID is for a parent event or the event itself.
     * @return true if the ID is for the parent event, false otherwise.
     */
    boolean isParentId();
}
