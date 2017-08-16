package kbaserelationengine.events.handler;

import kbaserelationengine.events.ObjectStatusEvent;

/** An interface for handling search events. The interface abstracts away event source specific
 * operations.
 * Handlers are not guaranteed to be thread-safe.
 * @author gaprice@lbl.gov
 *
 */
public interface EventHandler {

    /** Expands an event into multiple sub events. Returns the input event in a single item
     * Iterable if the event requires no expansion.
     * Note that the _id field of the sub events will be null since they have no storage system
     * records.
     * @param event the event to be expanded.
     * @return an Iterable of the of the events resulting from the expansion or the original
     * event if no expansion is necessary.
     */
    Iterable<ObjectStatusEvent> expand(ObjectStatusEvent event);
}
