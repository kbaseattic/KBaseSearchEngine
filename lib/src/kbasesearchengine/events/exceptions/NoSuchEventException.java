package kbasesearchengine.events.exceptions;

import kbasesearchengine.events.StoredStatusEvent;

/** Thrown when an attempt at accessing an event that does not exist is made.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class NoSuchEventException extends RuntimeException {
    
    public NoSuchEventException(final StoredStatusEvent event) {
        super(String.format("No event with ID %s is in a processing state", event));
    }
}