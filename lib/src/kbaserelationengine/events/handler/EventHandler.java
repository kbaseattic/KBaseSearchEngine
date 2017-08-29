package kbaserelationengine.events.handler;

import java.nio.file.Path;
import java.util.List;

import kbaserelationengine.common.GUID;
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
    
    
    /** The equivalent of {@link #load(List, Path) load(Arrays.asList(guid), tempfile)}
     * @param guid the globally unique ID of the source object to load.
     * @param file a file in which to store the object's data, which is expected to exist.
     * @return the source data.
     */
    SourceData load(GUID guid, Path file);
    
    /** Load an object's data from a remote source. The target object may need to be specified
     * as a path from an accessible object. If the target object is accessible only one entry is
     * expected in the guids field.
     * @param guids the path to the object from an accessible object, or only the object's guid
     * if it is accessible.
     * @param file a file in which to store the object's data, which is expected to exist.
     * @return the object's source data.
     */
    SourceData load(List<GUID> guids, Path file);
}
