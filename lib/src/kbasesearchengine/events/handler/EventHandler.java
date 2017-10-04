package kbasesearchengine.events.handler;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbasesearchengine.common.GUID;
import kbasesearchengine.events.ObjectStatusEvent;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.exceptions.IndexingExceptionUncheckedWrapper;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingExceptionUncheckedWrapper;

/** An interface for handling search events. The interface abstracts away event source specific
 * operations.
 * Handlers are not guaranteed to be thread-safe.
 * @author gaprice@lbl.gov
 *
 */
public interface EventHandler {
    
    /** Get the storage code for the storage implementation with which this event handler is
     * associated.
     * @return the storage code.
     */
    String getStorageCode();

    /** Expands an event into multiple sub events. Returns the input event in a single item
     * Iterable if the event requires no expansion.
     * Note that the _id field of the sub events will be null since they have no storage system
     * records.
     * Also note that the {@link Iterable#iterator()} and  {@link Iterator#next()} functions may
     * throw {@link IndexingExceptionUncheckedWrapper} and
     * {@link RetriableIndexingExceptionUncheckedWrapper} exceptions, which should be unwrapped
     * and rethrown as soon as possible.
     * @param event the event to be expanded.
     * @return an Iterable of the of the events resulting from the expansion or the original
     * event if no expansion is necessary.
     * @throws IndexingException if an error occurred expanding the event.
     * @throws RetriableIndexingException if a retriable error occurred loading the data.
     */
    Iterable<ObjectStatusEvent> expand(ObjectStatusEvent event)
            throws IndexingException, RetriableIndexingException;
    
    /** The equivalent of {@link #load(List, Path) load(Arrays.asList(guid), tempfile)}
     * @param guid the globally unique ID of the source object to load.
     * @param file a file in which to store the object's data, which is expected to exist.
     * @return the source data.
     * @throws IndexingException if an error occurred loading the data.
     * @throws RetriableIndexingException if a retriable error occurred loading the data.
     */
    SourceData load(GUID guid, Path file) throws IndexingException, RetriableIndexingException;
    
    /** Load an object's data from a remote source. The target object may need to be specified
     * as a path from an accessible object. If the target object is accessible only one entry is
     * expected in the guids field.
     * @param guids the path to the object from an accessible object, or only the object's guid
     * if it is accessible.
     * @param file a file in which to store the object's data, which is expected to exist.
     * @return the object's source data.
     * @throws IndexingException if an error occurred loading the data.
     * @throws RetriableIndexingException if a retriable error occurred loading the data.
     */
    SourceData load(List<GUID> guids, Path file)
            throws IndexingException, RetriableIndexingException;

    /** Build a set of reference paths from a path to the current object and the references found
     * in the current object.
     * @param refpath a reference path to the current object.
     * @param refs a set of references in the current object.
     * @return a mapping of the references to their full path.
     */
    Map<String, String> buildReferencePaths(List<GUID> refpath, Set<String> refs);

    /** Resolve a set of references in an object.
     * @param refpath the reference path to the current object.
     * @param refs the references in the current object to process.
     * @return a set of resolved references.
     * @throws IndexingException if an error occurred resolving the references.
     * @throws RetriableIndexingException if a retriable error occurred resolving the references.
     */
    Set<ResolvedReference> resolveReferences(List<GUID> refpath, Set<String> refs)
            throws IndexingException, RetriableIndexingException;
}
