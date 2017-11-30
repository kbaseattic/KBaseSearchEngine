package kbasesearchengine.events;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import kbasesearchengine.events.exceptions.NoSuchEventException;
import kbasesearchengine.tools.Utils;

/** A search status event queue. The purpose of the queue is to ensure that events that may
 * modify the same record are not processed concurrently, since currently the only search
 * storage implementation is elasticsearch.
 * 
 * Note that the calling code is responsible for ensuring that IDs for events added to this queue
 * are unique.
 * If events with duplicate IDs are added to the queue unexpected behavior may result.
 * 
 * Currently the queue can only process events with an access group id. Attempting to process
 * an event without an ID is an error.
 * 
 * This class is not thread safe.
 * @author gaprice@lbl.gov
 *
 */
public class EventQueue {
    
    private final Map<Integer, AccessGroupEventQueue> queues = new HashMap<>();
    
    private int size = 0; // record size rather than checking all queues
    
    /** Create an empty queue. */
    public EventQueue() {}
    
    /** Create a new queue and initialize it with events in the
     * {@link StatusEventProcessingState.READY} and {@link StatusEventProcessingState.PROC} states.
     * 
     * The events must abide by the rules set out in {@link AccessGroupEventQueue}.
     * @param initialLoad
     */
    public EventQueue(final List<StoredStatusEvent> initialLoad) {
        Utils.nonNull(initialLoad, "initialLoad");
        Utils.noNulls(initialLoad, "initialLoad has null entries");
        final Map<Integer, List<StoredStatusEvent>> events = new HashMap<>();
        for (final StoredStatusEvent e: initialLoad) {
            // if this is null, the access group should be ignored rather than putting all
            // events into the same AGQ -> need object event queues at this level. YAGNI for now.
            final int accgrpID = getAGID(e);
            if (!events.containsKey(accgrpID)) {
                events.put(accgrpID, new LinkedList<>());
            }
            events.get(accgrpID).add(e);
        }
        for (final int accgrpID: events.keySet()) {
            queues.put(accgrpID, new AccessGroupEventQueue(events.get(accgrpID)));
        }
        this.size = initialLoad.size();
    }
    
    /** Get the number of events in the queue.
     * @return the queue size.
     */
    public int size() {
        return size;
    }
    
    /** Return true if the queue is empty, false otherwise.
     * @return true if the queue is empty.
     */
    public boolean isEmpty() {
        return size() == 0;
    }
    
    private int getAGID(final StoredStatusEvent event) {
        Utils.nonNull(event, "event");
        return event.getEvent().getAccessGroupId().get();
    }
    
    /** Add an new {@link StatusEventProcessingState#UNPROC} event to the queue. Before any
     * loaded events are added to the ready or processing states, {@link #moveToReady()} must
     * be called.
     * @param event the event to add.
     */
    public void load(final StoredStatusEvent event) {
        final int accgrpID = getAGID(event);
        if (!queues.containsKey(accgrpID)) {
            queues.put(accgrpID, new AccessGroupEventQueue());
        }
        queues.get(accgrpID).load(event);
        size++;
    }
    
    /** Remove a processed event from the queue and update the queue state, potentially moving
     * events into the ready state.
     * This function implicitly calls {@link #moveToReady()}.
     * @param event the event to remove from the queue.
     * @throws NoSuchEventException if there is no event with the given ID in processing state.
     */
    public void setProcessingComplete(final StoredStatusEvent event) {
        final int id = getAGID(event);
        if (!queues.containsKey(id)) {
            throw new NoSuchEventException(event);
        }
        final AccessGroupEventQueue q = queues.get(id);
        q.setProcessingComplete(event);
        size--;
        if (q.isEmpty()) {
            queues.remove(id);
        }
    }
    
    /** Moves any events that are ready for processing based on the queue rules into the ready
     * state and returns them. Usually called after calling {@link #load(StoredStatusEvent)}
     * one or more times.
     * @return the events that were moved to the ready state.
     */
    public Set<StoredStatusEvent> moveToReady() {
        return gather(q -> q.moveToReady().stream());
    }
    
    /** Get the set of events in the ready state.
     * @return the events that are ready for processing.
     */
    public Set<StoredStatusEvent> getReadyForProcessing() {
        return gather(q -> q.getReadyForProcessing().stream());
    }
    
    /** Move any events in the ready state to the processing state and return the modified
     * events.
     * @return the events that were moved to the processing state.
     */
    public Set<StoredStatusEvent> moveReadyToProcessing() {
        return gather(q -> q.moveReadyToProcessing().stream());
    }
    
    /** Get the set of events in the processing state.
     * @return the events that are in the processing state.
     */
    public Set<StoredStatusEvent> getProcessing() {
        return gather(q -> q.getProcessing().stream());
    }
    
    private Set<StoredStatusEvent> gather(
            final Function<AccessGroupEventQueue, Stream<StoredStatusEvent>> func) {
        return Collections.unmodifiableSet(queues.values().stream()
                .flatMap(func).collect(Collectors.toSet()));
    }
}
