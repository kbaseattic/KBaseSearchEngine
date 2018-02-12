package kbasesearchengine.events;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;

import com.google.common.base.Optional;

import kbasesearchengine.events.exceptions.NoSuchEventException;
import kbasesearchengine.tools.Utils;

/** An event queue on the level of an access group. Any access group level event blocks the 
 * entire queue once it is in the ready or processing state, while object level events can
 * run independently subject to the {@link ObjectEventQueue} rules.
 * 
 * The queue never changes the state of the {@link StoredStatusEvent}s submitted to it.
 * 
 * Note that the calling code is responsible for ensuring that IDs for events added to this queue
 * are unique.
 * If events with duplicate IDs are added to the queue unexpected behavior may result.
 * 
 * This class is not thread safe.
 * @author gaprice@lbl.gov
 *
 */
public class AccessGroupEventQueue {
    
    private static final Set<StatusEventType> ACCESS_GROUP_EVENTS = new HashSet<>(Arrays.asList(
            StatusEventType.COPY_ACCESS_GROUP, StatusEventType.DELETE_ACCESS_GROUP,
            StatusEventType.PUBLISH_ACCESS_GROUP, StatusEventType.UNPUBLISH_ACCESS_GROUP)); 

    private final Map<String, ObjectEventQueue> objectQueues = new HashMap<>();
    
    private final PriorityQueue<StoredStatusEvent> accessGroupQueue =
            new PriorityQueue<StoredStatusEvent>(new Comparator<StoredStatusEvent>() {
                
                @Override
                public int compare(final StoredStatusEvent e1, final StoredStatusEvent e2) {
                    return e1.getEvent().getTimestamp().compareTo(e2.getEvent().getTimestamp());
                }
            });
    /* if drain != null, ready and processing must = null. Object queues should be draining
     * events until the timestamp on the drain event.
     * if ready or processing != null, drain and the other field must be null. All object
     * queues must be drained at this point. 
     */
    private StoredStatusEvent drain = null; // AG event waiting for object queues to drain
    private StoredStatusEvent ready = null; // AG event ready for processing
    private StoredStatusEvent processing = null; // AG event processing
    private int size = 0; // keep a record of size rather than running through sub queues
    
    /* should maybe initialize with an access group id and reject events that don't match */
    
    /** Create a new, empty, queue. */
    public AccessGroupEventQueue() {}
    
    /** Create a new queue and initialize it with events in the
     * {@link StatusEventProcessingState.READY} and {@link StatusEventProcessingState.PROC} states.
     * 
     * The set of events cannot violate the rules that
     * <ul>
     * <li> Only one access group level event can be in the ready or processing state, and
     * if so no other events can be in the ready or processing state</li>
     * <li> Only one event per object ID can be in the ready or processing state</li>
     * </ul>
     * @param initialLoad the events to load into the queue.
     */
    public AccessGroupEventQueue(final Collection<StoredStatusEvent> initialLoad) {
        Utils.nonNull(initialLoad, "initialLoad");
        Utils.noNulls(initialLoad, "initialLoad has null entries");
        final Map<String, StoredStatusEvent> objects = new HashMap<>();
        
        for (final StoredStatusEvent e: initialLoad) {
            if (!e.getState().equals(StatusEventProcessingState.READY) &&
                    !e.getState().equals(StatusEventProcessingState.PROC)) {
                throw new IllegalArgumentException("Illegal initial event state: " + e.getState());
            }
            if (ACCESS_GROUP_EVENTS.contains(e.getEvent().getEventType())) {
                initAccessGroupEvent(e);
            } else {
                initObjectEvent(objects, e);
            }
        }
        if ((ready != null || processing != null) && initialLoad.size() > 1) {
            final StoredStatusEvent current = ready == null ? processing : ready;
            throw new IllegalArgumentException("If an access group level event is in the " +
                    "ready or processing state, no other events may be submitted.\n" +
                    "Access group event: " + current);
        }
        
        for (final String objID: objects.keySet()) {
            objectQueues.put(objID, new ObjectEventQueue(objects.get(objID)));
        }
        this.size = initialLoad.size();
    }

    private void initObjectEvent(
            final Map<String, StoredStatusEvent> objectMap,
            final StoredStatusEvent e) {
        final String objID = e.getEvent().getAccessGroupObjectId().get();
        if (objectMap.containsKey(objID)) {
            throw new IllegalArgumentException(String.format(
                    "Already contains an event for object ID %s.\n" +
                    "Existing event: %s\nNew event: %s", objID, objectMap.get(objID), e));
        }
        objectMap.put(objID, e);
    }

    private void initAccessGroupEvent(final StoredStatusEvent e) {
        if (ready != null || processing != null) {
            final StoredStatusEvent existing = ready == null ? processing : ready;
            throw new IllegalArgumentException(String.format(
                    "More than one access level event per access group ID is not allowed.\n" +
                    "Existing: %s\nNew event: %s", existing, e));
        }
        if (e.getState().equals(StatusEventProcessingState.READY)) {
            ready = e;
        } else {
            processing = e;
        }
    }
    
    /** Add an new {@link StatusEventProcessingState#UNPROC} event to the queue. Before any
     * loaded events are added to the ready or processing states, {@link #moveToReady()} must
     * be called.
     * @param event the event to add.
     */
    public void load(final StoredStatusEvent event) {
        Utils.nonNull(event, "event");
        if (!event.getState().equals(StatusEventProcessingState.UNPROC)) {
            throw new IllegalArgumentException("Illegal state for loading event: " +
                    event.getState());
        }
        if (ACCESS_GROUP_EVENTS.contains(event.getEvent().getEventType())) {
            accessGroupQueue.add(event);
        } else {
            //TODO CODE add checks to StatusEvent to ensure object event types always have object IDs
            final String objID = event.getEvent().getAccessGroupObjectId().get();
            if (!objectQueues.containsKey(objID)) {
                objectQueues.put(objID, new ObjectEventQueue());
            }
            objectQueues.get(objID).load(event);
        }
        size++;
    }
    
    /** Moves any events that are ready for processing based on the queue rules into the ready
     * state and returns them. Usually called after calling {@link #load(StoredStatusEvent)}
     * one or more times.
     * @return the events that were moved to the ready state.
     */
    public Set<StoredStatusEvent> moveToReady() {
        final Set<StoredStatusEvent> ret = new HashSet<>();
        if (ready != null || processing != null) {
            return Collections.unmodifiableSet(ret); // nothing to do, queue is blocked
        }
        if (drain == null) {
            drain = accessGroupQueue.poll();
        }
        final Instant drainTime = drain == null ? null : drain.getEvent().getTimestamp();
        boolean drained = true;
        for (final ObjectEventQueue oq: objectQueues.values()) {
            if (drainTime != null) {
                oq.drainAndBlockAt(drainTime);
            }
            addMoveToReady(oq, ret);
            drained = drained && !oq.isProcessingOrReady();
        }
        if (drained && drain != null) {
            ready = drain;
            drain = null;
            ret.add(ready);
        }
        return Collections.unmodifiableSet(ret);
    }
    
    private void addMoveToReady(final ObjectEventQueue oq, final Set<StoredStatusEvent> ret) {
        add(q -> q.moveToReady(), oq, ret);
    }
    
    private void addGetReady(final ObjectEventQueue oq, final Set<StoredStatusEvent> ret) {
        add(q -> q.getReadyForProcessing(), oq, ret);
    }
    
    private void addMoveToProcessing(final ObjectEventQueue oq, final Set<StoredStatusEvent> ret) {
        add(q -> q.moveReadyToProcessing(), oq, ret);
    }
    
    private void addGetProcessing(final ObjectEventQueue oq, final Set<StoredStatusEvent> ret) {
        add(q -> q.getProcessing(), oq, ret);
    }

    private void add(
            final Function<ObjectEventQueue, Optional<StoredStatusEvent>> func,
            final ObjectEventQueue oq,
            final Set<StoredStatusEvent> ret) {
        final Optional<StoredStatusEvent> e = func.apply(oq);
        if (e.isPresent()) {
            ret.add(e.get());
        }
    }

    /** Move any events in the ready state to the processing state and return the modified
     * events.
     * @return the events that were moved to the processing state.
     */
    public Set<StoredStatusEvent> moveReadyToProcessing() {
        final Set<StoredStatusEvent> ret = new HashSet<>();
        if (ready == null) {
            if (processing == null) {
                for (final ObjectEventQueue oq: objectQueues.values()) {
                    addMoveToProcessing(oq, ret);
                }
            }
            // if processing != null do nothing
        } else {
            ret.add(ready);
            processing = ready;
            ready = null;
        }
        return Collections.unmodifiableSet(ret);
    }
    
    /** Remove a processed event from the queue and update the queue state, potentially moving
     * events into the ready state.
     * This function implicitly calls {@link #moveToReady()}.
     * @param event the event to remove from the queue.
     * @throws NoSuchEventException if there is no event with the given ID in processing state.
     */
    public void setProcessingComplete(final StoredStatusEvent event) {
        Utils.nonNull(event, "event");
        if (processing != null) {
            if (event.getId().equals(processing.getId())) {
                processing = null;
                size--;
                for (final ObjectEventQueue oq: objectQueues.values()) {
                    oq.removeBlock();
                }
                moveToReady();
            } else {
                throw new NoSuchEventException(event);
            }
        } else {
            final String objID = event.getEvent().getAccessGroupObjectId().get();
            if (!objectQueues.containsKey(objID)) {
                throw new NoSuchEventException(event);
            }
            final ObjectEventQueue q = objectQueues.get(objID);
            q.setProcessingComplete(event);
            if (q.isEmpty()) {
                objectQueues.remove(objID);
            }
            size--;
            moveToReady();
        }
    }
    
    /** Get the number of events in the queue.
     * @return the queue size.
     */
    public int size() {
        return size;
    }
    
    public int sizeNoMemoization() {
        return (drain == null ? 0 : 1) + (ready == null ? 0 : 1) + (processing == null ? 0 : 1) +
                accessGroupQueue.size() + objectQueues.values().stream()
                .mapToInt(q -> q.size()).sum();
                
    }
    
    /** Return whether the queue is empty.
     * @return true if the queue is empty, false otherwise.
     */
    public boolean isEmpty() {
        return size == 0;
    }
    
    /** Get the set of events in the ready state.
     * @return the events that are ready for processing.
     */
    public Set<StoredStatusEvent> getReadyForProcessing() {
        final Set<StoredStatusEvent> ret = new HashSet<>();
        if (ready != null) {
            ret.add(ready);
        } else if (processing == null) {
            objectQueues.values().stream().forEach(q -> addGetReady(q, ret));
        }
        return Collections.unmodifiableSet(ret);
    }
    
    /** Get the set of events in the processing state.
     * @return the events that are in the processing state.
     */
    public Set<StoredStatusEvent> getProcessing() {
        final Set<StoredStatusEvent> ret = new HashSet<>();
        if (processing != null) {
            ret.add(processing);
        } else if (ready == null) {
            objectQueues.values().stream().forEach(q -> addGetProcessing(q, ret));
        }
        return Collections.unmodifiableSet(ret);
    }
}
