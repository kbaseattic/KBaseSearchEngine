package kbasesearchengine.events;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import kbasesearchengine.events.exceptions.NoSuchEventException;
import kbasesearchengine.tools.Utils;

/** An event queue on the level of an access group. Any access group level event blocks the 
 * entire queue once it is in the ready or processing state, while object level events can
 * run independently subject to the {@link ObjectEventQueue} rules.
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
            StatusEventType.PUBLISH_ACCESS_GROUP)); 

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
     * if ready or processing != null, drain and the non null field must be null. All object
     * queues must be drained at this point. 
     */
    private StoredStatusEvent drain = null; // AG event waiting for object queues to drain
    private StoredStatusEvent ready = null; // AG event ready for processing
    private StoredStatusEvent processing = null; // AG event processing
    private int size = 0; // keep a record of size rather than running through sub queues
    
    /* should maybe initialize with an acccess group id and reject events that don't match */
    
    /** Create a new, empty, queue. */
    public AccessGroupEventQueue() {}
    
    /** Create a new queue and initialize it with events in the
     * {@link StatusEventProcessingState.READY} and {@link StatusEventProcessingState.PROC} states.
     * 
     * The set of events cannot violate the rules that
     * <ul>
     * <li> Only one access group level event can be in the ready or processing state, and
     * if so no other events can be in the ready or processing state</li>
     * <li> Only one object level event per object ID can be in the ready or processing state, and
     * if so no version level events for that objectID can be in the ready or processing state</li>
     * </ul>
     * @param initialLoad the events to load into the queue.
     */
    public AccessGroupEventQueue(final Collection<StoredStatusEvent> initialLoad) {
        Utils.nonNull(initialLoad, "initialLoad");
        Utils.noNulls(initialLoad, "initialLoad has null entries");
        final Map<String, StoredStatusEvent> objects = new HashMap<>();
        final Map<String, List<StoredStatusEvent>> objectVersionReady = new HashMap<>();
        final Map<String, List<StoredStatusEvent>> objectVersionProcessing = new HashMap<>();
        
        for (final StoredStatusEvent e: initialLoad) {
            if (!e.getState().equals(StatusEventProcessingState.READY) ||
                    !e.getState().equals(StatusEventProcessingState.PROC)) {
                throw new IllegalArgumentException("Illegal initial event state: " + e.getState());
            }
            if (ACCESS_GROUP_EVENTS.contains(e)) {
                initAccessGroupEvent(e);
            } else if (ObjectEventQueue.isVersionLevelEvent(e)) {
                initVersionEvent(objectVersionReady, objectVersionProcessing, e);
            } else {
                initObjectEvent(objects, e);
            }
        }
        final Set<String> allObjIDs = new HashSet<>();
        allObjIDs.addAll(objects.keySet());
        allObjIDs.addAll(objectVersionReady.keySet());
        allObjIDs.addAll(objectVersionProcessing.keySet());
        
        for (final String objID: allObjIDs) {
            if ((objectVersionReady.containsKey(objID) ||
                    objectVersionProcessing.containsKey(objID)) &&
                    objects.containsKey(objID)) {
                throw new IllegalArgumentException("Cannot submit both object level events and " +
                        "version level events for object ID " + objID);
            }
            if (objects.containsKey(objID)) {
                objectQueues.put(objID, new ObjectEventQueue(objects.get(objID)));
            } else {
                objectQueues.put(objID, new ObjectEventQueue(
                        objectVersionReady.getOrDefault(objID, Collections.emptyList()),
                        objectVersionProcessing.getOrDefault(objID, Collections.emptyList())));
            }
            
        }
    }

    private void initObjectEvent(
            final Map<String, StoredStatusEvent> objectMap,
            final StoredStatusEvent e) {
        final String objID = e.getEvent().getAccessGroupObjectId().get();
        if (objectMap.containsKey(objID)) {
            throw new IllegalArgumentException(
                    "Already contains an event for object ID " + objID);
        }
        objectMap.put(objID, e);
    }

    private void initVersionEvent(
            final Map<String, List<StoredStatusEvent>> objectVersionReady,
            final Map<String, List<StoredStatusEvent>> objectVersionProcessing,
            final StoredStatusEvent e) {
        final String objID = e.getEvent().getAccessGroupObjectId().get();
        if (e.getState().equals(StatusEventProcessingState.READY)) {
            addEvent(objectVersionReady, objID, e);
        } else {
            addEvent(objectVersionProcessing, objID, e);
        }
    }

    private void initAccessGroupEvent(final StoredStatusEvent e) {
        if (ready != null || processing != null) {
            throw new IllegalArgumentException(
                    "More than one access level event is not allowed");
        }
        if (e.getState().equals(StatusEventProcessingState.READY)) {
            ready = e;
        } else {
            processing = e;
        }
    }

    private void addEvent(
            final Map<String, List<StoredStatusEvent>> objectVersionProcessing,
            final String objID,
            final StoredStatusEvent e) {
        if (!objectVersionProcessing.containsKey(objID)) {
            objectVersionProcessing.put(objID, new LinkedList<>());
        }
        objectVersionProcessing.get(objID).add(e);
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
            return ret; // nothing to do, queue is blocked
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
            ret.addAll(oq.moveToReady());
            drained = drained && !oq.isProcessingOrReady();
        }
        if (drained && drain != null) {
            ready = drain;
            drain = null;
            ret.add(ready);
        }
        return Collections.unmodifiableSet(ret);
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
                    ret.addAll(oq.moveReadyToProcessing());
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
            objectQueues.values().stream().forEach(q -> ret.addAll(q.getReadyForProcessing()));
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
            objectQueues.values().stream().forEach(q -> ret.addAll(q.getProcessing()));
        }
        return Collections.unmodifiableSet(ret);
    }
}
