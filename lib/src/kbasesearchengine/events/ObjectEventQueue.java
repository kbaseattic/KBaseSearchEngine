package kbasesearchengine.events;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import com.google.common.base.Optional;

import kbasesearchengine.events.exceptions.NoSuchEventException;
import kbasesearchengine.tools.Utils;

/** An event queue on the level of an object. Object level events block the queue entirely,
 * while in contrast multiple version level events (e.g. {@link StatusEventType#NEW_VERSION})
 * can run at the same time.
 * 
 * Note that the calling code is responsible for ensuring that IDs for events added to this queue
 * are unique.
 * If events with duplicate IDs are added to the queue unexpected behavior may result.
 * 
 * This class is not thread safe.
 * @author gaprice@lbl.gov
 *
 */
public class ObjectEventQueue {
    
    /* Implementation notes:
     * the queue can have an unlimited number of NEW_VERSION events ready for processing or
     * processing at once, but only if no other type of event is ready for processing or
     * processing.
     * 
     * In other words, if the ready variable is not null, processing must be null and 
     * versionReady and versionProcessing must be empty. Same for processing except ready must
     * be null.
     * If versionReady or versionProcessing are not empty, both ready and processing must be null.
     * 
     * The intent is to ensure that no record in the search index is modified concurrently.
     */
    
    /* may want to initialize with an access group & object ID and ensure that all incoming
     * events match
     */
    
    private static final Set<StatusEventType> OBJ_LVL_EVENTS = new HashSet<>(Arrays.asList(
            StatusEventType.DELETE_ALL_VERSIONS, StatusEventType.NEW_ALL_VERSIONS,
            StatusEventType.PUBLISH_ALL_VERSIONS, StatusEventType.RENAME_ALL_VERSIONS,
            StatusEventType.UNDELETE_ALL_VERSIONS, StatusEventType.UNPUBLISH_ALL_VERSIONS));
    
    private static final Set<StatusEventType> OBJ_AND_VER_LVL_EVENTS = new HashSet<>(Arrays.asList(
            StatusEventType.NEW_VERSION));
    static {
        OBJ_AND_VER_LVL_EVENTS.addAll(OBJ_LVL_EVENTS);
    }
    
    private final PriorityQueue<StoredStatusEvent> queue = new PriorityQueue<StoredStatusEvent>(
            new Comparator<StoredStatusEvent>() {
                
                @Override
                public int compare(final StoredStatusEvent e1, final StoredStatusEvent e2) {
                    return e1.getEvent().getTimestamp().compareTo(e2.getEvent().getTimestamp());
                }
            });
    
    private StoredStatusEvent ready = null;
    private final Set<StoredStatusEvent> versionReady = new HashSet<>();
    private StoredStatusEvent processing = null;
    // maps id to event
    private final Map<StatusEventID, StoredStatusEvent> versionProcessing = new HashMap<>();
    private Instant blockTime = null;
    
    // could require an access group id and object id and reject any events that don't match
    
    /** Create a new, empty, queue. */
    public ObjectEventQueue() {}
    
    /** Create a new queue with an initial state consisting of {@link StatusEventType#NEW_VERSION}
     * events in the {@link StatusEventProcessingState#READY} and
     * {@link StatusEventProcessingState#PROC} states.
     * @param initialReady a list of events that are ready for processing.
     * @param initialProcessing a list of event that are being processed.
     */
    public ObjectEventQueue(
            final List<StoredStatusEvent> initialReady,
            final List<StoredStatusEvent> initialProcessing) {
        Utils.nonNull(initialReady, "initialReady");
        Utils.nonNull(initialProcessing, "initialProcessing");
        checkState(initialReady, StatusEventProcessingState.READY, StatusEventType.NEW_VERSION);
        checkState(initialProcessing, StatusEventProcessingState.PROC,
                StatusEventType.NEW_VERSION);
        versionReady.addAll(initialReady);
        initialProcessing.stream().forEach(e -> versionProcessing.put(e.getId(), e));
    }
    
    private void checkState(
            final List<StoredStatusEvent> events,
            final StatusEventProcessingState state,
            final StatusEventType type) {
        for (final StoredStatusEvent e: events) {
            if (!e.getState().equals(state)) {
                throw new IllegalArgumentException("Illegal initial event state: " + e.getState());
            }
            if (!e.getEvent().getEventType().equals(type)) {
                throw new IllegalArgumentException("Illegal initial event type: " +
                        e.getEvent().getEventType());
            }
        }
    }

    /** Create a new queue with an initial state consisting of an object-level, not version-level,
     * event. The event must be in either the {@link StatusEventProcessingState#READY} or
     * {@link StatusEventProcessingState#PROC} state.
     * @param initialEvent the initial event.
     */
    public ObjectEventQueue(final StoredStatusEvent initialEvent) {
        Utils.nonNull(initialEvent, "initialEvent");
        if (!isObjectLevelEvent(initialEvent)) {
            throw new IllegalArgumentException("Illegal initial event type: " +
                    initialEvent.getEvent().getEventType());
        }
        final StatusEventProcessingState state = initialEvent.getState();
        if (state.equals(StatusEventProcessingState.PROC)) {
            this.processing = initialEvent; 
        } else if (state.equals(StatusEventProcessingState.READY)){
            this.ready = initialEvent;
        } else {
            throw new IllegalArgumentException("Illegal initial event state: " + state);
        }
    }
    
    private boolean isObjectLevelEvent(final StoredStatusEvent initialEvent) {
        return OBJ_LVL_EVENTS.contains(initialEvent.getEvent().getEventType());
    }

    /** Add a new {@link StatusEventProcessingState#UNPROC} event to the queue.
     * Before any loaded events are added to the ready or processing states,
     * {@link #moveToReady()} must be called.
     * @param event the event to add.
     */
    public void load(final StoredStatusEvent event) {
        Utils.nonNull(event, "event");
        if (!event.getState().equals(StatusEventProcessingState.UNPROC)) {
            throw new IllegalArgumentException("Illegal state for loading event: " +
                    event.getState());
        }
        if (!OBJ_AND_VER_LVL_EVENTS.contains(event.getEvent().getEventType())) {
            throw new IllegalArgumentException("Illegal type for loading event: " +
                    event.getEvent().getEventType());
        }
        queue.add(event);
    }
    
    /** Get the set of events that the queue has determined are ready for processing.
     * @return the events ready for processing.
     */
    public Set<StoredStatusEvent> getReadyForProcessing() {
        final Set<StoredStatusEvent> ret;
        if (ready == null) {
            // if processing != null this'll return an empty list
            ret = new HashSet<>(versionReady);
        } else {
            ret = new HashSet<>(Arrays.asList(ready));
        }
        return Collections.unmodifiableSet(ret);
    }
    
    /** Get the set of events that the queue has determined are ready for processing, and set
     * those events as in process in the queue.
     * @return the events that were ready for processing and are now in the processing state.
     */
    public Set<StoredStatusEvent> moveReadyToProcessing() {
        final Set<StoredStatusEvent> ret;
        if (ready == null) {
            // if processing != null this'll return an empty list
            ret = new HashSet<>(versionReady);
            versionReady.stream().forEach(e -> versionProcessing.put(e.getId(), e));
            versionReady.clear();
        } else {
            ret = new HashSet<>(Arrays.asList(ready));
            processing = ready;
            ready = null;
        }
        return Collections.unmodifiableSet(ret);
    }
    
    /** Get the set of events in the processing state.
     * @return the events that are in the processing state.
     */
    public Set<StoredStatusEvent> getProcessing() {
        final Set<StoredStatusEvent> ret;
        if (processing == null) {
            // will be empty list if ready != null;
            ret = new HashSet<>(versionProcessing.values());
        } else {
            ret = new HashSet<>(Arrays.asList(processing));
        }
        return Collections.unmodifiableSet(ret);
    }
    
    /** Remove a processed event from the queue and update the queue state, potentially adding
     * more events to the ready state.
     * This function implicitly calls {@link #moveToReady()}.
     * @param event the event to remove.
     * @throws NoSuchEventException if there is no event with the given ID in the processing
     * state.
     */
    public void setProcessingComplete(final StoredStatusEvent event) {
        Utils.nonNull(event, "event");
        if (processing != null) {
            if (event.getId().equals(processing.getId())) {
                processing = null;
                moveToReady();
            } else {
                throw new NoSuchEventException(event);
            }
        } else {
            if (versionProcessing.containsKey(event.getId())) {
                versionProcessing.remove(event.getId());
                moveToReady();
            } else {
                throw new NoSuchEventException(event);
            }
        }
    }
    
    /** Returns true if at least one event is in the processing state.
     * @return true if the queue has events in the processing state.
     */
    public boolean isProcessing() {
        return !versionProcessing.isEmpty() || processing != null;
    }
    
    /** Returns true if at least one event is in the ready state.
     * @return true if the queue has events in the ready state.
     */
    public boolean hasReady() {
        return !versionReady.isEmpty() || ready != null;
    }
    
    /** Returns true if at least one event is in the ready or processing state.
     * @return true if the queue has events in the ready or processsing state.
     */
    public boolean isProcessingOrReady() {
        return isProcessing() || hasReady();
    }
    
    /** Returns the number of events in the queue.
     * @return the queue size.
     */
    public int size() {
        return queue.size() + versionReady.size() + versionProcessing.size() +
                (ready == null ? 0 : 1) + (processing == null ? 0 : 1);
    }
    
    /** Check if the queue is empty.
     * @return true if the queue is empty.
     */
    public boolean isEmpty() {
        return size() == 0;
    }
    
    /** Move events into the ready state if possible. Usually called after loading
     * ({@link #load(StoredStatusEvent)}) one or more events.
     * @return the events that have been moved into the ready state.
     */
    public Set<StoredStatusEvent> moveToReady() {
        final Set<StoredStatusEvent> ret = new HashSet<>();
        // if a object level event is ready for processing or processing, do nothing.
        // the queue is blocked.
        if (ready != null || processing != null) {
            return ret;
        }
        /* Either no events are ready/processing or only version level events are ready/processing.
         * Pull all the version level events off the front of the queue and put them in
         * the ready state.
         */
        /* should really check we're not running events for the same version at the same time,
         * but that should never happen since you only create a new version once.
         * might add admin tools to delete and reindex something but again, that should not
         * cause multiple events for the same version.
         */
        StoredStatusEvent next = queue.peek();
        while (next != null && isVersionLevelEvent(next) && !isBlockActive(next)) {
            versionReady.add(next);
            ret.add(next);
            queue.remove();
            next = queue.peek();
        }
        // if there's still an event left in the queue and no version level events are ready
        // for processing or processing, put the object level event in the ready state.
        // this blocks the queue for all other events.
        if (next != null && versionProcessing.isEmpty() && versionReady.isEmpty() &&
                !isBlockActive(next)) {
            ready = next;
            ret.add(next);
            queue.remove();
        }
        return Collections.unmodifiableSet(ret);
    }

    /** Check if an event is a version level, as opposed to an object or access group level,
     * event.
     * @param event the event to check.
     * @return true if the event is a version level event.
     */
    public static boolean isVersionLevelEvent(final StoredStatusEvent event) {
        Utils.nonNull(event, "event");
        return event.getEvent().getEventType().equals(StatusEventType.NEW_VERSION);
    }

    private boolean isBlockActive(final StoredStatusEvent next) {
        return blockTime != null && blockTime.isBefore(next.getEvent().getTimestamp());
    }
    
    /** Run all events until an event has an later date than blockTime, and then run no
     * more events until {@link #removeBlock()} is called. Any events in the ready or
     * processing state are not affected.
     * @param blockTime the time after which no more events should be set to the ready state.
     */
    public void drainAndBlockAt(final Instant blockTime) {
        Utils.nonNull(blockTime, "blockTime");
        this.blockTime = blockTime;
    }
    
    /** Get the block time set by {@link #drainAndBlockAt(Instant)}, if set.
     * @return the block time.
     */
    public Optional<Instant> getBlockTime() {
        return Optional.fromNullable(blockTime);
    }
    
    /** Removes the block set by {@link #drainAndBlockAt(Instant)}, but does not otherwise
     * alter the queue state.
     */
    public void removeBlock() {
        blockTime = null;
    }
}
