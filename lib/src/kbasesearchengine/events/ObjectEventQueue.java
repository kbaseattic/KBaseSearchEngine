package kbasesearchengine.events;

import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import com.google.common.base.Optional;

import kbasesearchengine.events.exceptions.NoSuchEventException;
import kbasesearchengine.tools.Utils;

/** An event queue on the level of an object.
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
public class ObjectEventQueue {
    
    /* may want to initialize with an access group & object ID and ensure that all incoming
     * events match
     */
    
    private static final Set<StatusEventType> OBJ_LVL_EVENTS = new HashSet<>(Arrays.asList(
            StatusEventType.DELETE_ALL_VERSIONS, StatusEventType.NEW_ALL_VERSIONS,
            StatusEventType.PUBLISH_ALL_VERSIONS, StatusEventType.RENAME_ALL_VERSIONS,
            StatusEventType.UNDELETE_ALL_VERSIONS, StatusEventType.UNPUBLISH_ALL_VERSIONS,
            StatusEventType.NEW_VERSION));
    
    private final PriorityQueue<StoredStatusEvent> queue = new PriorityQueue<StoredStatusEvent>(
            new Comparator<StoredStatusEvent>() {
                
                @Override
                public int compare(final StoredStatusEvent e1, final StoredStatusEvent e2) {
                    return e1.getEvent().getTimestamp().compareTo(e2.getEvent().getTimestamp());
                }
            });
    
    private StoredStatusEvent ready = null;
    private StoredStatusEvent processing = null;
    private Instant blockTime = null;
    private Set<StatusEventID> containedEvents = new HashSet<>();
    
    // could require an access group id and object id and reject any events that don't match
    
    /** Create a new, empty, queue. */
    public ObjectEventQueue() {}
    
    /** Create a new queue with an initial state consisting of an object-level
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
        containedEvents.add(initialEvent.getId());
    }
    
    private boolean isObjectLevelEvent(final StoredStatusEvent event) {
        return OBJ_LVL_EVENTS.contains(event.getEvent().getEventType());
    }

    /** Add a new {@link StatusEventProcessingState#UNPROC} event to the queue.
     * Events that already exist in the queue as determined by the event id are ignored.
     * Before any loaded events are added to the ready or processing states,
     * {@link #moveToReady()} must be called.
     * @param event the event to add.
     * @return true if the object was added to the queue, false if the event already existed in
     * the queue
     */
    public boolean load(final StoredStatusEvent event) {
        Utils.nonNull(event, "event");
        if (!event.getState().equals(StatusEventProcessingState.UNPROC)) {
            throw new IllegalArgumentException("Illegal state for loading event: " +
                    event.getState());
        }
        if (!isObjectLevelEvent(event)) {
            throw new IllegalArgumentException("Illegal type for loading event: " +
                    event.getEvent().getEventType());
        }
        if (!containedEvents.contains(event.getId())) {
            queue.add(event);
            containedEvents.add(event.getId());
            return true;
        }
        return false;
    }
    
    /** Get the event that the queue has determined are ready for processing, or absent if
     * no events are ready.
     * @return the event ready for processing.
     */
    public Optional<StoredStatusEvent> getReadyForProcessing() {
        return Optional.fromNullable(ready);
    }
    
    /** Get the event that the queue has determined is ready for processing, and set
     * that event as in process in the queue, or absent if no events are ready.
     * @return the event that was ready for processing and is now in the processing state.
     */
    public Optional<StoredStatusEvent> moveReadyToProcessing() {
        if (ready == null) {
            return Optional.absent();
        } else {
            processing = ready;
            ready = null;
            return Optional.of(processing);
        }
    }
    
    /** Get the event in the processing state, or absent if no events are in the processing
     * state.
     * @return the event that is in the processing state.
     */
    public Optional<StoredStatusEvent> getProcessing() {
        return Optional.fromNullable(processing);
    }
    
    /** Remove a processed event from the queue and update the queue state, potentially adding
     * an event to the ready state.
     * This function implicitly calls {@link #moveToReady()}.
     * @param event the event to remove.
     * @throws NoSuchEventException if there is no event with the given ID in the processing
     * state.
     */
    public void setProcessingComplete(final StoredStatusEvent event) {
        Utils.nonNull(event, "event");
        if (processing != null && event.getId().equals(processing.getId())) {
            containedEvents.remove(processing.getId());
            processing = null;
            moveToReady();
        } else {
            throw new NoSuchEventException(event);
        }
    }
    
    /** Returns true if an event is in the processing state.
     * @return true if the queue has an event in the processing state.
     */
    public boolean isProcessing() {
        return processing != null;
    }
    
    /** Returns true if an event is in the ready state.
     * @return true if the queue has an event in the ready state.
     */
    public boolean hasReady() {
        return ready != null;
    }
    
    /** Returns true if an event is in the ready or processing state.
     * @return true if the queue has an event in the ready or processing state.
     */
    public boolean isProcessingOrReady() {
        return isProcessing() || hasReady();
    }
    
    /** Returns the number of events in the queue.
     * @return the queue size.
     */
    public int size() {
        return queue.size() + (ready == null ? 0 : 1) + (processing == null ? 0 : 1);
    }
    
    /** Check if the queue is empty.
     * @return true if the queue is empty.
     */
    public boolean isEmpty() {
        return size() == 0;
    }
    
    /** Move an event into the ready state if possible, or absent if not.
     * Usually called after loading ({@link #load(StoredStatusEvent)}) one or more events.
     * @return the event that has been moved into the ready state.
     */
    public Optional<StoredStatusEvent> moveToReady() {
        // if a object level event is ready for processing or processing, do nothing.
        // the queue is blocked.
        if (ready != null || processing != null) {
            return Optional.absent();
        }
        final StoredStatusEvent next = queue.peek();
        if (next != null && !isBlockActive(next)) {
            ready = next;
            queue.remove();
        }
        return Optional.fromNullable(ready);
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
