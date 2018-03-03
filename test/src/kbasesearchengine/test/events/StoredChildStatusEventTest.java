package kbasesearchengine.test.events;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;

import org.junit.Test;

import kbasesearchengine.events.ChildStatusEvent;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredChildStatusEvent;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class StoredChildStatusEventTest {

    @Test
    public void equals() {
        EqualsVerifier.forClass(StoredChildStatusEvent.class).usingGetClass().verify();
    }
    
    @Test
    public void construct() {
        final StoredChildStatusEvent scse = new StoredChildStatusEvent(
                new ChildStatusEvent(
                        StatusEvent.getBuilder("Code", Instant.ofEpochMilli(20000L),
                                StatusEventType.DELETE_ACCESS_GROUP).build(),
                        new StatusEventID("parent id")),
                StatusEventProcessingState.FAIL,
                new StatusEventID("my id"),
                Instant.ofEpochMilli(10000));
        
        assertThat("incorrect event", scse.getChildEvent(), is(new ChildStatusEvent(
                        StatusEvent.getBuilder("Code", Instant.ofEpochMilli(20000L),
                                StatusEventType.DELETE_ACCESS_GROUP).build(),
                        new StatusEventID("parent id"))));
        assertThat("incorrect state", scse.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect id", scse.getID(), is(new StatusEventID("my id")));
        assertThat("incorrect time", scse.getStoreTime(), is(Instant.ofEpochMilli(10000)));
    }
    
    @Test
    public void constructFail() {
        final ChildStatusEvent c = new ChildStatusEvent(
                StatusEvent.getBuilder("Code", Instant.ofEpochMilli(20000L),
                        StatusEventType.DELETE_ACCESS_GROUP).build(),
                new StatusEventID("parent id"));
        final StatusEventProcessingState s = StatusEventProcessingState.INDX;
        final StatusEventID i = new StatusEventID("foo");
        final Instant t = Instant.ofEpochMilli(100000);
        failConstruct(null, s, i, t, new NullPointerException("childEvent"));
        failConstruct(c, null, i, t, new NullPointerException("state"));
        failConstruct(c, s, null, t, new NullPointerException("id"));
        failConstruct(c, s, i, null, new NullPointerException("storeTime"));
        
        failConstruct(c, StatusEventProcessingState.UNPROC, i, t, new IllegalArgumentException(
                "Child events may only have terminal states"));
        failConstruct(c, StatusEventProcessingState.READY, i, t, new IllegalArgumentException(
                "Child events may only have terminal states"));
        failConstruct(c, StatusEventProcessingState.PROC, i, t, new IllegalArgumentException(
                "Child events may only have terminal states"));
    }
    
    @Test
    public void isAllowedState() {
        assertThat("incorrect result", StoredChildStatusEvent.isAllowedState(
                StatusEventProcessingState.UNPROC), is(false));
        
        assertThat("incorrect result", StoredChildStatusEvent.isAllowedState(
                StatusEventProcessingState.READY), is(false));
        
        assertThat("incorrect result", StoredChildStatusEvent.isAllowedState(
                StatusEventProcessingState.PROC), is(false));
        
        assertThat("incorrect result", StoredChildStatusEvent.isAllowedState(
                StatusEventProcessingState.INDX), is(true));
        
        assertThat("incorrect result", StoredChildStatusEvent.isAllowedState(
                StatusEventProcessingState.UNINDX), is(true));
        
        assertThat("incorrect result", StoredChildStatusEvent.isAllowedState(
                StatusEventProcessingState.FAIL), is(true));
    }
    
    private void failConstruct(
            final ChildStatusEvent event,
            final StatusEventProcessingState state,
            final StatusEventID id,
            final Instant time,
            final Exception expected) {
        try {
            new StoredChildStatusEvent(event, state, id, time);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
}
