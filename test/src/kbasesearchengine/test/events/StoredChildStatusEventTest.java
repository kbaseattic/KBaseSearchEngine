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
    public void constructMinimal() {
        final StoredChildStatusEvent scse = StoredChildStatusEvent.getBuilder(
                new ChildStatusEvent(
                        StatusEvent.getBuilder("Code", Instant.ofEpochMilli(20000L),
                                StatusEventType.DELETE_ACCESS_GROUP).build(),
                        new StatusEventID("parent id")),
                new StatusEventID("my id"),
                Instant.ofEpochMilli(10000)).build();
        
        assertThat("incorrect event", scse.getChildEvent(), is(new ChildStatusEvent(
                        StatusEvent.getBuilder("Code", Instant.ofEpochMilli(20000L),
                                StatusEventType.DELETE_ACCESS_GROUP).build(),
                        new StatusEventID("parent id"))));
        assertThat("incorrect state", scse.getState(), is(StatusEventProcessingState.FAIL));
        assertThat("incorrect id", scse.getID(), is(new StatusEventID("my id")));
        assertThat("incorrect time", scse.getStoreTime(), is(Instant.ofEpochMilli(10000)));
    }
    
    @Test
    public void constructMaximal() {
        final StoredChildStatusEvent scse = StoredChildStatusEvent.getBuilder(
                new ChildStatusEvent(
                        StatusEvent.getBuilder("Code", Instant.ofEpochMilli(20000L),
                                StatusEventType.DELETE_ACCESS_GROUP).build(),
                        new StatusEventID("parent id")),
                new StatusEventID("my id"),
                Instant.ofEpochMilli(10000))
                .withState(StatusEventProcessingState.INDX)
                .build();
        
        assertThat("incorrect event", scse.getChildEvent(), is(new ChildStatusEvent(
                        StatusEvent.getBuilder("Code", Instant.ofEpochMilli(20000L),
                                StatusEventType.DELETE_ACCESS_GROUP).build(),
                        new StatusEventID("parent id"))));
        assertThat("incorrect state", scse.getState(), is(StatusEventProcessingState.INDX));
        assertThat("incorrect id", scse.getID(), is(new StatusEventID("my id")));
        assertThat("incorrect time", scse.getStoreTime(), is(Instant.ofEpochMilli(10000)));
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
    
    @Test
    public void startBuildFail() {
        final ChildStatusEvent c = new ChildStatusEvent(
                StatusEvent.getBuilder("Code", Instant.ofEpochMilli(20000L),
                        StatusEventType.DELETE_ACCESS_GROUP).build(),
                new StatusEventID("parent id"));
        final StatusEventID i = new StatusEventID("foo");
        final Instant t = Instant.ofEpochMilli(100000);
        failStartBuild(null, i, t, new NullPointerException("childEvent"));
        failStartBuild(c, null, t, new NullPointerException("id"));
        failStartBuild(c, i, null, new NullPointerException("storeTime"));
    }
    
    private void failStartBuild(
            final ChildStatusEvent event,
            final StatusEventID id,
            final Instant time,
            final Exception expected) {
        try {
            StoredChildStatusEvent.getBuilder(event, id, time);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void withStateFail() {
        failWithState(null, new NullPointerException("state"));
        failWithState(StatusEventProcessingState.UNPROC, new IllegalArgumentException(
                "Child events may only have terminal states"));
        failWithState(StatusEventProcessingState.READY, new IllegalArgumentException(
                "Child events may only have terminal states"));
        failWithState(StatusEventProcessingState.PROC, new IllegalArgumentException(
                "Child events may only have terminal states"));
    }
    
    private void failWithState(final StatusEventProcessingState state, final Exception expected) {
        final ChildStatusEvent c = new ChildStatusEvent(
                StatusEvent.getBuilder("Code", Instant.ofEpochMilli(20000L),
                        StatusEventType.DELETE_ACCESS_GROUP).build(),
                new StatusEventID("parent id"));
        try {
            StoredChildStatusEvent.getBuilder(
                    c, new StatusEventID("foo"), Instant.ofEpochMilli(10000))
                    .withState(state);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
}
