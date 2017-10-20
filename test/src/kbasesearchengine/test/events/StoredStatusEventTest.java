package kbasesearchengine.test.events;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;

import org.junit.Test;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.test.common.TestCommon;

public class StoredStatusEventTest {

    @Test
    public void construct() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        
        final StoredStatusEvent sei = new StoredStatusEvent(
                se, new StatusEventID("foo"), StatusEventProcessingState.UNPROC);
        assertThat("incorrect id", sei.getId(), is(new StatusEventID("foo")));
        assertThat("incorrect event", sei.getEvent(), is(StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect state", sei.getState(), is(StatusEventProcessingState.UNPROC));
    }
    
    @Test
    public void constructFail() {
        final StatusEvent event = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        final StatusEventID id = new StatusEventID("foo");
        final StatusEventProcessingState state = StatusEventProcessingState.UNINDX;
        
        failConstruct(null, id, state, new NullPointerException("event"));
        failConstruct(event, null, state, new NullPointerException("id"));
        failConstruct(event, id, null, new NullPointerException("state"));
        
    }
    
    private void failConstruct(
            final StatusEvent event,
            final StatusEventID id,
            final StatusEventProcessingState state,
            final Exception expected) {
        try {
            new StoredStatusEvent(event, id, state);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
}
