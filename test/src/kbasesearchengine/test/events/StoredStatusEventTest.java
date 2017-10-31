package kbasesearchengine.test.events;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.test.common.TestCommon;

public class StoredStatusEventTest {

    @Test
    public void constructNoUpdater() {
        constructNoUpdater(null);
        constructNoUpdater("   \t   \n   ");
    }

    private void constructNoUpdater(final String updater) {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        
        final StoredStatusEvent sei = new StoredStatusEvent(se, new StatusEventID("foo"),
                StatusEventProcessingState.UNPROC, null, updater);
        assertThat("incorrect id", sei.getId(), is(new StatusEventID("foo")));
        assertThat("incorrect event", sei.getEvent(), is(StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect state", sei.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sei.getUpdater(), is(Optional.absent()));
        assertThat("incorrect update time", sei.getUpdateTime(), is(Optional.absent()));
    }
    
    @Test
    public void constructWithUpdater() {
        final StatusEvent se = StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build();
        
        final StoredStatusEvent sei = new StoredStatusEvent(se, new StatusEventID("foo"),
                StatusEventProcessingState.UNPROC, Instant.ofEpochMilli(20000), "foo");
        assertThat("incorrect id", sei.getId(), is(new StatusEventID("foo")));
        assertThat("incorrect event", sei.getEvent(), is(StatusEvent.getBuilder(
                "foo", Instant.ofEpochMilli(10000), StatusEventType.DELETE_ALL_VERSIONS).build()));
        assertThat("incorrect state", sei.getState(), is(StatusEventProcessingState.UNPROC));
        assertThat("incorrect updater", sei.getUpdater(), is(Optional.of("foo")));
        assertThat("incorrect update time", sei.getUpdateTime(),
                is(Optional.of(Instant.ofEpochMilli(20000))));
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
            // no exceptions can be triggered by update time or updater
            new StoredStatusEvent(event, id, state, null, null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
}
