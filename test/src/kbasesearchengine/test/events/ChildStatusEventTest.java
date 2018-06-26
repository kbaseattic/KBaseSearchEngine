package kbasesearchengine.test.events;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;

import org.junit.Test;

import kbasesearchengine.events.ChildStatusEvent;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class ChildStatusEventTest {
    
    @Test
    public void equals() {
        EqualsVerifier.forClass(ChildStatusEvent.class).usingGetClass().verify();
    }

    @Test
    public void construct() {
        final ChildStatusEvent e = new ChildStatusEvent(StatusEvent.getBuilder(
                "ws", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build(),
                new StatusEventID("whee"));
        
        assertThat("incorrect event", e.getEvent(), is(StatusEvent.getBuilder(
                "ws", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build()));
        assertThat("incorrect id", e.getID(), is(new StatusEventID("whee")));
        assertThat("incorrect is parent", e.isParentId(), is(true));
    }
    
    @Test
    public void constructFail() {
        final StatusEvent e = StatusEvent.getBuilder(
                "ws", Instant.ofEpochMilli(10000), StatusEventType.COPY_ACCESS_GROUP).build();
        failConstruct(null, new StatusEventID("foo"), new NullPointerException("event"));
        failConstruct(e, null, new NullPointerException("parentId"));
    }
    
    private void failConstruct(
            final StatusEvent event,
            final StatusEventID id,
            final Exception expected) {
        try {
            new ChildStatusEvent(event, id);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
        
    }
    
}
