package kbasesearchengine.test.events.exceptions;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.time.Instant;

import org.junit.Test;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.NoSuchEventException;

public class ExceptionTest {

    // just testing NoSuchEventException for now since it's not a straight super(String)
    // constructor
    
    @Test
    public void noSuchEventException() {
        final StoredStatusEvent sse = StoredStatusEvent.getBuilder(StatusEvent.getBuilder(
                "sc", Instant.ofEpochMilli(1000), StatusEventType.COPY_ACCESS_GROUP).build(),
                new StatusEventID("foo"), StatusEventProcessingState.READY).build();
        
        final NoSuchEventException e = new NoSuchEventException(sse);
        assertThat("incorrect message", e.getMessage(), is("Event with ID foo not found"));
    }
    
}
