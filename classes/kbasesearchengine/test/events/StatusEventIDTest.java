package kbasesearchengine.test.events;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class StatusEventIDTest {

    @Test
    public void construct() {
        final StatusEventID id = new StatusEventID("foo");
        assertThat("incorrect id", id.getId(), is("foo"));
        assertThat("incorrect toString", id.toString(), is("StatusEventID [id=foo]"));
    }
    
    @Test
    public void equals() {
        EqualsVerifier.forClass(StatusEventID.class).usingGetClass().verify();
    }
    
    @Test
    public void constructFail() {
        failConstruct(null, new IllegalArgumentException("id cannot be null or the empty string"));
        failConstruct("   \t \n",
                new IllegalArgumentException("id cannot be null or the empty string"));
    }

    private void failConstruct(final String id, final Exception expected) {
        try {
            new StatusEventID(id);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
