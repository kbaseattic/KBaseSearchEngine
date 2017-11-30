package kbasesearchengine.test.events.handler;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;

import org.junit.Test;

import kbasesearchengine.common.GUID;
import kbasesearchengine.events.handler.ResolvedReference;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class ResolvedReferenceTest {
    
    @Test
    public void equals() {
        EqualsVerifier.forClass(ResolvedReference.class).usingGetClass().verify();
    }

    @Test
    public void construct() {
        final ResolvedReference rr = new ResolvedReference(
                new GUID("WS:1/2/1"), new GUID("KB:1/foo"),
                new StorageObjectType("WS", "bar"), Instant.ofEpochMilli(100000));
        
        assertThat("incorrect ref", rr.getReference(), is(new GUID("WS", 1, "2", 1, null, null)));
        assertThat("incorrect resolved ref", rr.getResolvedReference(),
                is(new GUID("KB", 1, "foo", null, null, null)));
        assertThat("incorrect type", rr.getType(), is(new StorageObjectType("WS", "bar")));
        assertThat("incorrect time", rr.getTimestamp(), is(Instant.ofEpochMilli(100000)));
        assertThat("incorrect toString", rr.toString(),
                is("ResolvedReference [reference=WS:1/2/1, resolvedReference=KB:1/foo, " +
                        "type=StorageObjectType [storageCode=WS, type=bar, " +
                        "version=Optional.absent()], timestamp=1970-01-01T00:01:40Z]"));
    }
    
    @Test
    public void constructFail() {
        final GUID ref = new GUID("WS:1/2/1");
        final GUID rref = new GUID("KB:1/foo");
        final StorageObjectType type = new StorageObjectType("WS", "bar");
        final Instant t = Instant.ofEpochMilli(10000);
        
        failConstruct(null, rref, type, t, new NullPointerException("reference"));
        failConstruct(ref, null, type, t, new NullPointerException("resolvedReference"));
        failConstruct(ref, rref, null, t, new NullPointerException("type"));
        failConstruct(ref, rref, type, null, new NullPointerException("time"));
    }
    
    private void failConstruct(
            final GUID reference,
            final GUID resolvedReference,
            final StorageObjectType type,
            final Instant time,
            final Exception expected) {
        try {
            new ResolvedReference(reference, resolvedReference, type, time);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
}
