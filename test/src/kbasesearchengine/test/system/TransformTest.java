package kbasesearchengine.test.system;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.system.LocationTransformType;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.Transform;
import kbasesearchengine.system.TransformType;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TransformTest {
    
    @Test
    public void equals() {
        EqualsVerifier.forClass(Transform.class).usingGetClass().verify();
    }

    @Test
    public void string() {
        final Transform t = Transform.string();
        assertThat("incorrect type", t.getType(), is(TransformType.string));
        assertEmptyFields(t);
    }

    private void assertEmptyFields(final Transform t) {
        assertThat("incorrect location", t.getLocation(), is(Optional.absent()));
        assertThat("incorrect targetKey", t.getTargetKey(), is(Optional.absent()));
        assertThat("incorrect targetObjectType", t.getTargetObjectType(), is(Optional.absent()));
        assertThat("incorrect subObjectIDKey", t.getSubobjectIdKey(), is(Optional.absent()));
    }
    
    @Test
    public void integer() {
        final Transform t = Transform.integer();
        assertThat("incorrect type", t.getType(), is(TransformType.integer));
        assertEmptyFields(t);
    }
    
    @Test
    public void value() {
        final Transform t = Transform.values();
        assertThat("incorrect type", t.getType(), is(TransformType.values));
        assertEmptyFields(t);
    }
    
    @Test
    public void location() {
        final Transform t = Transform.location(LocationTransformType.contig_id);
        assertThat("incorrect type", t.getType(), is(TransformType.location));
        assertThat("incorrect location", t.getLocation(),
                is(Optional.of(LocationTransformType.contig_id)));
        assertThat("incorrect targetKey", t.getTargetKey(), is(Optional.absent()));
        assertThat("incorrect targetObjectType", t.getTargetObjectType(), is(Optional.absent()));
        assertThat("incorrect subObjectIDKey", t.getSubobjectIdKey(), is(Optional.absent()));
    
    }
    
    @Test
    public void locationFail() {
        try {
            Transform.location(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("location"));
        }
    }
    
    @Test
    public void lookup() {
        final Transform t = Transform.lookup("foo");
        assertThat("incorrect type", t.getType(), is(TransformType.lookup));
        assertThat("incorrect location", t.getLocation(), is(Optional.absent()));
        assertThat("incorrect targetKey", t.getTargetKey(), is(Optional.of("foo")));
        assertThat("incorrect targetObjectType", t.getTargetObjectType(), is(Optional.absent()));
        assertThat("incorrect subObjectIDKey", t.getSubobjectIdKey(), is(Optional.absent()));
    }
    
    @Test
    public void lookupFail() {
        failLookup(null, new IllegalArgumentException("targetKey cannot be null or whitespace"));
        failLookup("   \t   \n ",
                new IllegalArgumentException("targetKey cannot be null or whitespace"));
    }
    
    private void failLookup(final String lookup, final Exception expected) {
        try {
            Transform.lookup(lookup);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void guid() {
        final Transform t = Transform.guid(new SearchObjectType("foo", 1));
        assertThat("incorrect type", t.getType(), is(TransformType.guid));
        assertThat("incorrect location", t.getLocation(), is(Optional.absent()));
        assertThat("incorrect targetKey", t.getTargetKey(), is(Optional.absent()));
        assertThat("incorrect targetObjectType", t.getTargetObjectType(),
                is(Optional.of(new SearchObjectType("foo", 1))));
        assertThat("incorrect subObjectIDKey", t.getSubobjectIdKey(), is(Optional.absent()));
    }
    
    @Test
    public void guidWithSubobject() {
        final Transform t = Transform.guid(new SearchObjectType("foo", 1), "bar");
        assertThat("incorrect type", t.getType(), is(TransformType.guid));
        assertThat("incorrect location", t.getLocation(), is(Optional.absent()));
        assertThat("incorrect targetKey", t.getTargetKey(), is(Optional.absent()));
        assertThat("incorrect targetObjectType", t.getTargetObjectType(),
                is(Optional.of(new SearchObjectType("foo", 1))));
        assertThat("incorrect subObjectIDKey", t.getSubobjectIdKey(), is(Optional.of("bar")));
    }
    
    @Test
    public void guidFail() {
        failGuid(null,
                new NullPointerException("targetObjectType"));
        failGuid(null, "foo",
                new NullPointerException("targetObjectType"));
        failGuid(new SearchObjectType("foo", 1), null,
                new IllegalArgumentException("subObjectIDKey cannot be null or whitespace"));
        failGuid(new SearchObjectType("foo", 1), "  \n   \t  ",
                new IllegalArgumentException("subObjectIDKey cannot be null or whitespace"));
    }
    
    private void failGuid(
            final SearchObjectType targetObjectType,
            final Exception expected) {
        try {
            Transform.guid(targetObjectType);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    private void failGuid(
            final SearchObjectType targetObjectType,
            final String subObjectIDKey,
            final Exception expected) {
        try {
            Transform.guid(targetObjectType, subObjectIDKey);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void unknownValues() {
        final Transform t = Transform.unknown("values", "foo", "bar", null, "baz");
        assertThat("incorrect type", t.getType(), is(TransformType.values));
        assertEmptyFields(t);
    }
    
    @Test
    public void unknownString() {
        final Transform t = Transform.unknown("string", "foo", "bar", 1, "baz");
        assertThat("incorrect type", t.getType(), is(TransformType.string));
        assertEmptyFields(t);
    }
    
    @Test
    public void unknownInteger() {
        final Transform t = Transform.unknown("integer", "foo", "bar", -23, "baz");
        assertThat("incorrect type", t.getType(), is(TransformType.integer));
        assertEmptyFields(t);
    }
    
    @Test
    public void unknownLocation() {
        final Transform t = Transform.unknown("location", "start", "bar", null, "baz");
        assertThat("incorrect type", t.getType(), is(TransformType.location));
        assertThat("incorrect location", t.getLocation(),
                is(Optional.of(LocationTransformType.start)));
        assertThat("incorrect targetKey", t.getTargetKey(), is(Optional.absent()));
        assertThat("incorrect targetObjectType", t.getTargetObjectType(), is(Optional.absent()));
        assertThat("incorrect subObjectIDKey", t.getSubobjectIdKey(), is(Optional.absent()));
    }
    
    @Test
    public void unknownLookup() {
        final Transform t = Transform.unknown("lookup", "whee", "bar", null, "baz");
        assertThat("incorrect type", t.getType(), is(TransformType.lookup));
        assertThat("incorrect location", t.getLocation(), is(Optional.absent()));
        assertThat("incorrect targetKey", t.getTargetKey(), is(Optional.of("whee")));
        assertThat("incorrect targetObjectType", t.getTargetObjectType(), is(Optional.absent()));
        assertThat("incorrect subObjectIDKey", t.getSubobjectIdKey(), is(Optional.absent()));
    }
    
    @Test
    public void unknownGuid() {
        unknownGuid(null);
        unknownGuid("  \t  \n  ");
    }

    private void unknownGuid(final String subObjectIDKey) {
        final Transform t = Transform.unknown("guid", "foo", "whoo", 1, subObjectIDKey);
        assertThat("incorrect type", t.getType(), is(TransformType.guid));
        assertThat("incorrect location", t.getLocation(), is(Optional.absent()));
        assertThat("incorrect targetKey", t.getTargetKey(), is(Optional.absent()));
        assertThat("incorrect targetObjectType", t.getTargetObjectType(),
                is(Optional.of(new SearchObjectType("whoo", 1))));
        assertThat("incorrect subObjectIDKey", t.getSubobjectIdKey(), is(Optional.absent()));
    }
    
    @Test
    public void unknownGuidWithSubObjectIDKey() {
        final Transform t = Transform.unknown("guid", "foo", "whoo", 6, "whoop");
        assertThat("incorrect type", t.getType(), is(TransformType.guid));
        assertThat("incorrect location", t.getLocation(), is(Optional.absent()));
        assertThat("incorrect targetKey", t.getTargetKey(), is(Optional.absent()));
        assertThat("incorrect targetObjectType", t.getTargetObjectType(),
                is(Optional.of(new SearchObjectType("whoo", 6))));
        assertThat("incorrect subObjectIDKey", t.getSubobjectIdKey(), is(Optional.of("whoop")));
    }
    
    @Test
    public void unknownFailTransformType() {
        failUnknown(null, null, null, null, null,
                new IllegalArgumentException("transform cannot be null or whitespace"));
        failUnknown("   \t  \n  ", null, null, null, null,
                new IllegalArgumentException("transform cannot be null or whitespace"));
        failUnknown("foo", null, null, null, null,
                new IllegalArgumentException("Illegal transform type: foo"));
    }
    
    @Test
    public void unknownFailLocation() {
        failUnknown("location", null, "foo", null, "bar", new IllegalArgumentException(
                "location transform location type cannot be null or whitespace"));
        failUnknown("location", "   \t   \n  ", "foo", null, "bar", new IllegalArgumentException(
                "location transform location type cannot be null or whitespace"));
        failUnknown("location", "badloc", "foo", 1, "bar", new IllegalArgumentException(
                "Illegal transform location: badloc"));
    }
    
    @Test
    public void unknownFailLookup() {
        failUnknown("lookup", null, "foo", null, "bar", new IllegalArgumentException(
                "lookup transform target key cannot be null or whitespace"));
        failUnknown("lookup", "   \t   \n  ", "foo", 2, "bar", new IllegalArgumentException(
                "lookup transform target key cannot be null or whitespace"));
    }
    
    @Test
    public void unknownFailGuid() {
        failUnknown("guid", "bleah", null, 1, "bar", new IllegalArgumentException(
                "targetObjectType cannot be null or whitespace"));
        failUnknown("guid", "bleah", "  \t   \n  ", 1, "bar", new IllegalArgumentException(
                "targetObjectType cannot be null or whitespace"));
        failUnknown("guid", "bleah", "type", null, "bar", new NullPointerException(
                "targetObjectTypeVersion cannot be null"));
    }
    
    private void failUnknown(
            final String transform,
            final String locationOrTargetKey,
            final String targetObjectType,
            final Integer targetObjectTypeVer,
            final String subObjectIDKey,
            final Exception expected) {
        try {
            Transform.unknown(transform, locationOrTargetKey,
                    targetObjectType, targetObjectTypeVer, subObjectIDKey);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
