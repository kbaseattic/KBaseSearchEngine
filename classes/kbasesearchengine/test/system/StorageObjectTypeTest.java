package kbasesearchengine.test.system;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class StorageObjectTypeTest {
    
    @Test
    public void equals() {
        EqualsVerifier.forClass(StorageObjectType.class).usingGetClass().verify();
    }

    @Test
    public void minimalConstructor() {
        final StorageObjectType sot = new StorageObjectType("ws", "foo");
        
        assertThat("incorrect storage code", sot.getStorageCode(), is("ws"));
        assertThat("incorrect storage code", sot.getType(), is("foo"));
        assertThat("incorrect storage code", sot.getVersion(), is(Optional.absent()));
        assertThat("incorrect storage code", sot.toString(),
                is("StorageObjectType [storageCode=ws, type=foo, version=Optional.absent()]"));
    }
    
    @Test
    public void maximalConstructor() {
        final StorageObjectType sot = new StorageObjectType("ws2", "foo2", 3);
        
        assertThat("incorrect storage code", sot.getStorageCode(), is("ws2"));
        assertThat("incorrect storage code", sot.getType(), is("foo2"));
        assertThat("incorrect storage code", sot.getVersion(), is(Optional.of(3)));
        assertThat("incorrect storage code", sot.toString(),
                is("StorageObjectType [storageCode=ws2, type=foo2, version=Optional.of(3)]"));
    }
    
    @Test
    public void minimalStaticBuilder() {
        final StorageObjectType sot = StorageObjectType.fromNullableVersion("whee", "bar", null);
        
        assertThat("incorrect storage code", sot.getStorageCode(), is("whee"));
        assertThat("incorrect storage code", sot.getType(), is("bar"));
        assertThat("incorrect storage code", sot.getVersion(), is(Optional.absent()));
        assertThat("incorrect storage code", sot.toString(),
                is("StorageObjectType [storageCode=whee, type=bar, version=Optional.absent()]"));
    }
    
    @Test
    public void maximalStaticBuilder() {
        final StorageObjectType sot = StorageObjectType.fromNullableVersion("whee2", "bar2", 3);
        
        assertThat("incorrect storage code", sot.getStorageCode(), is("whee2"));
        assertThat("incorrect storage code", sot.getType(), is("bar2"));
        assertThat("incorrect storage code", sot.getVersion(), is(Optional.of(3)));
        assertThat("incorrect storage code", sot.toString(),
                is("StorageObjectType [storageCode=whee2, type=bar2, version=Optional.of(3)]"));
    }
    
    @Test
    public void constructFail() {
        failConstruct(null, "bar",
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failConstruct(" \t   ", "bar",
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failConstruct("foo", null,
                new IllegalArgumentException("type cannot be null or the empty string"));
        failConstruct("foo", "   \n ",
                new IllegalArgumentException("type cannot be null or the empty string"));
        
        failConstruct(null, "bar", 1,
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failConstruct(" \t   ", "bar", 0,
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failConstruct("foo", null, 3,
                new IllegalArgumentException("type cannot be null or the empty string"));
        failConstruct("foo", "   \n ", 0,
                new IllegalArgumentException("type cannot be null or the empty string"));
        failConstruct("foo", "bar", -1,
                new IllegalArgumentException("version must be at least 0"));
    }
    
    @Test
    public void buildFail() {
        failBuild(null, "bar",
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failBuild(" \t   ", "bar",
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failBuild("foo", null,
                new IllegalArgumentException("type cannot be null or the empty string"));
        failBuild("foo", "   \n ",
                new IllegalArgumentException("type cannot be null or the empty string"));
        
        failBuild(null, "bar", 1,
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failBuild(" \t   ", "bar", 0,
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failBuild("foo", null, 3,
                new IllegalArgumentException("type cannot be null or the empty string"));
        failBuild("foo", "   \n ", 0,
                new IllegalArgumentException("type cannot be null or the empty string"));
        failBuild("foo", "bar", -1,
                new IllegalArgumentException("version must be at least 0"));
    }

    private void failConstruct(
            final String storageCode,
            final String type,
            final int version,
            final Exception expected) {
        try {
            new StorageObjectType(storageCode, type, version);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    private void failConstruct(
            final String storageCode,
            final String type,
            final Exception expected) {
        try {
            new StorageObjectType(storageCode, type);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    private void failBuild(
            final String storageCode,
            final String type,
            final Integer version,
            final Exception expected) {
        try {
            StorageObjectType.fromNullableVersion(storageCode, type, version);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    
    private void failBuild(
            final String storageCode,
            final String type,
            final Exception expected) {
        try {
            StorageObjectType.fromNullableVersion(storageCode, type, null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
