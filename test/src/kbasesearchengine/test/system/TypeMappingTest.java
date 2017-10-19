package kbasesearchengine.test.system;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static kbasesearchengine.test.common.TestCommon.set;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.system.TypeMapping;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TypeMappingTest {

    @Test
    public void equals() {
        EqualsVerifier.forClass(TypeMapping.class).usingGetClass().verify();
    }
    
    @Test
    public void minimalBuildDefault() {
        final TypeMapping.Builder b = TypeMapping.getBuilder("WS", "foo");
        
        assertThat("incorrect buildReady", b.isBuildReady(), is(false));
        
        b.withNullableDefaultSearchType("bar");
        
        assertThat("incorrect buildReady", b.isBuildReady(), is(true));
        
        final TypeMapping tm = b.build();
        
        assertThat("incorrect storage code", tm.getStorageCode(), is("WS"));
        assertThat("incorrect storage type", tm.getStorageType(), is("foo"));
        assertThat("incorrect search types", tm.getSearchTypes(), is(set("bar")));
        assertThat("incorrect search types without version", tm.getSearchTypes(Optional.absent()),
                is(set("bar")));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(1)),
                is(set("bar")));
        assertThat("incorrect source info", tm.getSourceInfo(), is(Optional.absent()));
        assertThat("incorrect toString", tm.toString(),
                is("TypeMapping [storageCode=WS, storageType=foo, sourceInfo=Optional.absent(), " +
                        "defaultSearchTypes=[bar], versions={}]"));
    }
    
    @Test
    public void minimalBuildWithVersion() {
        final TypeMapping.Builder b = TypeMapping.getBuilder("WS1", "foo1");
        
        assertThat("incorrect buildReady", b.isBuildReady(), is(false));
        
        b.withVersion(3, "baz");
        
        assertThat("incorrect buildReady", b.isBuildReady(), is(true));
        
        final TypeMapping tm = b.build();
        
        assertThat("incorrect storage code", tm.getStorageCode(), is("WS1"));
        assertThat("incorrect storage type", tm.getStorageType(), is("foo1"));
        assertThat("incorrect search types", tm.getSearchTypes(), is(set("baz")));
        assertThat("incorrect search types without version", tm.getSearchTypes(Optional.absent()),
                is(set()));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(1)),
                is(set()));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(3)),
                is(set("baz")));
        assertThat("incorrect source info", tm.getSourceInfo(), is(Optional.absent()));
        assertThat("incorrect toString", tm.toString(),
                is("TypeMapping [storageCode=WS1, storageType=foo1, " +
                        "sourceInfo=Optional.absent(), defaultSearchTypes=[], " +
                        "versions={3=[baz]}]"));
    }
    
    @Test
    public void maximalBuild() {
        final TypeMapping tm = TypeMapping.getBuilder("WS2", "foo2")
                .withNullableDefaultSearchType("foo")
                .withNullableDefaultSearchType("bar")
                .withNullableDefaultSearchType("foo")
                .withVersion(0, "0")
                .withVersion(3, "baz")
                .withVersion(4, "bat")
                .withVersion(3, "bag")
                .withVersion(3, "baz")
                .withNullableSourceInfo("whee")
                .build();
        
        assertThat("incorrect storage code", tm.getStorageCode(), is("WS2"));
        assertThat("incorrect storage type", tm.getStorageType(), is("foo2"));
        assertThat("incorrect search types", tm.getSearchTypes(),
                is(set("0", "foo", "bar", "baz", "bat", "bag")));
        assertThat("incorrect search types without version", tm.getSearchTypes(Optional.absent()),
                is(set("foo", "bar")));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(0)),
                is(set("0")));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(1)),
                is(set("foo", "bar")));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(3)),
                is(set("baz", "bag")));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(4)),
                is(set("bat")));
        assertThat("incorrect source info", tm.getSourceInfo(), is(Optional.of("whee")));
        assertThat("incorrect toString", tm.toString(),
                is("TypeMapping [storageCode=WS2, storageType=foo2, " +
                        "sourceInfo=Optional.of(whee), defaultSearchTypes=[bar, foo], " +
                        "versions={0=[0], 3=[bag, baz], 4=[bat]}]"));
    }
    
    @Test
    public void nullableAndWhitespaceBuild() {
        final TypeMapping tm = TypeMapping.getBuilder("WS3", "foo3")
                .withNullableDefaultSearchType("foo")
                .withNullableDefaultSearchType(null)
                .withNullableDefaultSearchType("   \t  \n ")
                .withNullableSourceInfo("whee")
                .withNullableSourceInfo(null)
                .withNullableSourceInfo("  \n  \t   ")
                .build();
        
        assertThat("incorrect storage code", tm.getStorageCode(), is("WS3"));
        assertThat("incorrect storage type", tm.getStorageType(), is("foo3"));
        assertThat("incorrect search types", tm.getSearchTypes(),
                is(set("foo")));
        assertThat("incorrect search types without version", tm.getSearchTypes(Optional.absent()),
                is(set("foo")));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(1)),
                is(set("foo")));
        assertThat("incorrect source info", tm.getSourceInfo(), is(Optional.absent()));
        assertThat("incorrect toString", tm.toString(),
                is("TypeMapping [storageCode=WS3, storageType=foo3, " +
                        "sourceInfo=Optional.absent(), defaultSearchTypes=[foo], versions={}]"));
    }
    
    @Test
    public void immutable() {
        //test that returned sets are immutable.
        final TypeMapping tm = TypeMapping.getBuilder("a", "b")
                .withNullableDefaultSearchType("whee")
                .withVersion(1, "whoo")
                .build();
        try {
            // test default types
            tm.getSearchTypes(Optional.of(2)).add("baz");
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // test passed
        }
        try {
            // test versioned types
            tm.getSearchTypes(Optional.of(1)).add("baz");
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // test passed
        }
    }
    
    @Test
    public void getBuilderFail() {
        failGetBuilder(null, "b",
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failGetBuilder("  \t   \n ", "b",
                new IllegalArgumentException("storageCode cannot be null or the empty string"));
        failGetBuilder("a", null,
                new IllegalArgumentException("storageType cannot be null or the empty string"));
        failGetBuilder("a", "   \n  \t",
                new IllegalArgumentException("storageType cannot be null or the empty string"));
    }
    
    private void failGetBuilder(
            final String storageCode,
            final String type,
            final Exception expected) {
        try {
            TypeMapping.getBuilder(storageCode, type);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void withVersionFail() {
        failWithVersion(-1, "foo", new IllegalArgumentException("version must be at least 0"));
        failWithVersion(0, null,
                new IllegalArgumentException("searchType cannot be null or the empty string"));
        failWithVersion(0, "   \t ",
                new IllegalArgumentException("searchType cannot be null or the empty string"));
    }
    
    private void failWithVersion(
            final int ver,
            final String type,
            final Exception expected) {
        try {
            TypeMapping.getBuilder("a", "b").withVersion(ver, type);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void buildFail() {
        try {
            TypeMapping.getBuilder("a", "b").withNullableSourceInfo("c").build();
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(
                    got, new IllegalStateException("No type mappings were supplied"));
        }
    }
    
    @Test 
    public void getSearchTypesFail() {
        failGetSearchTypes(null, new NullPointerException("version"));
        failGetSearchTypes(Optional.of(-1),
                new IllegalArgumentException("version must be at least 0"));
    }
    
    private void failGetSearchTypes(final Optional<Integer> version, final Exception expected) {
        try {
            TypeMapping.getBuilder("a", "b").withNullableDefaultSearchType("c").build()
                .getSearchTypes(version);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
