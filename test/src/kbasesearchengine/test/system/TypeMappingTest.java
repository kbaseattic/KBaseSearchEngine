package kbasesearchengine.test.system;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static kbasesearchengine.test.common.TestCommon.set;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.TypeMapping;
import kbasesearchengine.system.TypeMapping.Builder;
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
        
        b.withDefaultSearchType(new SearchObjectType("bar", 2));
        
        assertThat("incorrect buildReady", b.isBuildReady(), is(true));
        
        final TypeMapping tm = b.build();
        
        assertThat("incorrect storage code", tm.getStorageCode(), is("WS"));
        assertThat("incorrect storage type", tm.getStorageType(), is("foo"));
        assertThat("incorrect search types", tm.getSearchTypes(),
                is(set(new SearchObjectType("bar", 2))));
        assertThat("incorrect search types without version", tm.getSearchTypes(Optional.absent()),
                is(set(new SearchObjectType("bar", 2))));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(1)),
                is(set(new SearchObjectType("bar", 2))));
        assertThat("incorrect source info", tm.getSourceInfo(), is(Optional.absent()));
    }
    
    @Test
    public void minimalBuildWithVersion() {
        final TypeMapping.Builder b = TypeMapping.getBuilder("WS1", "foo1");
        
        assertThat("incorrect buildReady", b.isBuildReady(), is(false));
        
        b.withVersion(3, new SearchObjectType("baz", 1));
        
        assertThat("incorrect buildReady", b.isBuildReady(), is(true));
        
        final TypeMapping tm = b.build();
        
        assertThat("incorrect storage code", tm.getStorageCode(), is("WS1"));
        assertThat("incorrect storage type", tm.getStorageType(), is("foo1"));
        assertThat("incorrect search types", tm.getSearchTypes(),
                is(set(new SearchObjectType("baz", 1))));
        assertThat("incorrect search types without version", tm.getSearchTypes(Optional.absent()),
                is(set()));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(1)),
                is(set()));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(3)),
                is(set(new SearchObjectType("baz", 1))));
        assertThat("incorrect source info", tm.getSourceInfo(), is(Optional.absent()));
    }
    
    @Test
    public void maximalBuild() {
        final TypeMapping tm = TypeMapping.getBuilder("WS2", "foo2")
                .withDefaultSearchType(new SearchObjectType("foo", 16))
                .withDefaultSearchType(new SearchObjectType("bar", 3))
                .withDefaultSearchType(new SearchObjectType("foo", 26))
                .withVersion(0, new SearchObjectType("a0", 1))
                .withVersion(3, new SearchObjectType("baz", 6))
                .withVersion(4, new SearchObjectType("bat", 8))
                .withVersion(3, new SearchObjectType("bar", 10))
                .withVersion(3, new SearchObjectType("baz", 4))
                .withNullableSourceInfo("whee")
                .build();
        
        assertThat("incorrect storage code", tm.getStorageCode(), is("WS2"));
        assertThat("incorrect storage type", tm.getStorageType(), is("foo2"));
        assertThat("incorrect search types", tm.getSearchTypes(),
                is(set(new SearchObjectType("a0", 1), new SearchObjectType("foo", 26),
                        new SearchObjectType("bar", 3), new SearchObjectType("bar", 10),
                        new SearchObjectType("baz", 4), new SearchObjectType("bat", 8))));
        assertThat("incorrect search types without version", tm.getSearchTypes(Optional.absent()),
                is(set(new SearchObjectType("foo", 26), new SearchObjectType("bar", 3))));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(0)),
                is(set(new SearchObjectType("a0", 1))));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(1)),
                is(set(new SearchObjectType("foo", 26), new SearchObjectType("bar", 3))));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(3)),
                is(set(new SearchObjectType("baz", 4), new SearchObjectType("bar", 10))));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(4)),
                is(set(new SearchObjectType("bat", 8))));
        assertThat("incorrect source info", tm.getSourceInfo(), is(Optional.of("whee")));
    }
    
    @Test
    public void nullableAndWhitespaceBuild() {
        final TypeMapping tm = TypeMapping.getBuilder("WS3", "foo3")
                .withDefaultSearchType(new SearchObjectType("bar", 10))
                .withNullableSourceInfo("whee")
                .withNullableSourceInfo(null)
                .withNullableSourceInfo("  \n  \t   ")
                .build();
        
        assertThat("incorrect storage code", tm.getStorageCode(), is("WS3"));
        assertThat("incorrect storage type", tm.getStorageType(), is("foo3"));
        assertThat("incorrect search types", tm.getSearchTypes(),
                is(set(new SearchObjectType("bar", 10))));
        assertThat("incorrect search types without version", tm.getSearchTypes(Optional.absent()),
                is(set(new SearchObjectType("bar", 10))));
        assertThat("incorrect search types with version", tm.getSearchTypes(Optional.of(1)),
                is(set(new SearchObjectType("bar", 10))));
        assertThat("incorrect source info", tm.getSourceInfo(), is(Optional.absent()));
    }
    
    @Test
    public void immutable() {
        //test that returned sets are immutable.
        final TypeMapping tm = TypeMapping.getBuilder("a", "b")
                .withDefaultSearchType(new SearchObjectType("bar", 10))
                .withVersion(1, new SearchObjectType("whee", 1))
                .build();
        try {
            // test default types
            tm.getSearchTypes(Optional.of(2)).add(new SearchObjectType("bat", 10));
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // test passed
        }
        try {
            // test versioned types
            tm.getSearchTypes(Optional.of(1)).add(new SearchObjectType("bat", 10));
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
    public void withDefaultFail() {
        final Builder tm = TypeMapping.getBuilder("a", "b");
        try {
            tm.withDefaultSearchType(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("searchType"));
        }
    }
    
    @Test
    public void withVersionFail() {
        failWithVersion(-1, new SearchObjectType("t", 1),
                new IllegalArgumentException("version must be at least 0"));
        failWithVersion(0, null, new NullPointerException("searchType"));
    }
    
    private void failWithVersion(
            final int ver,
            final SearchObjectType type,
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
            TypeMapping.getBuilder("a", "b")
                    .withDefaultSearchType(new SearchObjectType("bar", 10)).build()
                    .getSearchTypes(version);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
