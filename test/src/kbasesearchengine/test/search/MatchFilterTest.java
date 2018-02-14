package kbasesearchengine.test.search;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.HashMap;

import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import kbasesearchengine.search.MatchFilter;
import kbasesearchengine.search.MatchValue;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class MatchFilterTest {

    @Test
    public void equals() {
        EqualsVerifier.forClass(MatchFilter.class).usingGetClass().verify();
    }
    
    @Test
    public void buildMinimal() {
        final MatchFilter mf = MatchFilter.getBuilder().build();
        
        assertThat("incorrect full text", mf.getFullTextInAll(), is(Optional.absent()));
        assertThat("incorrect lookup", mf.getLookupInKeys(), is(new HashMap<>()));
        assertThat("incorrect obj name", mf.getObjectName(), is(Optional.absent()));
        assertThat("incorrect source tags", mf.getSourceTags(), is(set()));
        assertThat("incorrect timestamp", mf.getTimestamp(), is(Optional.absent()));
        assertThat("incorrect exclude sub", mf.isExcludeSubObjects(), is(false));
        assertThat("incorrect is blacklist", mf.isSourceTagsBlacklist(), is(false));
    }

    @Test
    public void buildNulls() {
        final MatchFilter mf = MatchFilter.getBuilder()
                .withNullableFullTextInAll(null)
                .withExcludeSubObjects(false)
                .withIsSourceTagsBlackList(false)
                .withNullableObjectName(null)
                .withNullableTimestamp(null)
                .build();
        
        assertThat("incorrect full text", mf.getFullTextInAll(), is(Optional.absent()));
        assertThat("incorrect lookup", mf.getLookupInKeys(), is(new HashMap<>()));
        assertThat("incorrect obj name", mf.getObjectName(), is(Optional.absent()));
        assertThat("incorrect source tags", mf.getSourceTags(), is(set()));
        assertThat("incorrect timestamp", mf.getTimestamp(), is(Optional.absent()));
        assertThat("incorrect exclude sub", mf.isExcludeSubObjects(), is(false));
        assertThat("incorrect is blacklist", mf.isSourceTagsBlacklist(), is(false));
    }
    
    @Test
    public void buildMaximal() {
        final MatchFilter mf = MatchFilter.getBuilder()
                .withNullableFullTextInAll("fulltext")
                .withExcludeSubObjects(true)
                .withIsSourceTagsBlackList(true)
                .withLookupInKey("foo", new MatchValue(1))
                .withLookupInKey("bar", "baz")
                .withNullableObjectName("myobject")
                .withNullableTimestamp(new MatchValue(70000, 80000))
                .withSourceTag("source1")
                .withSourceTag("source2")
                .build();
        
        assertThat("incorrect full text", mf.getFullTextInAll(), is(Optional.of("fulltext")));
        assertThat("incorrect lookup", mf.getLookupInKeys(), is(ImmutableMap.of(
                "foo", new MatchValue(1), "bar", new MatchValue("baz"))));
        assertThat("incorrect obj name", mf.getObjectName(), is(Optional.of("myobject")));
        assertThat("incorrect source tags", mf.getSourceTags(), is(set("source1", "source2")));
        assertThat("incorrect timestamp", mf.getTimestamp(),
                is(Optional.of(new MatchValue(70000, 80000))));
        assertThat("incorrect exclude sub", mf.isExcludeSubObjects(), is(true));
        assertThat("incorrect is blacklist", mf.isSourceTagsBlacklist(), is(true));
    }
    
    @Test
    public void lookupBuildFail() {
        failLookupBuild(null, "bar",
                new IllegalArgumentException("key cannot be null or whitespace only"));
        failLookupBuild("   \t   \n  ", "bar",
                new IllegalArgumentException("key cannot be null or whitespace only"));
        
        failLookupBuild(null, new MatchValue("bar"),
                new IllegalArgumentException("key cannot be null or whitespace only"));
        failLookupBuild("   \t   \n  ", new MatchValue("bar"),
                new IllegalArgumentException("key cannot be null or whitespace only"));
        
        failLookupBuild("foo", (String) null,
                new IllegalArgumentException("value cannot be null or whitespace only"));
        failLookupBuild("foo", "   \t   \n  ",
                new IllegalArgumentException("value cannot be null or whitespace only"));
        
        failLookupBuild("foo", (MatchValue) null,
                new NullPointerException("value"));
    }
    
    private void failLookupBuild(final String key, final String value, final Exception expected) {
        try {
            MatchFilter.getBuilder().withLookupInKey(key, value);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    private void failLookupBuild(
            final String key,
            final MatchValue value,
            final Exception expected) {
        try {
            MatchFilter.getBuilder().withLookupInKey(key, value);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void sourceTagsBuildFail() {
        failSourceTagsBuild(null,
                new IllegalArgumentException("sourceTag cannot be null or whitespace only"));
        failSourceTagsBuild("   \t  \n  ",
                new IllegalArgumentException("sourceTag cannot be null or whitespace only"));
    }
    
    private void failSourceTagsBuild(final String sourceTag, final Exception expected) {
        try {
            MatchFilter.getBuilder().withSourceTag(sourceTag);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void lookupKeysImmutable() {
        try {
            MatchFilter.getBuilder().withLookupInKey("foo", "bar").build()
                    .getLookupInKeys().put("whee", new MatchValue("whoo"));
            fail("expected exception");
        } catch (UnsupportedOperationException got) {
            //test passed
        }
    }
    
    @Test
    public void sourceTagsImmutable() {
        try {
            MatchFilter.getBuilder().withSourceTag("foo").build()
                    .getSourceTags().add("bar");
            fail("expected exception");
        } catch (UnsupportedOperationException got) {
            //test passed
        }
    }
}