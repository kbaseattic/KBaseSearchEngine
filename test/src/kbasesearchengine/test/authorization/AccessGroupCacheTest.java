package kbasesearchengine.test.authorization;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.base.Ticker;

import kbasesearchengine.authorization.AccessGroupCache;
import kbasesearchengine.authorization.AccessGroupProvider;
import kbasesearchengine.test.common.TestCommon;

public class AccessGroupCacheTest {
    
    @SuppressWarnings("unchecked")
    @Test
    public void standardConstructor() throws Exception {
        // test the non-test constructor
        final AccessGroupProvider wrapped = mock(AccessGroupProvider.class);
        final AccessGroupCache cache = new AccessGroupCache(wrapped, 1, 10000);
        
        when(wrapped.findAccessGroupIds("foo")).thenReturn(
                Arrays.asList(1), Arrays.asList(2, 3), null);
        
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(1)));
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(1)));
        Thread.sleep(1001);
        
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(2, 3)));
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(2, 3)));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void expiresEveryGet() throws Exception {
        // test that expires the value from the cache every time
        final AccessGroupProvider wrapped = mock(AccessGroupProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final AccessGroupCache cache = new AccessGroupCache(wrapped, 10, 10000, ticker);
        
        when(wrapped.findAccessGroupIds("foo")).thenReturn(
                Arrays.asList(1), Arrays.asList(2, 3), null);
        when(ticker.read()).thenReturn(0L, 10000000001L, 20000000001L);
        
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(1)));
        
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(2, 3)));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void cacheAccessOnGet() throws Exception {
        // test that the cache is accessed when available
        final AccessGroupProvider wrapped = mock(AccessGroupProvider.class);
        final Ticker ticker = mock(Ticker.class);
        final AccessGroupCache cache = new AccessGroupCache(wrapped, 10, 10000, ticker);
        
        when(wrapped.findAccessGroupIds("foo")).thenReturn(
                Arrays.asList(1), Arrays.asList(2, 3), null);
        when(ticker.read()).thenReturn(0L, 5000000001L, 10000000001L, 15000000001L, 20000000001L);
        
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(1)));
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(1)));
        
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(2, 3)));
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(2, 3)));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void expiresOnSize() throws Exception {
        // test that the cache expires values when it reaches the max size
        final AccessGroupProvider wrapped = mock(AccessGroupProvider.class);
        final AccessGroupCache cache = new AccessGroupCache(wrapped, 10000, 10);
        
        when(wrapped.findAccessGroupIds("foo")).thenReturn(
                Arrays.asList(1, 2, 3, 4, 5), Arrays.asList(2, 3), null);
        when(wrapped.findAccessGroupIds("bar")).thenReturn(
                Arrays.asList(6, 7, 8, 9), Arrays.asList(11, 12), null);
        when(wrapped.findAccessGroupIds("baz")).thenReturn(
                Arrays.asList(20, 21), Arrays.asList(22, 23), null);
        
        // load 9 access group IDs into a max 10 cache
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(1, 2, 3, 4, 5)));
        assertThat("incorrect access groups", cache.findAccessGroupIds("bar"),
                is(Arrays.asList(6, 7, 8, 9)));
        
        // check cache access
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(1, 2, 3, 4, 5)));
        assertThat("incorrect access groups", cache.findAccessGroupIds("bar"),
                is(Arrays.asList(6, 7, 8, 9)));
        
        // force an expiration based on cache size
        assertThat("incorrect access groups", cache.findAccessGroupIds("baz"),
                is(Arrays.asList(20, 21)));
        
        // check that the largest value was expired
        assertThat("incorrect access groups", cache.findAccessGroupIds("foo"),
                is(Arrays.asList(2, 3)));
        assertThat("incorrect access groups", cache.findAccessGroupIds("bar"),
                is(Arrays.asList(6, 7, 8, 9)));
        assertThat("incorrect access groups", cache.findAccessGroupIds("baz"),
                is(Arrays.asList(20, 21)));
    }
    
    @Test
    public void constructFail() throws Exception {
        final AccessGroupProvider wrapped = mock(AccessGroupProvider.class);
        failConstruct(null, 10, 10, new NullPointerException("provider"));
        failConstruct(wrapped, 0, 10,
                new IllegalArgumentException("cache lifetime must be at least one second"));
        failConstruct(wrapped, 10, 0,
                new IllegalArgumentException("cache size must be at least one"));
    }

    private void failConstruct(
            final AccessGroupProvider provider,
            final int lifetimeSec,
            final int size,
            final Exception exception) {
        try {
            new AccessGroupCache(provider, lifetimeSec, size);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, exception);
        }
    }
    
    @Test
    public void getFailIOE() throws Exception {
        final AccessGroupProvider wrapped = mock(AccessGroupProvider.class);
        final AccessGroupCache cache = new AccessGroupCache(wrapped, 10000, 10);
        
        when(wrapped.findAccessGroupIds("foo")).thenThrow(new IOException("well poop"));
        
        failFindAccessGroupIDs(cache, "foo", new IOException("well poop"));
    }
    
    private void failFindAccessGroupIDs(
            final AccessGroupCache cache,
            final String user,
            final Exception expected) {
        try {
            cache.findAccessGroupIds(user);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

}
