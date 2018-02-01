package kbasesearchengine.authorization;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import workspace.WorkspaceClient;

/** A caching layer for access groups. Caches the results from the wrapped
 * {@link AccessGroupProvider} in memory for quick access.
 * @author gaprice@lbl.gov
 *
 */
public class AccessGroupCache implements AccessGroupProvider {
    
    private final LoadingCache<String, List<Integer>> cache;
    private final AccessGroupProvider provider;

    /** Create a cache.
     * @param provider the {@link AccessGroupProvider} whose results will be cached.
     * @param cacheLifeTimeInSec the number of seconds a set of access groups for a user should
     * remain in the cache.
     * @param cacheSizeInAccessGroups the maximum number of access groups, across all users, to
     * store in the cache.
     */
    public AccessGroupCache(
            final AccessGroupProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInAccessGroups) {
        this(provider, cacheLifeTimeInSec, cacheSizeInAccessGroups, Ticker.systemTicker());
    }
    
    /** Create a cache for testing purposes.
     * @param provider the {@link AccessGroupProvider} whose results will be cached.
     * @param cacheLifeTimeInSec the number of seconds an set of access groups for a user should
     * remain in the cache.
     * @param cacheSizeInAccessGroups the maximum number of access groups, across all users, to
     * store in the cache.
     * @param ticker a ticker implementation that allows controlling cache expiration with the
     * provided ticker rather than waiting for the system clock. This is exposed for testing
     * purposes.
     */
    public AccessGroupCache(
            final AccessGroupProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInAccessGroups,
            final Ticker ticker) {
        if (provider == null) {
            throw new NullPointerException("provider");
        }
        this.provider = provider;

        if (cacheLifeTimeInSec < 1) {
            throw new IllegalArgumentException("cache lifetime must be at least one second");
        }
        if (cacheSizeInAccessGroups < 1) {
            throw new IllegalArgumentException("cache size must be at least one");
        }
        cache = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(cacheLifeTimeInSec, TimeUnit.SECONDS)
                .maximumWeight(cacheSizeInAccessGroups)
                .weigher(new Weigher<String, List<Integer>>() {

                    @Override
                    public int weigh(String user, List<Integer> accessGroups) {
                        return accessGroups.size();
                    }
                    
                })
                .build(new CacheLoader<String, List<Integer>>() {

                    @Override
                    public List<Integer> load(String user) throws Exception {
                        return provider.findAccessGroupIds(user);
                    }

                });
    }

    @Override
    public WorkspaceClient getWorkspaceClient() {
        return provider.getWorkspaceClient();
    }

    @Override
    public List<Integer> findAccessGroupIds(final String user) throws IOException {
        try {
            return cache.get(user);
        } catch (ExecutionException e) {
            throw (IOException) e.getCause(); // IOE is the only checked exception
            // unchecked exceptions are wrapped in UncheckedExcecutionException
        }
    }

}
