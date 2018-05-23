package kbasesearchengine.main;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kbasesearchengine.tools.Utils;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import us.kbase.common.service.Tuple9;

/** A caching layer for workspace and narrative info. Caches the results in memory for quick access.
 * @author gaprice@lbl.gov
 * @author ganapathy@bnl.gov
 */

public class WorkspaceInfoCache implements WorkspaceInfoProvider {

    private final LoadingCache<Long, Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>>> cache;

    /** Create a cache.
     * @param provider the {@link NarrativeInfoProvider} whose results will be cached.
     * @param cacheLifeTimeInSec the number of seconds a set of narrative info for a user should
     * remain in the cache.
     * @param cacheSizeInNarrativeInfo the maximum number of narrative info, across all users, to
     * store in the cache.
     */
    public WorkspaceInfoCache(
            final WorkspaceInfoProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInWorkspaceInfo) {
        this(provider, cacheLifeTimeInSec, cacheSizeInWorkspaceInfo, Ticker.systemTicker());
    }

    /** Create a cache for testing purposes.
     * @param cacheLifeTimeInSec the number of seconds an set of narrative info for a user should
     * remain in the cache.
     * @param cacheSizeInNarrativeInfo the maximum number of narrative info, across all users, to
     * store in the cache.
     * @param ticker a ticker implementation that allows controlling cache expiration with the
     * provided ticker rather than waiting for the system clock. This is exposed for testing
     * purposes.
     */
    public WorkspaceInfoCache(
            final WorkspaceInfoProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInWorkspaceInfo,
            final Ticker ticker) {

        Utils.nonNull(provider, "provider");

        if (cacheLifeTimeInSec < 1) {
            throw new IllegalArgumentException("cache lifetime must be at least one second");
        }
        if (cacheSizeInWorkspaceInfo < 1) {
            throw new IllegalArgumentException("cache size must be at least one");
        }
        cache = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(cacheLifeTimeInSec, TimeUnit.SECONDS)
                .maximumWeight(cacheSizeInWorkspaceInfo)
                .weigher(new Weigher<Long, Tuple9<Long, String, String, String, Long, String,
                        String, String, Map<String, String>>>() {

                    @Override
                    public int weigh(Long wsId, Tuple9<Long, String, String, String, Long, String,
                            String, String, Map<String, String>> workspaceInfo)
                    {
                        return 9 + workspaceInfo.getE9().size();
                    }
                })
                .build(new CacheLoader<Long, Tuple9<Long, String, String, String, Long, String,
                        String, String, Map<String, String>>>() {

                    @Override
                    public Tuple9<Long, String, String, String, Long, String,
                            String, String, Map<String, String>> load(Long wsId) throws Exception {
                        return provider.getWorkspaceInfo(wsId);
                    }
                });
    }

    @Override
    public Tuple9<Long, String, String, String, Long, String,
            String, String, Map<String, String>> getWorkspaceInfo(final Long wsId) throws IOException {
        try {
            return cache.get(wsId);
        } catch (ExecutionException e) {
            throw (IOException) e.getCause(); // IOE is the only checked exception
            // unchecked exceptions are wrapped in UncheckedExcecutionException
        }
    }
}
