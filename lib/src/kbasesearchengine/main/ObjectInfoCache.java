package kbasesearchengine.main;

import kbasesearchengine.tools.Utils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import us.kbase.common.service.JsonClientException;
import kbasesearchengine.tools.Utils;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import us.kbase.common.service.Tuple11;

/** A caching layer for object info. Caches the results from the wrapped
 * {@link ObjectInfoProvider} in memory for quick access.
 * @author ganapathy@bnl.gov
 *
 */
public class ObjectInfoCache implements ObjectInfoProvider {

    private final LoadingCache<String, Tuple11<Long, String, String, String, Long,
            String, Long, String, String, Long, Map<String, String>>> cache;

    /** Create a cache.
     * @param provider the {@link ObjectInfoProvider} whose results will be cached.
     * @param cacheLifeTimeInSec the number of seconds a set of object info should
     * remain in the cache.
     * @param cacheSizeInObjectInfo the maximum number of object infos
     * to store in the cache.
     */
    public ObjectInfoCache(
            final ObjectInfoProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInObjectInfo) {
        this(provider, cacheLifeTimeInSec, cacheSizeInObjectInfo, Ticker.systemTicker());
    }

    /** Create a cache for testing purposes.
     * @param provider the {@link ObjectInfoProvider} whose results will be cached.
     * @param cacheLifeTimeInSec the number of seconds a set of object info should
     * remain in the cache.
     * @param cacheSizeInObjectInfo the maximum number of object infos
     * to store in the cache.
     * @param ticker a ticker implementation that allows controlling cache expiration with the
     * provided ticker rather than waiting for the system clock. This is exposed for testing
     * purposes.
     */
    public ObjectInfoCache(
            final ObjectInfoProvider provider,
            final int cacheLifeTimeInSec,
            final int cacheSizeInObjectInfo,
            final Ticker ticker) {

        Utils.nonNull(provider, "provider");

        if (cacheLifeTimeInSec < 1) {
            throw new IllegalArgumentException("cache lifetime must be at least one second");
        }
        if (cacheSizeInObjectInfo < 1) {
            throw new IllegalArgumentException("cache size must be at least one");
        }
        cache = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(cacheLifeTimeInSec, TimeUnit.SECONDS)
                .maximumWeight(cacheSizeInObjectInfo)
                .weigher(new Weigher<String, Tuple11<Long, String, String, String, Long,
                        String, Long, String, String, Long, Map<String, String>>>() {

                    @Override
                    public int weigh(String objRef, Tuple11<Long, String, String, String, Long,
                            String, Long, String, String, Long, Map<String, String>> objInfo) {
                        return 10 + objInfo.getE11().size();
                    }
                })
                .build(new CacheLoader<String, Tuple11<Long, String, String, String, Long,
                        String, Long, String, String, Long, Map<String, String>>> () {

                    @Override
                    public Tuple11<Long, String, String, String, Long, String, Long, String, String, Long,
                            Map<String, String>> load(String objRef) throws Exception {
                        return provider.getObjectInfo(objRef);
                    }

                    @Override
                    public Map<String, Tuple11<Long, String, String, String, Long, String, Long, String, String,
                            Long, Map<String, String>>> loadAll(Iterable<? extends String> objRefs) throws Exception {
                        return provider.getObjectsInfo(objRefs);
                    }
                });
    }

    @Override
    public Tuple11<Long, String, String, String, Long,
            String, Long, String, String, Long, Map<String, String>> getObjectInfo(final String objRef)
            throws IOException, JsonClientException {
        try {
            return cache.get(objRef);
        } catch (ExecutionException e) {
            throw (IOException) e.getCause(); // IOE is the only checked exception
            // unchecked exceptions are wrapped in UncheckedExcecutionException
        }
    }

    @Override
    public Map<String, Tuple11<Long, String, String, String, Long, String, Long, String, String, Long,
            Map<String, String>>> getObjectsInfo(final Iterable <? extends String> objRefs)
            throws IOException, JsonClientException {
        try {
            return cache.getAll(objRefs);
        } catch (ExecutionException e) {
            throw (IOException) e.getCause(); // IOE is the only checked exception
            // unchecked exceptions are wrapped in UncheckedExcecutionException
        }
    }
}

