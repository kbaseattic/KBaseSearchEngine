package kbasesearchengine.test.main;

import static kbasesearchengine.test.common.TestCommon.set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import kbasesearchengine.events.handler.EventHandler;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.main.IndexerWorkerConfigurator;
import kbasesearchengine.main.IndexerWorkerConfigurator.Builder;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.system.TypeStorage;
import kbasesearchengine.test.common.TestCommon;

public class IndexerWorkerConfiguratorTest {

    @Test
    public void buildMinimal() {
        final TypeStorage ts = mock(TypeStorage.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        final StatusEventStorage ses = mock(StatusEventStorage.class);
        final LineLogger ll = mock(LineLogger.class);
        
        final IndexerWorkerConfigurator cfg = IndexerWorkerConfigurator.getBuilder(
                "id", Paths.get("foo"),ll)
                .withStorage(ses, ts, is)
                .build();
        
        assertThat("incorrect id", cfg.getWorkerID(), is("id"));
        assertThat("incorrect temp dir", cfg.getRootTempDir(), is(Paths.get("foo")));
        assertThat("incorrect logger", cfg.getLogger(), is(ll));
        assertThat("incorrect event store", cfg.getEventStorage(), is(ses));
        assertThat("incorrect type store", cfg.getTypeStorage(), is(ts));
        assertThat("incorrect index store", cfg.getIndexingStorage(), is(is));
        assertThat("incorrect event handlers", cfg.getEventHandlers(), is(Collections.emptyMap()));
        assertThat("incorrect wrk codes", cfg.getWorkerCodes(), is(Collections.emptySet()));
        assertThat("incorrect max objects", cfg.getMaxObjectsPerLoad(), is(200_000));
        assertThat("incorrect retry count", cfg.getRetryCount(), is(5));
        assertThat("incorrect retry sleep", cfg.getRetrySleepMS(), is(1000));
        assertThat("incorrect retry fatal", cfg.getRetryFatalBackoffMS(), is(Arrays.asList(
                1000, 2000, 4000, 8000, 16000)));
    }
    
    @Test
    public void buildMaximal() {
        final TypeStorage ts = mock(TypeStorage.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        final StatusEventStorage ses = mock(StatusEventStorage.class);
        final LineLogger ll = mock(LineLogger.class);
        final EventHandler eh1 = mock(EventHandler.class);
        final EventHandler eh2 = mock(EventHandler.class);
        
        when(eh1.getStorageCode()).thenReturn("sc1");
        when(eh2.getStorageCode()).thenReturn("sc2");
        
        final IndexerWorkerConfigurator cfg = IndexerWorkerConfigurator.getBuilder(
                "id", Paths.get("foo"),ll)
                .withStorage(ses, ts, is)
                .withEventHandler(eh1)
                .withEventHandler(eh2)
                .withMaxObjectsPerIndexingLoad(1)
                .withWorkerCode("foo")
                .withWorkerCode("bar")
                .withRetryCount(1)
                .withRetrySleepTimeMS(1)
                .withRetryFatalBackoffTimeMS(1, 6, 3, 42)
                .build();
        
        assertThat("incorrect id", cfg.getWorkerID(), is("id"));
        assertThat("incorrect temp dir", cfg.getRootTempDir(), is(Paths.get("foo")));
        assertThat("incorrect logger", cfg.getLogger(), is(ll));
        assertThat("incorrect event store", cfg.getEventStorage(), is(ses));
        assertThat("incorrect type store", cfg.getTypeStorage(), is(ts));
        assertThat("incorrect index store", cfg.getIndexingStorage(), is(is));
        assertThat("incorrect event handlers", cfg.getEventHandlers(), is(ImmutableMap.of(
                "sc1", eh1, "sc2", eh2)));
        assertThat("incorrect wrk codes", cfg.getWorkerCodes(), is(set("foo", "bar")));
        assertThat("incorrect max objects", cfg.getMaxObjectsPerLoad(), is(1));
        assertThat("incorrect retry count", cfg.getRetryCount(), is(1));
        assertThat("incorrect retry sleep", cfg.getRetrySleepMS(), is(1));
        assertThat("incorrect retry fatal", cfg.getRetryFatalBackoffMS(), is(Arrays.asList(
                1, 6, 3, 42)));
    }
    
    @Test
    public void immutableCollections() {
        final TypeStorage ts = mock(TypeStorage.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        final StatusEventStorage ses = mock(StatusEventStorage.class);
        final LineLogger ll = mock(LineLogger.class);
        final EventHandler eh1 = mock(EventHandler.class);
        final EventHandler eh2 = mock(EventHandler.class);
        
        when(eh1.getStorageCode()).thenReturn("sc1");
        when(eh2.getStorageCode()).thenReturn("sc2");
        
        final IndexerWorkerConfigurator cfg = IndexerWorkerConfigurator.getBuilder(
                "id", Paths.get("foo"),ll)
                .withStorage(ses, ts, is)
                .withEventHandler(eh1)
                .withEventHandler(eh2)
                .withMaxObjectsPerIndexingLoad(3)
                .withWorkerCode("foo")
                .withWorkerCode("bar")
                .build();
        
        try {
            cfg.getEventHandlers().put("foo", eh1);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // test passed
        }
        
        try {
            cfg.getWorkerCodes().add("baz");
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // test passed
        }
        
        try {
            cfg.getRetryFatalBackoffMS().add(1);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // test passed
        }
    }
    
    @Test
    public void startBuildFail() {
        final LineLogger ll = mock(LineLogger.class);
        final Path f = Paths.get("foo");
        
        failStartBuild(null, f, ll, new IllegalArgumentException(
                "workerID cannot be null or whitespace only"));
        failStartBuild("   \t   \n  ", f, ll, new IllegalArgumentException(
                "workerID cannot be null or whitespace only"));
        failStartBuild("i", null, ll, new NullPointerException("rootTempDirectory"));
        failStartBuild("i", f, null, new NullPointerException("logger"));
    }
    
    
    private void failStartBuild(
            final String id,
            final Path tempDir,
            final LineLogger logger,
            final Exception expected) {
        try {
            IndexerWorkerConfigurator.getBuilder(id, tempDir, logger);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void withStorageFail() {
        final StatusEventStorage ses = mock(StatusEventStorage.class);
        final TypeStorage ts = mock(TypeStorage.class);
        final IndexingStorage is = mock(IndexingStorage.class);
        
        failWithStorage(null, ts, is, new NullPointerException("eventStorage"));
        failWithStorage(ses, null, is, new NullPointerException("typeStorage"));
        failWithStorage(ses, ts, null, new NullPointerException("indexingStorage"));
    }
    
    private void failWithStorage(
            final StatusEventStorage ses,
            final TypeStorage ts,
            final IndexingStorage is,
            final Exception expected) {
        try {
            IndexerWorkerConfigurator.getBuilder("id", Paths.get("f"), mock(LineLogger.class))
                    .withStorage(ses, ts, is);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void withEventHandlerFail() {
        final IndexerWorkerConfigurator.Builder cfg = getBuilder();
        
        failWithEventHandler(cfg, null, new NullPointerException("handler"));
        
        final EventHandler eh1 = mock(EventHandler.class);
        when(eh1.getStorageCode()).thenReturn("mycode");
        cfg.withEventHandler(eh1);
        
        final EventHandler eh2 = mock(EventHandler.class);
        when(eh2.getStorageCode()).thenReturn("mycode");
        
        failWithEventHandler(cfg, eh2, new IllegalArgumentException(
                "Already registered a handler for storage code mycode"));
    }
    
    private void failWithEventHandler(
            final IndexerWorkerConfigurator.Builder builder,
            final EventHandler handler,
            final Exception expected) {
        try {
            builder.withEventHandler(handler);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void withWorkerCodeFail() {
        failWithWorkerCode(null, new IllegalArgumentException(
                "workerCode cannot be null or whitespace only"));
        failWithWorkerCode("   \t   \n ", new IllegalArgumentException(
                "workerCode cannot be null or whitespace only"));
    }
    
    private void failWithWorkerCode(final String workerCode, final Exception expected) {
        try {
            getBuilder().withWorkerCode(workerCode);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void withMaxObjectsFail() {
        failWithMaxObjects(0, new IllegalArgumentException("maxObjects must be at least 1"));
    }
    
    private void failWithMaxObjects(final int maxObjects, final Exception expected) {
        try {
            getBuilder().withMaxObjectsPerIndexingLoad(maxObjects);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    private Builder getBuilder() {
        return IndexerWorkerConfigurator.getBuilder("id", Paths.get("f"), mock(LineLogger.class))
                .withStorage(mock(StatusEventStorage.class), mock(TypeStorage.class),
                        mock(IndexingStorage.class));
    }
    
    @Test
    public void withRetryCountFail() {
        try {
            getBuilder().withRetryCount(0);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new IllegalArgumentException(
                    "retryCount must be at least 1"));
        }
    }
    
    @Test
    public void withRetrySleepFail() {
        try {
            getBuilder().withRetrySleepTimeMS(0);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new IllegalArgumentException(
                    "retrySleepMS must be at least 1"));
        }
    }
    
    @Test
    public void withRetryFatalBackoffFail() {
        failWithRetryFatalBackoff(new IllegalArgumentException(
                "Must provide at least one retry backoff time"));
        failWithRetryFatalBackoff(new NullPointerException("retryBackoffMS"), (Integer[]) null);
        failWithRetryFatalBackoff(new IllegalArgumentException(
                "The entries of retryBackoffMS must be non-null and at least 1"),
                (Integer) null, 6, 10);
        failWithRetryFatalBackoff(new IllegalArgumentException(
                "The entries of retryBackoffMS must be non-null and at least 1"), 6, 0, 10);
    }
    
    private void failWithRetryFatalBackoff(final Exception expected, final Integer... backoff) {
        try {
            getBuilder().withRetryFatalBackoffTimeMS(backoff);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void buildFail() {
        
        try {
            IndexerWorkerConfigurator.getBuilder("id", Paths.get("f"), mock(LineLogger.class))
                    .build();
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new IllegalArgumentException(
                    "storage systems must be set"));
        }
    }
}
