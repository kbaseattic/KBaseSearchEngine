package kbasesearchengine.events.storage;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.google.common.base.Optional;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;

import kbasesearchengine.events.ChildStatusEvent;
import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredChildStatusEvent;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.StatusEvent.Builder;
import kbasesearchengine.events.exceptions.ErrorType;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.tools.Utils;

import javax.swing.event.DocumentEvent;

import static com.mongodb.client.model.Filters.eq;

/** An implementation of {@link StatusEventStorage} with MongoDB as the backend.
 * @author gaprice@lbl.gov
 *
 */
public class MongoDBStatusEventStorage implements StatusEventStorage {
    
    /* Note that general mongoexceptions are more or less impossible to test. */
    
    /* No need for a worker code index at the moment I think. The query for
     * setAndGetProcessingState should run down all the events in the target state in order of
     * timestamp until it finds one with an appropriate code.
     * Look into adding one if we start seeing long queries.
     */
    
    //TODO DB add schema ver code
    
    private static final List<String> DEFAULT_WORKER_CODES_LIST = Collections.unmodifiableList(
            Arrays.asList(StatusEventStorage.DEFAULT_WORKER_CODE));
    private static final Set<String> DEFAULT_WORKER_CODES_SET = Collections.unmodifiableSet(
            new HashSet<>(DEFAULT_WORKER_CODES_LIST));
    
    private static final int MAX_RETURNED_EVENTS = 10000;
    private static final int MAX_ERR_CODE_LEN = 20;
    private static final int TRUNC_ERR_MSG_LEN = 1000;
    private static final int TRUNC_ERR_TRACE_LEN = 100_000;
    
    private static final String FLD_STATUS = "status";
    private static final String FLD_STORAGE_CODE = "strcde";
    private static final String FLD_ACCESS_GROUP_ID = "accgrp";
    private static final String FLD_OBJECT_ID = "objid";
    private static final String FLD_VERSION = "ver";
    private static final String FLD_TIMESTAMP = "time";
    private static final String FLD_EVENT_TYPE = "evtype";
    private static final String FLD_OBJECT_TYPE = "objtype";
    private static final String FLD_OBJECT_TYPE_VER = "objtypever";
    private static final String FLD_PUBLIC = "public";
    private static final String FLD_NEW_NAME = "newname";
    private static final String FLD_OVERWRITE_EXISTING_DATA = "overwrite";
    private static final String FLD_WORKER_CODES = "wrkcde";
    private static final String FLD_UPDATE_TIME = "updte";
    // the ID, if any, of the operator that last changed the event status. Arbitrary string.
    private static final String FLD_UPDATER = "updtr";
    // the ID, if any, of the entity that stored the event. Arbitrary string.
    private static final String FLD_STORED_BY = "stby";
    private static final String FLD_STORED_TIME = "sttime";
    
    private static final String FLD_PARENT_ID = "parid";
    
    private static final String FLD_ERR_CODE = "errcde";
    private static final String FLD_ERR_MSG = "errmsg";
    private static final String FLD_ERR_TRACE = "errtrce";
    
    private static final String COL_EVENT = "searchEvents";
    private static final String COL_CHILD = "childEvents";
    
    private Map<String, List<IndexSpecification>> getIndexSpecs() {
        // should probably rework this and the index spec class
        //hardcoded indexes
        final HashMap<String, List<IndexSpecification>> indexes = new HashMap<>();
        
        // event indexes
        final LinkedList<IndexSpecification> event = new LinkedList<>();
        //find events by status and time stamp
        event.add(idxSpec(FLD_STATUS, 1, FLD_TIMESTAMP, 1, null));
        // find events by status and store time
        event.add(idxSpec(FLD_STORED_TIME, 1, FLD_STATUS, 1, null));
        indexes.put(COL_EVENT, event);
        
        // child event indexes
        final LinkedList<IndexSpecification> child = new LinkedList<>();
        // find events by status and store time
        child.add(idxSpec(FLD_STORED_TIME, 1, FLD_STATUS, 1, null));
        indexes.put(COL_CHILD, child);
        
        return indexes;
    }
    
    private static class IndexSpecification {
        public Document index;
        public IndexOptions options;
        
        private IndexSpecification(final Document index, final IndexOptions options) {
            this.index = index;
            this.options = options;
        }
    }
    
    // 1 for ascending sort, -1 for descending
    @SuppressWarnings("unused")
    private static IndexSpecification idxSpec(
            final String field, final int ascendingSort,
            final IndexOptions options) {
        
        return new IndexSpecification(new Document(field, ascendingSort), options);
    }

    private static IndexSpecification idxSpec(
            final String field1, final int ascendingSort1,
            final String field2, final int ascendingSort2,
            final IndexOptions options) {
        return new IndexSpecification(
                new Document(field1, ascendingSort1).append(field2, ascendingSort2), options);
    }

    @SuppressWarnings("unused")
    private static IndexSpecification idxSpec(
            final String field1, final int ascendingSort1,
            final String field2, final int ascendingSort2,
            final String field3, final int ascendingSort3,
            final IndexOptions options) {
        return new IndexSpecification(
                new Document(field1, ascendingSort1)
                    .append(field2, ascendingSort2)
                    .append(field3, ascendingSort3),
                options);
    }
    
    private void ensureIndexes() throws StorageInitException {
        final Map<String, List<IndexSpecification>> indexes = getIndexSpecs();
        for (final String col: indexes.keySet()) {
            for (IndexSpecification idx: indexes.get(col)) {
                final Document index = idx.index;
                final IndexOptions opts = idx.options;
                final MongoCollection<Document> dbcol = db.getCollection(col);
                try {
                    if (opts == null) {
                        dbcol.createIndex(index);
                    } else {
                        dbcol.createIndex(index, opts);
                    }
                } catch (MongoException me) {
                    throw new StorageInitException(
                            "Failed to create index: " + me.getMessage(), me);
                }
            }
        }
    }

    private final MongoDatabase db;
    private final Clock clock;
    
    /** Create the storage system.
     * @param db the mongo database in which to store events.
     * @throws StorageInitException if the storage system could not be initialized.
     */
    public MongoDBStatusEventStorage(final MongoDatabase db) throws StorageInitException {
        this(db, Clock.systemDefaultZone());
    }
    
    /** A test constructor that allows setting the storage clock. Do not use this constructor for
     * anything except tests.
     * @param db the mongo database in which to store events.
     * @param clock a clock to use for generating timestamps when updating event states.
     * Usually a mock.
     * @throws StorageInitException if the storage system could not be initialized.
     */
    public MongoDBStatusEventStorage(final MongoDatabase db, final Clock clock)
            throws StorageInitException {
        Utils.nonNull(db, "db");
        this.db = db;
        ensureIndexes();
        this.clock = clock;
    }
    
    @Override
    public StoredStatusEvent store(
            final StatusEvent newEvent,
            final StatusEventProcessingState state,
            Set<String> workerCodes,
            final String storedBy)
            throws FatalRetriableIndexingException {
        Utils.nonNull(newEvent, "newEvent");
        Utils.nonNull(state, "state");
        Utils.notNullOrEmpty(storedBy, "storedBy cannot be null or whitespace only");
        if (workerCodes == null || workerCodes.isEmpty()) {
            workerCodes = DEFAULT_WORKER_CODES_SET;
        }
        for (final String code: workerCodes) {
            if (Utils.isNullOrEmpty(code)) {
                throw new IllegalArgumentException("null or whitespace only item in workerCodes");
            }
        }
        final Instant now = clock.instant();
        final Document doc = toStorageDocument(newEvent, state, now)
                .append(FLD_WORKER_CODES, workerCodes)
                .append(FLD_STORED_BY, storedBy);
        final StatusEventID newID = insertOne(COL_EVENT, doc);
        final StoredStatusEvent.Builder b = StoredStatusEvent.getBuilder(newEvent, newID, state)
                .withNullableStoredBy(storedBy)
                .withNullableStoreTime(now);
        for (final String code: workerCodes) {
            b.withWorkerCode(code);
        }
        return b.build();
    }
    
    @Override
    public StoredChildStatusEvent store(
            final ChildStatusEvent newEvent,
            final String errorCode,
            final Throwable error) 
            throws FatalRetriableIndexingException {
        Utils.nonNull(newEvent, "newEvent");
        final Instant now = clock.instant();
        final Document doc = toStorageDocument(
                newEvent.getEvent(), StatusEventProcessingState.FAIL, now)
                .append(FLD_PARENT_ID, newEvent.getID().getId());
        addError(doc, errorCode, error);
        final StatusEventID newID = insertOne(COL_CHILD, doc);
        return StoredChildStatusEvent.getBuilder(newEvent, newID, now)
                .withNullableError(errorCode, doc.getString(FLD_ERR_MSG),
                        doc.getString(FLD_ERR_TRACE))
                .build();
    }

    // modifies doc in place
    private void addError(final Document doc, final String errorCode, final Throwable error) {
        checkErrorCode(errorCode);
        Utils.nonNull(error, "error");
        final StringWriter sw = new StringWriter();
        error.printStackTrace(new PrintWriter(sw));
        doc.append(FLD_ERR_CODE, errorCode)
                .append(FLD_ERR_MSG, truncate(error.getMessage(), TRUNC_ERR_MSG_LEN))
                .append(FLD_ERR_TRACE, truncate(sw.toString(), TRUNC_ERR_TRACE_LEN));
    }

    // assumes length > 3
    private Object truncate(final String string, final int length) {
        if (string.length() > length) {
            return string.substring(0, length - 3) + "...";
        }
        return string;
    }

    private void checkErrorCode(final String errorCode) {
        Utils.notNullOrEmpty(errorCode, "errorCode cannot be null or whitespace only");
        if (errorCode.length() > MAX_ERR_CODE_LEN) {
            throw new IllegalArgumentException("errorCode exceeds max length of " +
                    MAX_ERR_CODE_LEN);
        }
    }

    private StatusEventID insertOne(final String colEvent, final Document doc)
            throws FatalRetriableIndexingException {
        try {
            db.getCollection(colEvent).insertOne(doc);
        } catch (MongoException e) {
            throw new FatalRetriableIndexingException(
                    ErrorType.OTHER, "Failed event storage: " + e.getMessage(), e);
        }
        return new StatusEventID(doc.getObjectId("_id").toString());
    }
    
    private Document toStorageDocument(
            final StatusEvent newEvent,
            final StatusEventProcessingState state,
            final Instant now) {
        final Optional<StorageObjectType> sot = newEvent.getStorageObjectType();
        return new Document()
                .append(FLD_ACCESS_GROUP_ID, newEvent.getAccessGroupId().orNull())
                .append(FLD_OBJECT_ID, newEvent.getAccessGroupObjectId().orNull())
                .append(FLD_VERSION, newEvent.getVersion().orNull())
                .append(FLD_EVENT_TYPE, newEvent.getEventType())
                .append(FLD_OBJECT_TYPE, sot.isPresent() ? sot.get().getType() : null)
                .append(FLD_OBJECT_TYPE_VER, sot.isPresent() ?
                        sot.get().getVersion().orNull() : null)
                .append(FLD_STORAGE_CODE, newEvent.getStorageCode())
                .append(FLD_EVENT_TYPE, newEvent.getEventType().toString())
                .append(FLD_NEW_NAME, newEvent.getNewName().orNull())
                .append(FLD_OVERWRITE_EXISTING_DATA, newEvent.isOverwriteExistingData().orNull())
                .append(FLD_PUBLIC, newEvent.isPublic().orNull())
                .append(FLD_TIMESTAMP, Date.from(newEvent.getTimestamp()))
                .append(FLD_STATUS, state.toString())
                .append(FLD_STORED_TIME, Date.from(now));
    }

    @Override
    public Optional<StoredStatusEvent> get(final StatusEventID id)
            throws FatalRetriableIndexingException {
        final Document event = getEventDoc(id, COL_EVENT);
        if (event == null) {
            return Optional.absent();
        }
        return Optional.of(toStoredStatusEvent(event));
    }

    @Override
    public Optional<StoredChildStatusEvent> getChild(final StatusEventID id)
            throws FatalRetriableIndexingException {
        final Document event = getEventDoc(id, COL_CHILD);
        if (event == null) {
            return Optional.absent();
        }
        return Optional.of(StoredChildStatusEvent.getBuilder(
                new ChildStatusEvent(
                        toStatusEvent(event),
                        new StatusEventID(event.getString(FLD_PARENT_ID))),
                new StatusEventID(event.getObjectId("_id").toString()),
                event.getDate(FLD_STORED_TIME).toInstant())
                .withState(StatusEventProcessingState.valueOf(event.getString(FLD_STATUS)))
                .withNullableError(
                        event.getString(FLD_ERR_CODE),
                        event.getString(FLD_ERR_MSG),
                        event.getString(FLD_ERR_TRACE))
                .build());
    }

    private Document getEventDoc(final StatusEventID id, final String colEvent)
            throws FatalRetriableIndexingException {
        Utils.nonNull(id, "id");
        try {
            return db.getCollection(colEvent).find(
                    new Document("_id", new ObjectId(id.getId()))).first();
        } catch (MongoException e) {
            throw new FatalRetriableIndexingException(
                    ErrorType.OTHER, "Failed getting event: " + e.getMessage(), e);
        }
    }

    private StoredStatusEvent toStoredStatusEvent(final Document event) {
        final Date updateTime = event.getDate(FLD_UPDATE_TIME);
        final Date storeTime = event.getDate(FLD_STORED_TIME);
        @SuppressWarnings("unchecked")
        List<String> workerCodes = (List<String>) event.get(FLD_WORKER_CODES);
        if (workerCodes == null || workerCodes.isEmpty()) {
            workerCodes = DEFAULT_WORKER_CODES_LIST;
        }
        final StoredStatusEvent.Builder b2 = StoredStatusEvent.getBuilder(
                toStatusEvent(event),
                new StatusEventID(event.getObjectId("_id").toString()),
                StatusEventProcessingState.valueOf(event.getString(FLD_STATUS)))
                .withNullableUpdate(updateTime == null ? null : updateTime.toInstant(),
                        event.getString(FLD_UPDATER))
                .withNullableStoredBy(event.getString(FLD_STORED_BY))
                .withNullableStoreTime(storeTime == null ? null : storeTime.toInstant())
                .withNullableError(
                        event.getString(FLD_ERR_CODE),
                        event.getString(FLD_ERR_MSG),
                        event.getString(FLD_ERR_TRACE));
        for (final String code: workerCodes) {
            b2.withWorkerCode(code);
        }
        return b2.build();
    }

    private StatusEvent toStatusEvent(final Document event) {
        final String storageCode = (String) event.get(FLD_STORAGE_CODE);
        final String type = (String) event.get(FLD_OBJECT_TYPE);
        final Integer ver = (Integer) event.get(FLD_OBJECT_TYPE_VER);
        final StorageObjectType sot = type == null ? null :
            StorageObjectType.fromNullableVersion(storageCode, type, ver);
        final Instant time = event.getDate(FLD_TIMESTAMP).toInstant();
        final StatusEventType eventType = StatusEventType.valueOf(event.getString(FLD_EVENT_TYPE));
        final Builder b;
        if (sot == null) {
            b = StatusEvent.getBuilder(storageCode, time, eventType);
        } else {
            b = StatusEvent.getBuilder(sot, time, eventType);
        }
        return b
                .withNullableAccessGroupID(event.getInteger(FLD_ACCESS_GROUP_ID))
                .withNullableObjectID(event.getString(FLD_OBJECT_ID))
                .withNullableVersion(event.getInteger(FLD_VERSION))
                .withNullableNewName(event.getString(FLD_NEW_NAME))
                .withOverwriteExistingData(event.getBoolean(FLD_OVERWRITE_EXISTING_DATA))
                .withNullableisPublic(event.getBoolean(FLD_PUBLIC))
                .build();
    }
    
    // note returns in order of time stamp, oldest first (e.g FIFO)
    @Override
    public List<StoredStatusEvent> get(final StatusEventProcessingState state, int limit)
            throws FatalRetriableIndexingException {
        Utils.nonNull(state, "state");
        if (limit < 1 || limit > MAX_RETURNED_EVENTS) {
            limit = MAX_RETURNED_EVENTS;
        }
        final List<StoredStatusEvent> ret = new LinkedList<>();
        try {
            // tested query in mongo & ensured it uses indexes - e.g. no in memory sort
            final FindIterable<Document> iter = db.getCollection(COL_EVENT)
                    .find(new Document(FLD_STATUS, state.toString()))
                    .sort(new Document(FLD_TIMESTAMP, 1))
                    .limit(limit);
            for (final Document event: iter) {
                ret.add(toStoredStatusEvent(event));
            }
        } catch (MongoException e) {
            throw new FatalRetriableIndexingException(
                    ErrorType.OTHER, "Failed getting events: " + e.getMessage(), e);
        }
        return ret;
    }
    
    @Override
    public boolean setProcessingState(
            final StatusEventID id,
            final StatusEventProcessingState oldState,
            final StatusEventProcessingState newState)
            throws FatalRetriableIndexingException {
        return setProcessingState(id, oldState, newState, null, null);
    }
    
    @Override
    public boolean setProcessingState(
            final StatusEventID id,
            final StatusEventProcessingState oldState,
            final String errorCode,
            final Throwable error)
            throws FatalRetriableIndexingException {
        Utils.nonNull(error, "error");
        return setProcessingState(id, oldState, StatusEventProcessingState.FAIL, errorCode, error);
    }

    private boolean setProcessingState(
            final StatusEventID id,
            final StatusEventProcessingState oldState,
            final StatusEventProcessingState newState,
            final String errorCode,
            final Throwable error)
            throws FatalRetriableIndexingException {
        Utils.nonNull(id, "id");
        Utils.nonNull(newState, "newState");
        final Document update = new Document(FLD_STATUS, newState.toString())
                .append(FLD_UPDATE_TIME, Date.from(clock.instant()));
        if (error != null) {
            addError(update, errorCode, error);
        }
        final Document query = new Document("_id", new ObjectId(id.getId()));
        if (oldState != null) {
            query.append(FLD_STATUS, oldState.toString());
        }
        try {
            final UpdateResult res = db.getCollection(COL_EVENT).updateOne(query, 
                    new Document("$set", update));
            return res.getMatchedCount() == 1;
        } catch (MongoException e) {
            throw new FatalRetriableIndexingException(
                    ErrorType.OTHER, "Failed setting event state: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<StoredStatusEvent> setAndGetProcessingState(
            final StatusEventProcessingState oldState,
            final Set<String> workerCodes,
            final StatusEventProcessingState newState,
            final String updater)
            throws FatalRetriableIndexingException {
        Utils.nonNull(oldState, "oldState");
        Utils.nonNull(newState, "newState");
        Utils.notNullOrEmpty(updater, "updater cannot be null or whitespace");
        final List<Document> codeQuery = new LinkedList<>();
        final Set<String> codeSet = new HashSet<>();
        if (workerCodes == null || workerCodes.isEmpty() ||
                workerCodes.contains(StatusEventStorage.DEFAULT_WORKER_CODE)) {
            // next line matches missing field & null fields
            codeQuery.add(new Document(FLD_WORKER_CODES, null));
            codeQuery.add(new Document(FLD_WORKER_CODES, Collections.emptyList()));
            codeSet.add(StatusEventStorage.DEFAULT_WORKER_CODE);
        }
        if (workerCodes != null) {
            Utils.noNulls(workerCodes, "null item in workerCodes");
            codeSet.addAll(workerCodes);
        }
        codeQuery.add(new Document(FLD_WORKER_CODES, new Document("$in", codeSet)));
        
        final Document innerUpdate = new Document(FLD_STATUS, newState.toString())
                .append(FLD_UPDATE_TIME, Date.from(clock.instant()))
                .append(FLD_UPDATER, updater);
        final Document ret;
        try {
            ret = db.getCollection(COL_EVENT).findOneAndUpdate(
                     new Document(FLD_STATUS, oldState.toString()).append("$or", codeQuery),
                     new Document("$set", innerUpdate),
                     new FindOneAndUpdateOptions()
                             .sort(new Document(FLD_TIMESTAMP, 1))
                             .returnDocument(ReturnDocument.AFTER));
        } catch (MongoException e) {
            throw new FatalRetriableIndexingException(
                    ErrorType.OTHER, "Failed setting event state: " + e.getMessage(), e);
        }
        if (ret == null) {
            return Optional.absent();
        }
        return Optional.of(toStoredStatusEvent(ret));
    }

    @Override
    public void resetFailedEvents()
            throws FatalRetriableIndexingException {

        try {
            db.getCollection(COL_EVENT).
                updateMany(eq(FLD_STATUS,StatusEventProcessingState.FAIL.toString()),
                              new Document("$set",
                                  new Document("status",
                                          StatusEventProcessingState.UNPROC.toString())));
        } catch (MongoException ex) {
            throw new FatalRetriableIndexingException(ErrorType.OTHER,
                    "Failed to reset failed events: " + ex.getMessage(), ex);
        }
    }
}
