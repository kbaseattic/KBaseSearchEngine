package kbasesearchengine.events.storage;

import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.StatusEvent.Builder;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.tools.Utils;

/** An implementation of {@link StatusEventStorage} with MongoDB as the backend.
 * @author gaprice@lbl.gov
 *
 */
public class MongoDBStatusEventStorage implements StatusEventStorage {
    
    /* Note that general mongoexceptions are more or less impossible to test. */
    
    //TODO DB add schema ver code
    //TODO EVENT need optional parent event for sub events, so can check if event already exists
    
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
    // the ID, if any, of the operator that last changed the event status. Arbitrary string.
    private static final String FLD_UPDATER = "updtr";
    
    private static final String COL_EVENT = "searchEvents";
    
    private Map<String, List<IndexSpecification>> getIndexSpecs() {
        // should probably rework this and the index spec class
        //hardcoded indexes
        final HashMap<String, List<IndexSpecification>> indexes = new HashMap<>();
        
        // event indexes
        final LinkedList<IndexSpecification> event = new LinkedList<>();
        //find events by status and time stamp
        event.add(idxSpec(FLD_STATUS, 1, FLD_TIMESTAMP, 1, null));
        indexes.put(COL_EVENT, event);
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
    
    /** Create the storage system.
     * @param db the mongo database in which to store events.
     * @throws StorageInitException if the storage system could not be initialized.
     */
    public MongoDBStatusEventStorage(final MongoDatabase db) throws StorageInitException {
        Utils.nonNull(db, "db");
        this.db = db;
        ensureIndexes();
    }
    
    @Override
    public StoredStatusEvent store(
            final StatusEvent newEvent,
            final StatusEventProcessingState state)
            throws FatalRetriableIndexingException {
        Utils.nonNull(newEvent, "newEvent");
        Utils.nonNull(state, "state");
        final Optional<StorageObjectType> sot = newEvent.getStorageObjectType();
        final Document doc = new Document()
                .append(FLD_ACCESS_GROUP_ID, newEvent.getAccessGroupId().orNull())
                .append(FLD_OBJECT_ID, newEvent.getAccessGroupObjectId().orNull())
                .append(FLD_VERSION, newEvent.getVersion().orNull())
                .append(FLD_EVENT_TYPE, newEvent.getEventType())
                .append(FLD_OBJECT_TYPE, sot.isPresent() ? sot.get().getType() : null)
                .append(FLD_OBJECT_TYPE_VER, sot.isPresent() ?
                        sot.get().getVersion().orNull() : null)
                .append(FLD_STORAGE_CODE, newEvent.getStorageCode())
                .append(FLD_STATUS, state.toString())
                .append(FLD_EVENT_TYPE, newEvent.getEventType().toString())
                .append(FLD_NEW_NAME, newEvent.getNewName().orNull())
                .append(FLD_PUBLIC, newEvent.isPublic().orNull())
                .append(FLD_TIMESTAMP, Date.from(newEvent.getTimestamp()));
        try {
            db.getCollection(COL_EVENT).insertOne(doc);
        } catch (MongoException e) {
            throw new FatalRetriableIndexingException(
                    "Failed event storage: " + e.getMessage(), e);
        }
        return new StoredStatusEvent(
                newEvent, new StatusEventID(doc.getObjectId("_id").toString()), state, null);
    }
    
    @Override
    public Optional<StoredStatusEvent> get(final StatusEventID id)
            throws FatalRetriableIndexingException {
        Utils.nonNull(id, "id");
        final Document event;
        try {
            event = db.getCollection(COL_EVENT).find(
                    new Document("_id", new ObjectId(id.getId()))).first();
        } catch (MongoException e) {
            throw new FatalRetriableIndexingException(
                    "Failed getting event: " + e.getMessage(), e);
        }
        if (event == null) {
            return Optional.absent();
        }
        return Optional.of(toEvent(event));
    }

    private StoredStatusEvent toEvent(final Document event) {
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
        return new StoredStatusEvent(b
                .withNullableAccessGroupID(event.getInteger(FLD_ACCESS_GROUP_ID))
                .withNullableObjectID(event.getString(FLD_OBJECT_ID))
                .withNullableVersion(event.getInteger(FLD_VERSION))
                .withNullableNewName(event.getString(FLD_NEW_NAME))
                .withNullableisPublic(event.getBoolean(FLD_PUBLIC))
                .build(),
                new StatusEventID(event.getObjectId("_id").toString()),
                StatusEventProcessingState.valueOf(event.getString(FLD_STATUS)),
                event.getString(FLD_UPDATER));
    }
    
    // note returns in order of time stamp, oldest first (e.g FIFO)
    @Override
    public List<StoredStatusEvent> get(final StatusEventProcessingState state, int limit)
            throws FatalRetriableIndexingException {
        Utils.nonNull(state, "state");
        if (limit < 1 || limit > 1000) {
            limit = 1000;
        }
        final List<StoredStatusEvent> ret = new LinkedList<>();
        try {
            // tested query in mongo & ensured it uses indexes - e.g. no in memory sort
            final FindIterable<Document> iter = db.getCollection(COL_EVENT)
                    .find(new Document(FLD_STATUS, state.toString()))
                    .sort(new Document(FLD_TIMESTAMP, 1))
                    .limit(limit);
            for (final Document event: iter) {
                ret.add(toEvent(event));
            }
        } catch (MongoException e) {
            throw new FatalRetriableIndexingException(
                    "Failed getting events: " + e.getMessage(), e);
        }
        return ret;
    }
    
    @Override
    public boolean setProcessingState(
            final StoredStatusEvent event,
            final StatusEventProcessingState state)
            throws FatalRetriableIndexingException {
        Utils.nonNull(event, "event");
        Utils.nonNull(state, "state");
        try {
            final UpdateResult res = db.getCollection(COL_EVENT).updateOne(
                    new Document("_id", new ObjectId(event.getId().getId())), 
                    new Document("$set", new Document(FLD_STATUS, state.toString())));
            return res.getMatchedCount() == 1;
        } catch (MongoException e) {
            throw new FatalRetriableIndexingException(
                    "Failed setting event state: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<StoredStatusEvent> getAndSetProcessingState(
            final StatusEventProcessingState oldState,
            final StatusEventProcessingState newState,
            final String updater)
            throws FatalRetriableIndexingException {
        Utils.nonNull(oldState, "oldState");
        Utils.nonNull(newState, "newState");
        final Document innerQuery = new Document(FLD_STATUS, newState.toString());
        if (!Utils.isNullOrEmpty(updater)) {
            innerQuery.append(FLD_UPDATER, updater);
        }
        final Document ret;
        try {
             ret = db.getCollection(COL_EVENT).findOneAndUpdate(
                     new Document(FLD_STATUS, oldState.toString()),
                     new Document("$set", innerQuery),
                     new FindOneAndUpdateOptions()
                             .sort(new Document(FLD_TIMESTAMP, 1))
                             .returnDocument(ReturnDocument.AFTER));
        } catch (MongoException e) {
            throw new FatalRetriableIndexingException(
                    "Failed setting event state: " + e.getMessage(), e);
        }
        if (ret == null) {
            return Optional.absent();
        }
        return Optional.of(toEvent(ret));
    }

}
