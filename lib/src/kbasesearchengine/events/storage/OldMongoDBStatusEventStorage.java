package kbasesearchengine.events.storage;

import java.sql.Date;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.google.common.base.Optional;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEvent.Builder;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StatusEventWithID;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.tools.Utils;

public class OldMongoDBStatusEventStorage implements OldStatusEventStorage {

    private static final String COLLECTION_OBJECT_STATUS_EVENTS = "ObjectStatusEvents";

    private MongoDatabase db;

    public OldMongoDBStatusEventStorage(final MongoDatabase db){
        Utils.nonNull(db, "db");
        this.db = db;
    }

    private MongoCollection<Document> collection(String name){
        return db.getCollection(name);
    }

    @Override
    public StatusEventWithID store(StatusEvent obj) {
        final Document dobj = new Document();
        final Optional<StorageObjectType> sot = obj.getStorageObjectType();
        dobj.put("storageCode", obj.getStorageCode());
        dobj.put("accessGroupId", obj.getAccessGroupId().isPresent() ?
                obj.getAccessGroupId().get(): null);
        dobj.put("accessGroupObjectId", obj.getAccessGroupObjectId().isPresent() ?
                obj.getAccessGroupObjectId().get() : null);
        dobj.put("version", obj.getVersion().isPresent() ? obj.getVersion().get() : null);
        dobj.put("timestamp", Date.from(obj.getTimestamp()));
        dobj.put("eventType", obj.getEventType().toString());
        dobj.put("storageObjectType", sot.isPresent() ? sot.get().getType() : null);
        final Optional<Integer> version = sot.isPresent() ?
                sot.get().getVersion() : Optional.absent();
        dobj.put("storageObjectTypeVersion", version.isPresent() ? version.get() : null);
        dobj.put("isGlobalAccessed", obj.isGlobalAccessed().isPresent() ? obj.isGlobalAccessed().get() : null);
        dobj.put("newName", obj.getNewName().isPresent() ? obj.getNewName().get() : null);
        dobj.put("procst", obj.getProcessingState().toString());
        collection(COLLECTION_OBJECT_STATUS_EVENTS).insertOne(dobj);
        return new StatusEventWithID(obj, new StatusEventID(dobj.getObjectId("_id").toString()));
    }

    @Override
    public void markAsProcessed(
            final StatusEventWithID row,
            final StatusEventProcessingState state) {
        final Document doc = new Document().append("$set",
                new Document("procst", state.toString()));

        final Document query = new Document("_id", new ObjectId(row.getId().getId() ) );
        collection(COLLECTION_OBJECT_STATUS_EVENTS).updateOne(query, doc);
    }

    private List<StatusEventWithID> find(Document query, int skip, int limit) {
        List<StatusEventWithID> events = new ArrayList<>();

        FindIterable<Document> cursor = collection(COLLECTION_OBJECT_STATUS_EVENTS).find(query).skip(skip).limit(limit);
        for (final Document dobj: cursor) {
            final String storageCode = (String)dobj.get("storageCode");
            final String type = (String)dobj.get("storageObjectType");
            final Integer ver = (Integer) dobj.get("storageObjectTypeVersion");
            final StorageObjectType sot = type == null ? null :
                StorageObjectType.fromNullableVersion(storageCode, type, ver);
            final Instant time = dobj.getDate("timestamp").toInstant();
            final StatusEventType eventType = StatusEventType.valueOf(dobj.getString("eventType"));
            final Builder b;
            if (sot == null) {
                b = StatusEvent.getBuilder(storageCode, time, eventType);
            } else {
                b = StatusEvent.getBuilder(sot, time, eventType);
            }
            events.add(new StatusEventWithID(b
                    .withNullableAccessGroupID(dobj.getInteger("accessGroupId"))
                    .withNullableObjectID(dobj.getString("accessGroupObjectId"))
                    .withNullableVersion(dobj.getInteger("version"))
                    .withNullableNewName(dobj.getString("newName"))
                    .withNullableisPublic(dobj.getBoolean("isGlobalAccessed"))
                    .withProcessingState(StatusEventProcessingState.valueOf(
                            dobj.getString("procst")))
                    .build(),
                    new StatusEventID(dobj.getObjectId("_id").toString())));
        }
        return events;

    }

    class _Cursor extends StatusEventCursor{
        final Document query;

        public _Cursor(String cursorId, int pageSize, String timeAlive, Document query) {
            super(cursorId, pageSize, timeAlive);
            this.query = query;
        }
    }

    @Override
    public StatusEventCursor cursor(
            final String storageCode,
            final boolean processed,
            final int pageSize,
            final String timeAlive) {

        final List<Document> queryItems = new LinkedList<>();
        queryItems.add(new Document("procst", StatusEventProcessingState.UNPROC.toString()));
        if(storageCode != null){
            queryItems.add(new Document("storageCode", storageCode));
        }
        final Document query = new Document("$and", queryItems);

        _Cursor cursor = new _Cursor(null, pageSize, timeAlive, query);
        nextPage(cursor, 0);
        return cursor;
    }

    @Override
    public boolean nextPage(StatusEventCursor cursor, int nRemovedItems) {
        _Cursor _cursor = (_Cursor)cursor;
        List<StatusEventWithID> objs = find(_cursor.query, _cursor.getPageIndex()*_cursor.getPageSize() - nRemovedItems, _cursor.getPageSize());

        _cursor.nextPage(objs);

        return objs.size() > 0;
    }
}
