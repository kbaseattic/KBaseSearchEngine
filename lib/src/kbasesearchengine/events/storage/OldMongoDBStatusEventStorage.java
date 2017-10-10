package kbasesearchengine.events.storage;

import java.io.IOException;
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
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.tools.Utils;

public class OldMongoDBStatusEventStorage implements StatusEventStorage {

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
    public void store(StatusEvent obj) throws IOException {
        final Document dobj = new Document();
        dobj.put("storageCode", obj.getStorageCode());
        dobj.put("accessGroupId", obj.getAccessGroupId());
        dobj.put("accessGroupObjectId", obj.getAccessGroupObjectId().toString());
        dobj.put("version", obj.getVersion());
        dobj.put("targetAccessGroupId", obj.getTargetAccessGroupId());
        dobj.put("timestamp", obj.getTimestamp());
        dobj.put("eventType", obj.getEventType().toString());
        dobj.put("storageObjectType", obj.getStorageObjectType().getType());
        final Optional<Integer> version = obj.getStorageObjectType().getVersion();
        dobj.put("storageObjectTypeVersion", version.isPresent() ? version.get() : null);
        dobj.put("isGlobalAccessed", obj.isGlobalAccessed());
        dobj.put("newName", obj.getNewName());
        dobj.put("indexed", false);
        dobj.put("processed", false);		
        collection(COLLECTION_OBJECT_STATUS_EVENTS).insertOne(dobj);
    }

    @Override
    public void markAsProcessed(StatusEvent row, boolean isIndexed) throws IOException {
        final Document doc = new Document().append("$set", 
                new Document()
                .append("processed", true)
                .append("indexed", isIndexed)

                );

        final Document query = new Document("_id", new ObjectId(row.getId() ) );
        collection(COLLECTION_OBJECT_STATUS_EVENTS).updateOne(query, doc);
    }

    private List<StatusEvent> find(Document query, int skip, int limit) {
        List<StatusEvent> events = new ArrayList<StatusEvent>();

        FindIterable<Document> cursor = collection(COLLECTION_OBJECT_STATUS_EVENTS).find(query).skip(skip).limit(limit);
        for (final Document dobj: cursor) {
            final String storageCode = (String)dobj.get("storageCode");
            final String type = (String)dobj.get("storageObjectType");
            final Integer ver = (Integer) dobj.get("storageObjectTypeVersion");
            final StorageObjectType sot = type == null ? null :
                StorageObjectType.fromNullableVersion(storageCode, type, ver);
            StatusEvent event = new StatusEvent(
                    dobj.get("_id").toString(),
                    (Integer)dobj.get("accessGroupId"),
                    (String)dobj.get("accessGroupObjectId"),
                    (Integer)dobj.get("version"),
                    (String) dobj.get("newName"),
                    (Integer)dobj.get("targetAccessGroupId"),
                    (Long)dobj.get("timestamp"),
                    sot,
                    StatusEventType.valueOf((String)dobj.get("eventType")),
                    (Boolean)dobj.get("isGlobalAccessed")

                    );
            events.add(event);
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
        queryItems.add(new Document("processed", processed));
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
        List<StatusEvent> objs = find(_cursor.query, _cursor.getPageIndex()*_cursor.getPageSize() - nRemovedItems, _cursor.getPageSize());

        _cursor.nextPage(objs);

        return objs.size() > 0;
    }
}
