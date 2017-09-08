package kbaserelationengine.events.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.google.common.base.Optional;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;

import kbaserelationengine.events.AccessGroupProvider;
import kbaserelationengine.events.AccessGroupStatus;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventType;
import kbaserelationengine.events.StatusEventListener;
import kbaserelationengine.system.StorageObjectType;

public class MongoDBStatusEventStorage implements AccessGroupProvider, StatusEventStorage, StatusEventListener{
	private static final String COLLECTION_GROUP_STATUS = "GroupStatus";
	private static final String COLLECTION_OBJECT_STATUS_EVENTS = "ObjectStatusEvents";
	
	private MongoDatabase db;

	public MongoDBStatusEventStorage(final MongoDatabase db){
	    this.db = db;
	}

	@Override
	public void createStorage() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void deleteStorage() throws IOException {
		// TODO Auto-generated method stub
	}

	private MongoCollection<Document> collection(String name){
		return db.getCollection(name);
	}
		
	@Override	
	public List<AccessGroupStatus> findAccessGroups(String storageCode){
		final Document query = new Document();
		if(storageCode != null){
			query.append("storageCode", storageCode);			
		}	
		FindIterable<Document> cursor = collection(COLLECTION_GROUP_STATUS).find(query);
		
		List<AccessGroupStatus> gss = new ArrayList<AccessGroupStatus>();
		for (final Document dobj: cursor) {
			
			@SuppressWarnings("unchecked")
            List<String> users = (List<String>) dobj.get("users");
			
			gss.add(new AccessGroupStatus(
					dobj.get("_id").toString(), 
					(String)dobj.get("storageCode"), 
					(Integer) dobj.get("accessGroupId"), 
					(Long)dobj.get("timestamp"),
					(Boolean)dobj.get("isPrivate"),
					(Boolean)dobj.get("isDeleted"),
					users
			));
		}
		
		return gss;
	}
	
	@Override
	public List<Integer> findAccessGroupIds(String user){
		BasicDBList queryItems = new BasicDBList();
		queryItems.add(new BasicDBObject("users", user));
		queryItems.add(new BasicDBObject("storageCode", "WS"));
		BasicDBObject query = new BasicDBObject("$and", queryItems);	
		FindIterable<Document> cursor = collection(COLLECTION_GROUP_STATUS).find(query);
				
		List<Integer> groupIds = new ArrayList<Integer>();
		for (final Document d: cursor) {
			groupIds.add(d.getInteger("accessGroupId"));
		}
		return groupIds;
	}
	
	public void store(AccessGroupStatus obj) throws IOException {
		final Document dobj = new Document();
		dobj.put("storageCode", obj.getStorageCode());
		dobj.put("accessGroupId", obj.getAccessGroupId());
		dobj.put("timestamp", obj.getTimestamp());
		dobj.put("isPrivate", obj.isPrivate());
		dobj.put("isDeleted", obj.isDeleted());
		dobj.put("users", obj.getUsers());
		
		ObjectId objId = obj.getId() != null ?  new ObjectId(obj.getId() ) : new ObjectId();
		final Document query = new Document("_id",  objId );
		
//		BasicDBList queryItems = new BasicDBList();
//		queryItems.add(new BasicDBObject("storageCode", obj.getStorageCode()));			
//		queryItems.add(new BasicDBObject("accessGroupId", obj.getAccessGroupId()));
//		BasicDBObject query = new BasicDBObject("$and", queryItems);
		
		collection(COLLECTION_GROUP_STATUS).updateOne(
		        query, dobj, new UpdateOptions().upsert(true));
	}	
	
	private void updateGroupPrivelages(AccessGroupStatus obj) {
				
		
//		DBObject dobj = new BasicDBObject();		
//		dobj.put("storageCode", obj.getStorageCode());
//		dobj.put("accessGroupId", obj.getAccessGroupId());
//		dobj.put("timestamp", obj.getTimestamp());
//		dobj.put("isDeleted", obj.isDeleted());
//		dobj.put("isPrivate", obj.isPrivate());
//		dobj.put("users", obj.getUsers());
		
		// Update only info about privacy and users
		final Document dobj = new Document();
		dobj.append("$set", new Document("isPrivate", obj.isPrivate()));
		dobj.append("$set", new Document("users", obj.getUsers()));
		
		
//		ObjectId objId = obj.getId() != null ?  new ObjectId(obj.getId() ) : new ObjectId();
		BasicDBObject query = new BasicDBObject("_id",  new ObjectId(obj.getId() ) );
		
//		BasicDBList queryItems = new BasicDBList();
//		queryItems.add(new BasicDBObject("storageCode", obj.getStorageCode()));			
//		queryItems.add(new BasicDBObject("accessGroupId", obj.getAccessGroupId()));
//		BasicDBObject query = new BasicDBObject("$and", queryItems);
		
		collection(COLLECTION_GROUP_STATUS).updateOne(query, dobj);
	}
	
	@Override
	public void store(ObjectStatusEvent obj) throws IOException {
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
	public void markAsProcessed(ObjectStatusEvent row, boolean isIndexed) throws IOException {
		final Document doc = new Document().append("$set", 
				new Document()
					.append("processed", true)
					.append("indexed", isIndexed)
					
		);

		final Document query = new Document("_id", new ObjectId(row.getId() ) );
		collection(COLLECTION_OBJECT_STATUS_EVENTS).updateOne(query, doc);
	}

	@Override
	public void markAsNonprocessed(String storageCode, String storageObjectType) throws IOException {
		final Document doc = new Document().append("$set", 
				new Document()
					.append("processed", false)
					.append("indexed", false)
					
		);

		final List<Document> queryItems = new LinkedList<>();		
		if(storageCode != null){
			
			queryItems.add(new Document("storageCode", storageCode));			
		}
		if(storageObjectType != null){
			queryItems.add(new Document("storageObjectType", storageObjectType));
		}
		BasicDBObject query = new BasicDBObject("$and", queryItems);
		
		collection(COLLECTION_OBJECT_STATUS_EVENTS).updateMany(query, doc);
	}

	@Override
	public int count(String storageCode, boolean processed) throws IOException {
		
		final List<Document> queryItems = new LinkedList<>();
		queryItems.add(new Document("processed", processed));
		if(storageCode != null){
			
			queryItems.add(new Document("storageCode", storageCode));			
		}
		final Document query = new Document("$and", queryItems);
		
		return (int) collection(COLLECTION_OBJECT_STATUS_EVENTS).count(query);
	}

    private List<ObjectStatusEvent> find(Document query, int skip, int limit) throws IOException {
            List<ObjectStatusEvent> events = new ArrayList<ObjectStatusEvent>();

    FindIterable<Document> cursor = collection(COLLECTION_OBJECT_STATUS_EVENTS).find(query).skip(skip).limit(limit);
        for (final Document dobj: cursor) {
            final String storageCode = (String)dobj.get("storageCode");
            final String type = (String)dobj.get("storageObjectType");
            final Integer ver = (Integer) dobj.get("storageObjectTypeVersion");
            final StorageObjectType sot = type == null ? null :
                StorageObjectType.fromNullableVersion(storageCode, type, ver);
            ObjectStatusEvent event = new ObjectStatusEvent(
                    dobj.get("_id").toString(),
                    storageCode,
                    (Integer)dobj.get("accessGroupId"),
                    (String)dobj.get("accessGroupObjectId"),
                    (Integer)dobj.get("version"),
                    (String) dobj.get("newName"),
                    (Integer)dobj.get("targetAccessGroupId"),
                    (Long)dobj.get("timestamp"),
                    sot,
                    ObjectStatusEventType.valueOf((String)dobj.get("eventType")),
                    (Boolean)dobj.get("isGlobalAccessed")

                    );
            events.add(event);
        }
        return events;

    }
	
	@Override
	public List<ObjectStatusEvent> find(String storageCode, boolean processed, int maxSize) throws IOException {
		final List<Document> queryItems = new LinkedList<>();
		queryItems.add(new Document("processed", processed));
		if(storageCode != null){
			queryItems.add(new Document("storageCode", storageCode));			
		}
		final Document query = new Document("$and", queryItems);
		
	
		return find(query, 0, maxSize);
	}

	class _Cursor extends ObjectStatusCursor{
		final Document query;

		public _Cursor(String cursorId, int pageSize, String timeAlive, Document query) {
			super(cursorId, pageSize, timeAlive);
			this.query = query;
		}
	}
	
	@Override
	public ObjectStatusCursor cursor(String storageCode, boolean processed, int pageSize, String timeAlive)
			throws IOException {
		
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
	public boolean nextPage(ObjectStatusCursor cursor, int nRemovedItems) throws IOException {
		_Cursor _cursor = (_Cursor)cursor;
		List<ObjectStatusEvent> objs = find(_cursor.query, _cursor.getPageIndex()*_cursor.getPageSize() - nRemovedItems, _cursor.getPageSize());
		
		_cursor.nextPage(objs);
		
		return objs.size() > 0;
	}

	@Override
	public List<ObjectStatusEvent> find(String storageCode, int accessGroupId, List<String> accessGroupObjectIds)
			throws IOException {
		// TODO Auto-generated method stub
		return new ArrayList<ObjectStatusEvent>();
	}

	@Override
	public void objectStatusChanged(List<ObjectStatusEvent> events) throws IOException {
		//TODO bulk query
		for(ObjectStatusEvent event: events){
			
			if(event.getAccessGroupId() == 15 
					&& event.getStorageObjectType().equals("KBaseGenomes.Genome")){
				continue;
			}
			
			
			store(event);		
		}
	}

	@Override
	public void groupPermissionsChanged(List<AccessGroupStatus> newStatuses) throws IOException {
		//TODO bulk query
		for(AccessGroupStatus newStatus: newStatuses){
			updateGroupPrivelages(newStatus);
		}
	}


	@Override
	public void groupStatusChanged(List<AccessGroupStatus> newStatuses) throws IOException {
		//TODO bulk query
		for(AccessGroupStatus newStatus: newStatuses){
			store(newStatus);				
		}		
	}	
}
