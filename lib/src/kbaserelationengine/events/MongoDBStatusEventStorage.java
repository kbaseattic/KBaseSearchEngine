package kbaserelationengine.events;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBStatusEventStorage implements AccessGroupProvider, StatusEventStorage, StatusEventListener{
	private static final String DEFAULT_DB_NAME = "DataStatus";
	private static final String COLLECTION_GROUP_STATUS = "GroupStatus";
	private static final String COLLECTION_OBJECT_STATUS_EVENTS = "ObjectStatusEvents";
	
	private String host;
	private int port;
	private String dbName;
	private MongoClient mongoClient;
	
	public MongoDBStatusEventStorage(String host, int port){
	    this(host, port, DEFAULT_DB_NAME);
	}
	
    public MongoDBStatusEventStorage(String host, int port, String dbName){
		this.host = host;
		this.port = port;
		this.dbName = dbName;
		mongoClient = new MongoClient(this.host, this.port);
	}
	


	@Override
	public void createStorage() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void deleteStorage() throws IOException {
		// TODO Auto-generated method stub
	}

	private DBCollection collection(String name){
		return  mongoClient.getDB(dbName).getCollection(name);
	}
	
	@Override	
	public List<AccessGroupStatus> findAccessGroups(String storageCode){
		BasicDBObject query = new BasicDBObject();
		if(storageCode != null){
			query.append("storageCode", storageCode);			
		}	
		DBCursor cursor = collection(COLLECTION_GROUP_STATUS).find(query);
		
		List<AccessGroupStatus> gss = new ArrayList<AccessGroupStatus>();		
		while(cursor.hasNext()){
			DBObject dobj = cursor.next();
			
			BasicDBList usersList = (BasicDBList) dobj.get("users");
			String[] users = usersList.<String>toArray(new String[usersList.size()]);
			
			gss.add(new AccessGroupStatus(
					dobj.get("_id").toString(), 
					(String)dobj.get("storageCode"), 
					(Integer) dobj.get("accessGroupId"), 
					(Long)dobj.get("timestamp"),
					users
			));
		}
		cursor.close();
		
		return gss;
	}
	
	@Override
	public List<Integer> findAccessGroupIds(String storageCode, String user){
		BasicDBList queryItems = new BasicDBList();
		queryItems.add(new BasicDBObject("users", user));
		if(storageCode != null){
			queryItems.add(new BasicDBObject("storageCode", storageCode));			
		}
		BasicDBObject query = new BasicDBObject("$and", queryItems);	
		DBCursor cursor = collection(COLLECTION_GROUP_STATUS).find(query);
				
		List<Integer> groupIds = new ArrayList<Integer>();
		while(cursor.hasNext()){
			groupIds.add(( Integer) cursor.next().get("accessGroupId"));
		}
		return groupIds;
	}
	
	public void store(AccessGroupStatus obj) throws IOException {
		DBObject dobj = new BasicDBObject();		
		dobj.put("storageCode", obj.getStorageCode());
		dobj.put("accessGroupId", obj.getAccessGroupId());
		dobj.put("timestamp", obj.getTimestamp());
		dobj.put("users", obj.getUsers());
		
		ObjectId objId = obj.getId() != null ?  new ObjectId(obj.getId() ) : new ObjectId();
		BasicDBObject query = new BasicDBObject("_id",  objId );
		
		collection(COLLECTION_GROUP_STATUS).update(query, dobj, true, false);				
	}	
	
	
	@Override
	public void store(ObjectStatusEvent obj) throws IOException {
		DBObject dobj = new BasicDBObject();		
		dobj.put("storageCode", obj.getStorageCode());
		dobj.put("accessGroupId", obj.getAccessGroupId());
		dobj.put("accessGroupObjectId", obj.getAccessGroupObjectId().toString());
		dobj.put("version", obj.getVersion());
		dobj.put("targetAccessGroupId", obj.getTargetAccessGroupId());
		dobj.put("timestamp", obj.getTimestamp());
		dobj.put("eventType", obj.getEventType().toString());
		dobj.put("storageObjectType", obj.getStorageObjectType());
		dobj.put("indexed", false);
		dobj.put("processed", false);		
		collection(COLLECTION_OBJECT_STATUS_EVENTS).insert(dobj);				
	}

	@Override
	public void markAsProcessed(ObjectStatusEvent row, boolean isIndexed) throws IOException {
		BasicDBObject doc = new BasicDBObject().append("$set", 
				new BasicDBObject()
					.append("processed", true)
					.append("indexed", isIndexed)
					
		);

		BasicDBObject query = new BasicDBObject("_id", new ObjectId(row.getId() ) );
		collection(COLLECTION_OBJECT_STATUS_EVENTS).update(query, doc);
	}

	@Override
	public void markAsNonprocessed(String storageCode, String storageObjectType) throws IOException {
		BasicDBObject doc = new BasicDBObject().append("$set", 
				new BasicDBObject()
					.append("processed", false)
					.append("indexed", false)
					
		);

		BasicDBList queryItems = new BasicDBList();		
		if(storageCode != null){
			
			queryItems.add(new BasicDBObject("storageCode", storageCode));			
		}
		if(storageObjectType != null){
			queryItems.add(new BasicDBObject("storageObjectType", storageObjectType));
		}
		BasicDBObject query = new BasicDBObject("$and", queryItems);
		
		collection(COLLECTION_OBJECT_STATUS_EVENTS).updateMulti(query, doc);		
	}

	@Override
	public int count(String storageCode, boolean processed) throws IOException {
		
		BasicDBList queryItems = new BasicDBList();
		queryItems.add(new BasicDBObject("processed", processed));
		if(storageCode != null){
			
			queryItems.add(new BasicDBObject("storageCode", storageCode));			
		}
		BasicDBObject query = new BasicDBObject("$and", queryItems);
		
		return (int) collection(COLLECTION_OBJECT_STATUS_EVENTS).count(query);
	}

	private List<ObjectStatusEvent> find(BasicDBObject query, int skip, int limit) throws IOException {
		List<ObjectStatusEvent> events = new ArrayList<ObjectStatusEvent>();
		
		DBCursor cursor = collection(COLLECTION_OBJECT_STATUS_EVENTS).find(query).skip(skip).limit(limit);		
		while(cursor.hasNext()){
			DBObject dobj = cursor.next();
			ObjectStatusEvent event = new ObjectStatusEvent(
					dobj.get("_id").toString(),
					(String)dobj.get("storageCode"),
					(Integer)dobj.get("accessGroupId"),
					(String)dobj.get("accessGroupObjectId"),
					(Integer)dobj.get("version"),
					(Integer)dobj.get("targetAccessGroupId"),
					(Long)dobj.get("timestamp"),
					(String)dobj.get("storageObjectType"),
					ObjectStatusEventType.valueOf((String)dobj.get("eventType"))					
					);
			events.add(event);
		}	
		cursor.close();
		return events;

	}
	
	@Override
	public List<ObjectStatusEvent> find(String storageCode, boolean processed, int maxSize) throws IOException {
		BasicDBList queryItems = new BasicDBList();
		queryItems.add(new BasicDBObject("processed", processed));
		if(storageCode != null){
			queryItems.add(new BasicDBObject("storageCode", storageCode));			
		}
		BasicDBObject query = new BasicDBObject("$and", queryItems);
		
	
		return find(query, 0, maxSize);
	}

	class _Cursor extends ObjectStatusCursor{
		BasicDBObject query;

		public _Cursor(String cursorId, int pageSize, String timeAlive, BasicDBObject query) {
			super(cursorId, pageSize, timeAlive);
			this.query = query;
		}
	}
	
	@Override
	public ObjectStatusCursor cursor(String storageCode, boolean processed, int pageSize, String timeAlive)
			throws IOException {
		
		BasicDBList queryItems = new BasicDBList();
		queryItems.add(new BasicDBObject("processed", processed));
		if(storageCode != null){
			queryItems.add(new BasicDBObject("storageCode", storageCode));			
		}
		BasicDBObject query = new BasicDBObject("$and", queryItems);
		
		_Cursor cursor = new _Cursor(null, pageSize, timeAlive, query);
		nextPage(cursor);
		return cursor;
	}

	@Override
	public boolean nextPage(ObjectStatusCursor cursor) throws IOException {
		_Cursor _cursor = (_Cursor)cursor;
		List<ObjectStatusEvent> objs = find(_cursor.query, _cursor.getPageIndex()*_cursor.getPageSize(), _cursor.getPageSize());
		
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
	public void statusChanged(ObjectStatusEvent event) throws IOException {
		store(event);		
	}

	@Override
	public void statusChanged(AccessGroupStatus newStatus) throws IOException {
		store(newStatus);		
	}
	
}
