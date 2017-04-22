package kbaserelationengine.events;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import us.kbase.common.service.UObject;

public class ESObjectStatusEventStorage implements ObjectStatusEventStorage, ObjectStatusEventListener{
	private static final int MAX_SIZE = 10000; 
    private RestClient _restClient = null;
	private HttpHost esHost;
	private String esIndexName = "object_indexing_queue";

    public ESObjectStatusEventStorage(HttpHost esHost) {
		super();
		this.esHost = esHost;
	}
	
	
	private RestClient restClient(){
    	if(_restClient == null){
            RestClientBuilder restClientBld = RestClient.builder(esHost);
            
			List<Header> headers = new ArrayList<Header>();
			headers.add(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
			restClientBld.setDefaultHeaders(headers.toArray(new Header[headers.size()]));
            
    		_restClient = restClientBld.build();
    	}
    	return _restClient;    	
    }		
	
	
	@Override
	public void createStorage() throws IOException {
        makeRequest("PUT", "/" + esIndexName, null);    	
	}
       
	@Override
	public void deleteStorage() throws IOException {
        makeRequest("DELETE", "/" + esIndexName, null);    	
	}	
	
	@Override
    public void store(ObjectStatusEvent obj) throws IOException{
        Map<String, Object> doc = new LinkedHashMap<String, Object>();
        
        doc.put("accessGroupId", obj.getAccessGroupId());
        doc.put("accessGroupObjectId", obj.getAccessGroupObjectId().toString());
        doc.put("version", obj.getVersion());
        doc.put("targetAccessGroupId", obj.getTargetAccessGroupId());
        doc.put("timestamp", obj.getTimestamp());
        doc.put("eventType", obj.getEventType().toString());
        doc.put("storageObjectType", obj.getStorageObjectType());
        doc.put("indexed", false);
        doc.put("processed", false);
        
        makeRequest("POST", "/" + esIndexName + "/" + obj.getStorageCode() + "/", doc);		    	
    }

    @Override
	public void statusChanged(ObjectStatusEvent obj) throws IOException {
		store(obj);
	}	
    
    @Override
	public void markAsProcessed(ObjectStatusEvent row, boolean isIndexed) throws IOException {
		ESQuery query = new ESQuery()
			.map("doc")
			.value("processed", true)
			.value("indexed",isIndexed);
		
		makeRequest("POST", "/" + esIndexName + "/" + row.getStorageCode() + "/" + row.getId() + "/" + "_update" , query.document());				
	}

	private Response makeRequest(String reqType, String urlPath, Map<String, ?> doc) throws IOException{
		RestClient restClient = restClient();
		HttpEntity body = doc == null ? null : 
						new StringEntity(UObject.transformObjectToString(doc));
		return restClient.performRequest(reqType, urlPath, Collections.<String, String>emptyMap(), body);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int count(String storageCode, boolean processed) throws IOException {
		refreshIndex();		
		ESQuery query = new ESQuery()
			.map("query")
				.map("match")
					.value("processed", processed);
		
		String url = "/" + esIndexName + "/";
		if(storageCode != null){
			url += storageCode + "/";
		}
	
		Response resp = makeRequest("GET", url + "_count", query.document());

		Map<String, Object> data = UObject.getMapper().readValue(
				resp.getEntity().getContent(), Map.class);

		return  (Integer)data.get("count"); 		
	}



	@SuppressWarnings("unchecked")
    @Override	
	public List<ObjectStatusEvent> find(String storageCode, boolean processed, int maxSize)
			throws IOException {
		refreshIndex();	
		ESQuery query = new ESQuery()
			.value("from",0)
			.value("size",maxSize)
			.map("query")
				.map("match")
					.value("processed",false);
	
		String url = "/" + esIndexName + "/";
		if(storageCode != null){
			url += storageCode + "/";
		}

		Response resp = makeRequest("GET", url + "_search", query.document());
		Map<String, Object> data = UObject.getMapper().readValue(
            resp.getEntity().getContent(), Map.class);
		List<Map<String, Object>> hits  = (List<Map<String, Object>>) 
				((LinkedHashMap<String,Object>) data.get("hits")).get("hits");	
		return toObjectStatuses(hits);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ObjectStatusCursor cursor(String storageCode, boolean processed, int pageSize, String timeAlive)
			throws IOException {
		
		refreshIndex();		
		ESQuery query = new ESQuery()
				.value("size", pageSize)
				.map("query")
					.map("match")
						.value("processed",false);
		
		String url = "/" + esIndexName + "/";
		if(storageCode != null){
			url += storageCode + "/";
		}

		Response resp = makeRequest("GET", url + "_search?scroll=" + timeAlive, query.document());	
		Map<String, Object> data = UObject.getMapper().readValue(
            resp.getEntity().getContent(), Map.class);
		List<Map<String, Object>> hits  = (List<Map<String, Object>>) 
				((LinkedHashMap<String,Object>) data.get("hits")).get("hits");	
		
		ObjectStatusCursor cursor = new ObjectStatusCursor( 
				(String) data.get("_scroll_id"),
				pageSize,
				timeAlive
		);
		cursor.nextPage(toObjectStatuses(hits));
		return cursor;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean nextPage(ObjectStatusCursor cursor) throws IOException {
		ESQuery query = new ESQuery()
				.value("scroll", cursor.getTimeAlive())
				.value("scroll_id", cursor.getCursorId());

		Response resp = makeRequest("GET", "/_search/scroll", query.document());	
		Map<String, Object> data = UObject.getMapper().readValue(
            resp.getEntity().getContent(), Map.class);
		List<Map<String, Object>> hits  = (List<Map<String, Object>>) 
				((LinkedHashMap<String,Object>) data.get("hits")).get("hits");	
		
		cursor.nextPage(toObjectStatuses(hits));
		return cursor.getData().size() > 0;
	}	
	
	
	@SuppressWarnings("unchecked")
    @Override	
	public List<ObjectStatusEvent> find(String storageCode, int accessGroupId, List<String> accessGroupObjectIds) throws IOException{
		
		refreshIndex();		
		ESQuery query = new ESQuery()
			.value("size", MAX_SIZE)
			.map("query")
			.map("constant_score")
			.map("filter")
			.map("bool")
				.array("must")						
					.map(null)
						.map("term")
							.value("accessGroupId", accessGroupId)
						.back()
					.back()
					.map(null)
						.map("terms")
							.value("accessGroupObjectId", accessGroupObjectIds);
				
		String url = "/" + esIndexName + "/";
		if(storageCode != null){
			url += storageCode + "/";
		}

		Response resp = makeRequest("GET", url + "_search", query.document());

		Map<String, Object> data = UObject.getMapper().readValue(
			resp.getEntity().getContent(), Map.class);
		List<Map<String, Object>> hits  = (List<Map<String, Object>>) 
				((LinkedHashMap<String,Object>) data.get("hits")).get("hits");	
		
		return toObjectStatuses(hits);
	}
	
	@Override
	public void markAsNonprocessed(String storageCode, String storageObjectType)
			throws IOException {
		
		ESQuery query = new ESQuery()
			.map("query")
				.map("match")
					.value("storageObjectType",storageObjectType)
				.back()
			.back()
			.map("script")
				.value("inline", "ctx._source.processed = false; ctx._source.indexed = false;");

		String url = "/" + esIndexName + "/";
		if(storageCode != null){
			url += storageCode + "/";
		}		
		makeRequest("POST", url + "_update_by_query", query.document());		
	}
	
	
	@SuppressWarnings("unchecked")
	private List<ObjectStatusEvent> toObjectStatuses(List<Map<String, Object>> hits) {
		List<ObjectStatusEvent> rows = new ArrayList<ObjectStatusEvent>();		
		for(Map<String, Object> hit: hits){								
			String storageCode = (String)hit.get("_type");
			String _id = (String)hit.get("_id");
			hit = (Map<String, Object>) hit.get("_source");
		
			rows.add( new ObjectStatusEvent(
				_id, 
				storageCode, 
				(Integer)hit.get("accessGroupId"),
				(String)hit.get("accessGroupObjectId"), 
				(Integer)hit.get("version"), 
				(Integer) hit.get("targetAccessGroupId"),
				(Long) hit.get("timestamp"),
				(String)hit.get("storageObjectType"), 
				ObjectStatusEventType.valueOf((String)hit.get("eventType"))
			));
		}			
		return rows;
	}


    private void refreshIndex() throws IOException {
        makeRequest("POST", "/" + esIndexName + "/_refresh", null);
    }


	@Override
	public void statusChanged(AccessGroupStatus newStatus) throws IOException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public List<AccessGroupStatus> findAccessGroups(String storageCode) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public List<Integer> findAccessGroupIds(String storageCode, String user) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
