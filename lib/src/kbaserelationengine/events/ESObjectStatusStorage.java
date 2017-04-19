package kbaserelationengine.events;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import kbaserelationengine.common.GUID;

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

public class ESObjectStatusStorage implements ObjectStatusStorage{
    private RestClient _restClient = null;
	private HttpHost esHost;
	private String esIndexName = "_ObjectIndexingQueue";

    public ESObjectStatusStorage(HttpHost esHost) {
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
	
	
	public void initStorage() throws IOException {
        makeRequest("PUT", "/" + esIndexName, null);    	
	}
        
    public void store(ObjectStatus obj) throws IOException{
        Map<String, Object> doc = new LinkedHashMap<String, Object>();
        
        doc.put("accessGroupId", obj.getAccessGroupId());
        doc.put("accessGroupObjectId", obj.getAccessGroupObjectId().toString());
        doc.put("version", obj.getVersion());
        doc.put("eventType", obj.getEventType().toString());
        doc.put("storageObjectType", obj.getStorageObjectType());
        doc.put("indexed", false);
        
        makeRequest("POST", "/" + esIndexName + "/" + obj.getStorageCode() + "/", doc);		    	
    }
    
	public void markAsIndexed(ObjectStatus row) throws IOException {
		ESQuery query = new ESQuery().value("indexed",true);		
		makeRequest("PUT", "/" + esIndexName + "/" + row.getStorageCode() + "/" + row.getId()  , query.document());				
	}
	
	private Response makeRequest(String reqType, String urlPath, Map<String, ?> doc) throws IOException{
		RestClient restClient = restClient();
		HttpEntity body = doc == null ? null : 
						new StringEntity(UObject.transformObjectToString(doc));
		return restClient.performRequest(reqType, urlPath, Collections.<String, String>emptyMap(), body);
	}

	@SuppressWarnings("unchecked")
	public int countNonIndexedObjects(String storageCode) throws IOException {
		ESQuery query = new ESQuery()
			.term("query")
			.term("match")
			.value("indexed",false);
		
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
	public List<ObjectStatus> find(String storageCode, int maxSize)
			throws IOException {
		ESQuery query = new ESQuery()
			.value("from",0)
			.value("size",maxSize)
			.term("query")
			.term("match")
			.value("indexed",false);
	
		String url = "/" + esIndexName + "/";
		if(storageCode != null){
			url += storageCode + "/";
		}

		Response resp = makeRequest("GET", url + "_search", query.document());
		Map<String, Object> data = UObject.getMapper().readValue(
            resp.getEntity().getContent(), Map.class);
		List<Map<String, Object>> hits  = (List<Map<String, Object>> ) data.get("hits");
	
		List<ObjectStatus> rows = new ArrayList<ObjectStatus>();		
		for(Map<String, Object> hit: hits){				
			storageCode = (String)hit.get("_type");
		
			rows.add( new ObjectStatus(
				(String) hit.get("_id") , 
				storageCode, 
				(Integer)hit.get("accessGroupId"),
				(String)hit.get("accessGroupObjectId"), 
				(Integer)hit.get("version"), 
				(String)hit.get("storageObjectType"), 
				ObjectStatusEventType.valueOf((String)hit.get("eventType"))
			));
		}			
	
		return rows;	
	}



	public List<ObjectStatus> find(String storageCode, List<GUID> guids)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}


    
}
