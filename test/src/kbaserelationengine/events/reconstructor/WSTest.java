package kbaserelationengine.events.reconstructor;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventType;
import kbaserelationengine.events.reconstructor.Util;
import kbaserelationengine.events.reconstructor.WSStatusEventReconstructorImpl;
import kbaserelationengine.system.StorageObjectType;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple9;
import workspace.CopyObjectParams;
import workspace.CreateWorkspaceParams;
import workspace.ListObjectsParams;
import workspace.ListWorkspaceInfoParams;
import workspace.ObjectIdentity;
import workspace.WorkspaceClient;
import workspace.WorkspaceIdentity;

public class WSTest {
	WorkspaceClient wsClient;
	
	@Before
	public void init() throws IOException{
        AuthToken token = new AuthToken(System.getenv().get("AUTH_TOKEN"), "unknown") ;
    	URL wsURL = new URL("https://ci.kbase.us/services/ws");
        WSStatusEventReconstructorImpl wet = new WSStatusEventReconstructorImpl(wsURL, token , null);
        wsClient = wet.wsClient();	
	}
	
	@Test
	public void test() throws IOException, JsonClientException{
		Long wsId = 20281L;
//		Long newWsId = 20282L;
//		Long objId = 2L;

//		wsId = newWsId;
		
		System.out.println("=======Before "); 
//		printStat(wsId);
		
		//Do something
//		undeleteObject(wsId, objId);
//		deleteWorkspace(wsId);
//		undeleteWorksapce(wsId);
		
//		Long newWsId = createWarksapace("ws_test");
//		System.out.println("New ws id: " + newWsId);
		
//		copyObject(wsId, objId, newWsId, "qqq");
		
		System.out.println("=======After "); 
//		printStat(wsId);
//		listPrivateDeletedWorksapces();
		
		listAllObjects(wsId);
		
	}
	
	private void listAllObjects(Long wsId) throws IOException, JsonClientException {
		ListObjectsParams params;
		List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows;
		params = new ListObjectsParams().withIds( Arrays.asList(wsId) );
    	params.setShowHidden(1L);
    	params.setShowAllVersions(1L);      
    	params.setType("DataPalette.DataPalette");
    	
		String dateTo = Util.DATE_FORMATTER.print(1493838717000L + 1000);
		params.withBefore(dateTo);
		
    	rows = wsClient.listObjects(params);
    	for(Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>> row: rows){    		

    		String storageObjectType = row.getE3().split("-")[0];
    		ObjectDescriptor od = new ObjectDescriptor(
    				row.getE7().intValue(), 
    				row.getE1().toString(),  
    				row.getE5().intValue(),
    				storageObjectType,
    				Util.DATE_PARSER.parseDateTime(row.getE4()).getMillis(),
    				false);
    		System.out.println(od);
    	}			
	}

	public void listPrivateDeletedWorksapces() throws IOException, JsonClientException{
		ListWorkspaceInfoParams params = new ListWorkspaceInfoParams();
		params .setExcludeGlobal(1L);
		params.setShowDeleted(1L);
//		params.setShowOnlyDeleted(1L);

    	List<Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>>> wsInfos = wsClient.listWorkspaceInfo(params);
    	for(Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo: wsInfos){
    		System.out.println(wsInfo.getE1());
    		System.out.println( Util.DATE_PARSER.parseDateTime(wsInfo.getE4()).getMillis() );
    	}
	}
	
	
	public void printStat(Long wsId) throws IOException, JsonClientException{
		long wsTimestamp = getWorkspaceTimestamp(wsId);
		System.out.println(wsTimestamp);
		
		for(ObjectStatusEvent row: getWorkspaceObjectsInfo(wsId)){
			System.out.println(row);
		}					
	}
	
	public List<ObjectStatusEvent> getWorkspaceObjectsInfo(Long wsId) throws IOException, JsonClientException{
		List<ObjectStatusEvent> objs = new ArrayList<ObjectStatusEvent>();
		
		ListObjectsParams params = new ListObjectsParams();
		params.setIds(Arrays.asList(wsId));
    	params.setShowHidden(1L);
    	params.setShowAllVersions(1L);
    	params.setShowDeleted(1L);

    	List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows;
    	rows = wsClient.listObjects(params);
    	for(Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>> row: rows){
    		ObjectStatusEvent obj = new ObjectStatusEvent(
    				null,
    				"WS",
    				row.getE7().intValue(), 
    				row.getE1().toString(),  
    				row.getE5().intValue(), 
    				null,
    				null,
    				Util.DATE_PARSER.parseDateTime(row.getE4()).getMillis(),
    				new StorageObjectType("WS", row.getE3().split("-")[0]),
    				ObjectStatusEventType.CREATED, false);    	
    		objs.add(obj);
    	}
    		
	     return objs; 		
	}
	
	public long getWorkspaceTimestamp(Long wsId) throws IOException, JsonClientException{
		WorkspaceIdentity params = new WorkspaceIdentity();
		params.withId(wsId);
		Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo 
			= wsClient.getWorkspaceInfo(params);

		return  Util.DATE_PARSER.parseDateTime(wsInfo.getE4()).getMillis();
//		return new WorkspaceDescriptor(
//				wsInfo.getE1().intValue(), 
//				,
//				false);		
	}
	
	public void undeleteWorksapce(Long wsId) throws IOException, JsonClientException{
		wsClient.undeleteWorkspace(new WorkspaceIdentity().withId(wsId));
	}
	
	public void undeleteObject(Long wsId, Long objId) throws IOException, JsonClientException{
		wsClient.undeleteObjects(Arrays.asList(new ObjectIdentity().withWsid(wsId).withObjid(objId)));		
	}
	
	public void deleteWorkspace(Long wsId) throws IOException, JsonClientException{
		wsClient.deleteWorkspace(new WorkspaceIdentity().withId(wsId));
	}
	
	public void deleteObjects(Long wsId, Long objId) throws IOException, JsonClientException{
		wsClient.deleteObjects(Arrays.asList(new ObjectIdentity().withWsid(wsId).withObjid(objId)));
	}
	
	public Long createWarksapace(String name) throws IOException, JsonClientException{
		CreateWorkspaceParams params = new CreateWorkspaceParams();
		params.withWorkspace(name);
		Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo = wsClient.createWorkspace(params );
		return wsInfo.getE1();
		
//		return new WorkspaceDescriptor(
//				wsInfo.getE1().intValue(), 
//				Util.DATE_PARSER.parseDateTime(wsInfo.getE4()).getMillis(),
//				false);		
	}
	
	public void copyObject(Long wsIdFrom, Long objIdFrom, Long wsIdTo, String name) throws IOException, JsonClientException{
		CopyObjectParams params = new CopyObjectParams();
		params.setFrom(new ObjectIdentity().withWsid(wsIdFrom).withObjid(objIdFrom));
		params.setTo(new ObjectIdentity().withWsid(wsIdTo).withName(name));
		wsClient.copyObject(params);
	}
}
