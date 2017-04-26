package kbaserelationengine.events.reconstructor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import kbaserelationengine.events.AccessGroupStatus;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.StatusEventListener;
import kbaserelationengine.events.WSStatusEventTrigger;
import kbaserelationengine.events.reconstructor.WSStatusEventReconstructorImpl;
import kbaserelationengine.events.storage.FakeStatusStorage;
import kbaserelationengine.events.storage.MongoDBStatusEventStorage;
import kbaserelationengine.events.storage.StatusEventStorage;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientException;
import workspace.GetObjects2Params;
import workspace.GetObjects2Results;
import workspace.ObjectSpecification;
import workspace.WorkspaceClient;
import workspace.WorkspaceIdentity;


public class WSStatusEventReconstructorTest {
	WSStatusEventReconstructorImpl reconstructor;
	StatusEventStorage fakeStorage;
	MongoDBStatusEventStorage mdStorage;
	
	@Before
	public void init() throws MalformedURLException{
        fakeStorage = new FakeStatusStorage();
        mdStorage  = new MongoDBStatusEventStorage("localhost", 27017);

        StatusEventStorage storage = mdStorage;
        WSStatusEventTrigger eventTrigger = new WSStatusEventTrigger();
        
        
        AuthToken token = new AuthToken(System.getenv().get("AUTH_TOKEN"), "unknown") ;
    	URL wsURL = new URL("https://ci.kbase.us/services/ws");
        reconstructor = new WSStatusEventReconstructorImpl(wsURL, token , storage, eventTrigger);
        
        // Register listeners
        StatusEventListener listener;         
        listener = new StatusEventListener(){
			@Override
			public void statusChanged(ObjectStatusEvent obj) throws IOException {
				System.out.println(obj);
			}

			@Override
			public void statusChanged(AccessGroupStatus newStatus) throws IOException {
				System.out.println(newStatus);				
			}
        }; 
        eventTrigger.registerListener(listener);  
        
        eventTrigger.registerListener(mdStorage);
	}
	
    @Test
    public void testRefilQueueStorage() throws Exception {
    	Set<Long> excludeWsIds = new HashSet<Long>();
//    	excludeWsIds.add(19971L);
//    	excludeWsIds.add(20281L);
//    	excludeWsIds.add(20266L);
    	
    	
        reconstructor.processWorkspaceObjects(AccessType.PRIVATE, PresenceType.PRESENT, excludeWsIds );        
    }	       
    
//    @Test
    public void test02() throws Exception {
    	WorkspaceClient wsClient =  reconstructor.wsClient();
		Map<String, String> ret = wsClient.getPermissions(new WorkspaceIdentity().withId(19971L));
		for(String key: ret.keySet()){
			System.out.println(key);
		}
    }

    
    
//  @Test
    public void test99() throws IOException, JsonClientException{
    	WorkspaceClient wsClient =  reconstructor.wsClient();
    	GetObjects2Params params = new GetObjects2Params().withObjects(Arrays.asList(
    			new ObjectSpecification().withWsid(19971L).withObjid(2L).withVer(0L)
    	));
		GetObjects2Results ret = wsClient.getObjects2(params );
		System.out.println("=======");
		
		System.out.println(ret.getData().get(0).getInfo());    	
  }
	
}
