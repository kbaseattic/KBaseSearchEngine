package kbaserelationengine.events.test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;

import org.apache.http.HttpHost;
import org.junit.Before;
import org.junit.Test;

import kbaserelationengine.events.AccessGroupStatus;
import kbaserelationengine.events.ESObjectStatusEventStorage;
import kbaserelationengine.events.MongoDBObjectStatusEventStorage;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventListener;
import kbaserelationengine.events.ObjectStatusEventStorage;
import kbaserelationengine.events.WSObjectStatusEventReconstructor;
import kbaserelationengine.events.WSObjectStatusEventTrigger;
import kbaserelationengine.events.test.fake.FakeObjectStatusStorage;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientException;
import workspace.GetObjects2Params;
import workspace.GetObjects2Results;
import workspace.ObjectSpecification;
import workspace.WorkspaceClient;
import workspace.WorkspaceIdentity;


public class WSObjectStatusEventReconstructorTest {
	WSObjectStatusEventReconstructor wet;
	ObjectStatusEventStorage fakeStorage;
	ESObjectStatusEventStorage esStorage;
	MongoDBObjectStatusEventStorage mdStorage;
	
	@Before
	public void init() throws MalformedURLException{
        fakeStorage = new FakeObjectStatusStorage();
        esStorage = new ESObjectStatusEventStorage(new HttpHost("localhost", 9200));
        mdStorage  = new MongoDBObjectStatusEventStorage("localhost", 27017);

        ObjectStatusEventStorage storage = mdStorage;
        WSObjectStatusEventTrigger eventTrigger = new WSObjectStatusEventTrigger();
        
        
        AuthToken token = new AuthToken(System.getenv().get("AUTH_TOKEN"), "unknown") ;
    	URL wsURL = new URL("https://ci.kbase.us/services/ws");
        wet = new WSObjectStatusEventReconstructor(wsURL, token , storage, eventTrigger);
        
        // Register listeners
        ObjectStatusEventListener listener;         
        listener = new ObjectStatusEventListener(){
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
//		esStorage.deleteStorage();
//		esStorage.createStorage();    	
        wet.update();        
    }	       
    
//    @Test
    public void test02() throws Exception {
    	WorkspaceClient wsClient =  wet.wsClient();
		Map<String, String> ret = wsClient.getPermissions(new WorkspaceIdentity().withId(19971L));
		for(String key: ret.keySet()){
			System.out.println(key);
		}
    }

    
    
//  @Test
    public void test99() throws IOException, JsonClientException{
    	WorkspaceClient wsClient =  wet.wsClient();
    	GetObjects2Params params = new GetObjects2Params().withObjects(Arrays.asList(
    			new ObjectSpecification().withWsid(19971L).withObjid(2L).withVer(0L)
    	));
		GetObjects2Results ret = wsClient.getObjects2(params );
		System.out.println("=======");
		
		System.out.println(ret.getData().get(0).getInfo());    	
  }
	
}
