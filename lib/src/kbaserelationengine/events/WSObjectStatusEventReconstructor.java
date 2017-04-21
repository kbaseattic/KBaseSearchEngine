package kbaserelationengine.events;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple9;
import us.kbase.common.service.UObject;
import us.kbase.common.service.UnauthorizedException;
import workspace.GetObjects2Params;
import workspace.GetObjects2Results;
import workspace.ListObjectsParams;
import workspace.ListWorkspaceInfoParams;
import workspace.ObjectData;
import workspace.ObjectSpecification;
import workspace.WorkspaceClient;

public class WSObjectStatusEventReconstructor {
	
	private static String WS_STORAGE_CODE = "WS";
	private static String DATA_PALETTE_TYPE = "DataPalette.DataPalette";
	
	private URL wsUrl;
	private AuthToken token;
	private ObjectStatusEventStorage objStatusStorage;
	private WSObjectStatusEventTrigger eventTrigger;
	
	private WorkspaceClient _wsClient;
	
	private final static DateTimeFormatter DATE_PARSER =
			new DateTimeFormatterBuilder()
				.append(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss"))
				.appendOptional(DateTimeFormat.forPattern(".SSS").getParser())
				.append(DateTimeFormat.forPattern("Z"))
				.toFormatter();	
	
	
	public WSObjectStatusEventReconstructor(URL wsUrl, AuthToken token, 
			ObjectStatusEventStorage objStatusStorage,
			WSObjectStatusEventTrigger eventTrigger) {
		this.wsUrl = wsUrl;
		this.token = token;
		this.objStatusStorage = objStatusStorage;
		this.eventTrigger = eventTrigger;
	}
		
	public void update() throws IOException {
		try {
			doPrivateWorkspaces();
		} catch (JsonClientException e) {
			throw new IOException(e);
		}		
	}
	
	
	public void doPublicWorkspaces(){
		
	}
	
	class ObjDescriptor{
		Integer wsId;
		String objId;
		Integer version;
		String dataType;
		long timestamp;
		boolean isDeleted;
		boolean isProcessed;
		
		ObjectStatusEventType evetType;
		
		public ObjDescriptor(Integer wsId, String objId, Integer version, String dataType, long timestamp, boolean isDeleted) {
			super();
			this.wsId = wsId;
			this.objId = objId;
			this.version = version;
			this.dataType = dataType;
			this.timestamp = timestamp;
			this.isDeleted = isDeleted;
		}
		
		@Override
		public String toString(){
			return "{ wsId=" + wsId
					+ ", objId=" + objId
					+ ", version=" + version
					+ ", dataType=" + dataType
					+ ", timestamp=" + timestamp
					+ ", isDeleted=" + isDeleted
					+ ", isProcessed=" + isProcessed
					+ ", evetType=" + (evetType != null ? evetType.toString() : null)
					+" }";
		}

		public String toRef() {
			return wsId.toString() + "/" + objId + "/" + version.toString();
		}
	}
	
	
	public void doPrivateWorkspaces() throws IOException, JsonClientException{
    	WorkspaceClient wsClient = wsClient();
    	
    	// To collect data palettes from all workspaces
    	List<ObjDescriptor> dataPalettes = new ArrayList<ObjDescriptor>();    	

    	
    	// Objects which are candidates for create/update/delete events 
    	Map<String,List<ObjDescriptor>> objCandidates = new Hashtable<String,List<ObjDescriptor>>(); 

    	
    	List<Long> wsIds = getWorkspaceIds(true);
    	for(Long wsId: wsIds){
    		
    		// Collect obj candidates and data palettes from a given workspace 
    		
    		objCandidates.clear();
    		
    		ListObjectsParams params;
    		List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows;
    		
    		// No deleted
    		params = new ListObjectsParams().withIds( Arrays.asList(wsId) );
        	params.setShowHidden(1L);
        	params.setShowAllVersions(1L);        	
        	rows = wsClient.listObjects(params);
    		collectObjects(rows, objCandidates, dataPalettes, false);
    		
    		// Only deleted
        	params = new ListObjectsParams().withIds( Arrays.asList(wsId) );
        	params.setShowAllVersions(1L);
        	params.setShowDeleted(1L);
        	params.setShowOnlyDeleted(1L);
        	rows = wsClient.listObjects(params);
        	collectObjects(rows, objCandidates, dataPalettes, true);    		    		
        	
//        	processObjectCandidates(wsId, objCandidates);   
    	}
    	
    	// Process all palettes. We should do it only after events like create/update/detele are done
    	processDataPalettes(dataPalettes);
	}
	
	private void collectObjects(List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows,
			Map<String, List<ObjDescriptor>> objCandidates,
			List<ObjDescriptor> dataPalettes, boolean isDeleted) {
		
    	for(Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>> row: rows){
    		    		
    		ObjDescriptor od = new ObjDescriptor(
    				row.getE7().intValue(), 
    				row.getE1().toString(),  
    				row.getE5().intValue(), 
    				row.getE3().split("-")[0],
    				DATE_PARSER.parseDateTime(row.getE4()).getMillis(),
    				isDeleted);
    		if(od.dataType.equals(DATA_PALETTE_TYPE)){
    			dataPalettes.add(od);
    		} else{
    			List<ObjDescriptor> ods = objCandidates.get(od.objId);
    			if(ods == null){
    				ods = new ArrayList<ObjDescriptor>();
    				objCandidates.put(od.objId, ods);
    			}
    			ods.add(od);    			        			
    		}
    	}
	}

	private void processObjectCandidates(Long wsId,
		Map<String, List<ObjDescriptor>> objCandidates) throws IOException {
		
		List<String> objIds = new ArrayList<String>();
		for(String objId: objCandidates.keySet()){
			objIds.add(objId.toString());
		}
	
		System.out.println(objIds);
		
		
		// Mark objects that were already processed before
		List<ObjectStatusEvent> objPrevStates = objStatusStorage.find(WS_STORAGE_CODE, wsId.intValue(), objIds);
		for(ObjectStatusEvent op: objPrevStates){
			List<ObjDescriptor> candidates = objCandidates.get(op.getAccessGroupObjectId());
			if(candidates != null){
				for(ObjDescriptor od: candidates){
					if(od.version.intValue() == op.getVersion().intValue() ){
						od.isProcessed = true;
					}
				}
			}
		}
		
		// Define eventType for all nonindexed ones and raise event
		for(Map.Entry<String, List<ObjDescriptor>> entry: objCandidates.entrySet()){
			for(ObjDescriptor od: entry.getValue()){
				if(od.isProcessed ) continue;
				
				if(od.isDeleted){
					triggerEvent(od, ObjectStatusEventType.DELETED);
				} else{
					if(od.version.intValue() == 1){
						triggerEvent(od, ObjectStatusEventType.CREATED);
					} else{
						triggerEvent(od, ObjectStatusEventType.NEW_VERSION);						
					}
				}
			} 
		}		
	}

	private void triggerEvent(ObjDescriptor od, ObjectStatusEventType eventType) throws IOException {
		ObjectStatusEvent event = new ObjectStatusEvent(
				null,
				WS_STORAGE_CODE,
				od.wsId.intValue(), 
				od.objId.toString(), 
				od.version.intValue(), 
				null,
				od.timestamp,
				od.dataType, eventType);
		eventTrigger.trigger(event);
	}

	private void processDataPalettes(List<ObjDescriptor> dataPalettes) throws IOException, JsonClientException {
		
		List<ObjectStatusEvent> events = reconstructDataPalletObjectStatusEvents(dataPalettes);

	}

	class DataPallete implements Comparable<DataPallete>{
		long wsId;
		long objId;
		long version;
		long timestamp;
		String[] refs;
		DataPallete(ObjectData data){
			this.wsId = data.getInfo().getE7();
			this.objId = data.getInfo().getE1();
			this.version = data.getInfo().getE5();
			this.timestamp = DATE_PARSER.parseDateTime(data.getInfo().getE4()).getMillis();
			List<UObject> urefs = data.getData().asMap().get("data").asList();
			refs = new String[urefs.size()];
        	for(int i = 0; i < urefs.size(); i++){
        		refs[i] = (String) urefs.get(i).asMap().get("ref").asScalar();
        	}

		}
		@Override
		public int compareTo(DataPallete o) {
			if(wsId < o.wsId) return -1;
			else if(wsId > o.wsId) return 1;
			
			//We expect that worksapce can have DataPallete objects with the same objId
			//thus we do not have to check obj ids 
			return version < o.version ? -1 : (version >o.version ? 1 :0);
		}
	}
    private List<ObjectStatusEvent> reconstructDataPalletObjectStatusEvents(List<ObjDescriptor> dataPalettes) throws IOException, JsonClientException {
    	
    	// Get data palettes
    	WorkspaceClient wsClient = wsClient();    	
    	List<ObjectSpecification> objSpecs = new ArrayList<ObjectSpecification>();
    	for(ObjDescriptor od: dataPalettes){
    		objSpecs.add(new ObjectSpecification().withRef(od.toRef()));
    	}
    	GetObjects2Params params = new GetObjects2Params();
        params.withObjects(objSpecs);
        GetObjects2Results ret = wsClient.getObjects2(params);
        
        List<DataPallete> dps = new ArrayList<DataPallete>();
        for(ObjectData data: ret.getData()){
        	dps.add(new DataPallete(data));
        }
        Collections.sort(dps);
        
        // Reconstruct history of events
        List<ObjectStatusEvent> events = new ArrayList<ObjectStatusEvent>();        
        Set<String> curRefs = new HashSet<String>();         
        for(DataPallete dp: dps){
        	for(String ref: dp.refs){
        		if(!curRefs.contains(ref)){
        			// New ref
        			String[] vals = ref.split("/");
        			ObjectStatusEvent event = new ObjectStatusEvent(
        					null, 
        					WS_STORAGE_CODE, 
        					Integer.parseInt(vals[0]),
        					vals[1],
        					Integer.parseInt(vals[2]),
        					(int)dp.wsId,
        					dp.timestamp,        					
        					DATA_PALETTE_TYPE,
        					ObjectStatusEventType.SHARED);
        				events.add(event);
        		} else{
        			// Exist
        			curRefs.remove(ref);
        		}
        	}
        	// All that were not found are unshared
        	for(String ref: curRefs){
    			String[] vals = ref.split("/");
    			ObjectStatusEvent event = new ObjectStatusEvent(
    					null, 
    					WS_STORAGE_CODE, 
    					Integer.parseInt(vals[0]),
    					vals[1],
    					Integer.parseInt(vals[2]),
    					(int)dp.wsId,
    					dp.timestamp,        					
    					DATA_PALETTE_TYPE,
    					ObjectStatusEventType.UNSHARED);
    			events.add(event);        		
        	}
        	// Set up the current state
        	curRefs.clear();
        	curRefs.addAll(Arrays.asList(dp.refs));
        	
        	
        	System.out.println(dp.wsId + "/" + dp.objId + "/" + dp.version + ": " + dp.timestamp);
        }
        
        System.out.println("=== Events");
        for(ObjectStatusEvent event: events){
        	System.out.println(event);
        }
        
        
//        System.out.println(ret);
        
		return null;
	}

	private List<Long> getWorkspaceIds(boolean excludePublicWorkspaces) throws IOException, JsonClientException{
    	List<Long> wsIds = new ArrayList<Long>();
    	ListWorkspaceInfoParams params = new ListWorkspaceInfoParams();
    	if(excludePublicWorkspaces){
    		params.setExcludeGlobal(1L);
    	}
    	
    	WorkspaceClient wsClient = wsClient();
    	List<Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>>> wsInfos = wsClient.listWorkspaceInfo(params);
    	for(Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo: wsInfos){    		
    		wsIds.add(wsInfo.getE1());    		
    	}    	
    	return wsIds;
    }

    public WorkspaceClient wsClient() throws IOException{
    	if(_wsClient == null){
    		
    		try {
				_wsClient = new WorkspaceClient(wsUrl, token);
			} catch (UnauthorizedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
            _wsClient.setIsInsecureHttpConnectionAllowed(true); 		
    	}
    	return _wsClient;
    }
    	
}
