package kbaserelationengine.events;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import kbaserelationengine.common.GUID;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple9;
import us.kbase.common.service.UnauthorizedException;
import workspace.ListObjectsParams;
import workspace.ListWorkspaceInfoParams;
import workspace.WorkspaceClient;

public class WSEventTracker {
	
	private static String WS_STORAGE_CODE = "WS";
	private static String DATA_PALETTE_TYPE = "DataPalette.DataPalette";
	
	private URL wsUrl;
	private AuthToken token;
	private ObjectStatusStorage objStatusStorage;
	
	private WorkspaceClient _wsClient;
	private List<ObjectStatusEventListener> eventListeners = new ArrayList<ObjectStatusEventListener>(); 
	
	public WSEventTracker(URL wsUrl, AuthToken token, ObjectStatusStorage objStatusStorage) {
		this.wsUrl = wsUrl;
		this.token = token;
		this.objStatusStorage = objStatusStorage;
	}
		
	public void registerEventListener(ObjectStatusEventListener listener){
		eventListeners.add(listener);
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
		Long wsId;
		Long objId;
		Long version;
		String dataType;
		boolean isDeleted;
		boolean isProcessed;
		
		ObjectStatusEventType evetType;
		
		public ObjDescriptor(Long wsId, Long objId, Long version, String dataType, boolean isDeleted) {
			super();
			this.wsId = wsId;
			this.objId = objId;
			this.version = version;
			this.dataType = dataType;
			this.isDeleted = isDeleted;
		}
	}
	
	
	public void doPrivateWorkspaces() throws IOException, JsonClientException{
    	WorkspaceClient wsClient = wsClient();
    	
    	// To collect data palettes from all workspaces
    	List<ObjDescriptor> dataPalettes = new ArrayList<ObjDescriptor>();    	

    	
    	// Objects which are candidates for create/update/delete events 
    	Map<Long,List<ObjDescriptor>> objCandidates = new Hashtable<Long,List<ObjDescriptor>>(); 

    	
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
        	
        	processObjectCandidates(wsId, objCandidates);   
    	}
    	
    	// Process all palettes. We should do it only after events like create/update/detele are done
    	processDataPalettes(dataPalettes);
	}
	
	private void collectObjects(List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows,
			Map<Long, List<ObjDescriptor>> objCandidates,
			List<ObjDescriptor> dataPalettes, boolean isDeleted) {
		
    	for(Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>> row: rows){
    		    		
    		ObjDescriptor od = new ObjDescriptor(row.getE7(), row.getE1(),  row.getE5(), row.getE3().split("-")[0], isDeleted);
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
			Map<Long, List<ObjDescriptor>> objCandidates) throws IOException {
		
		List<GUID> guids = new ArrayList<GUID>();
		for(Long objId: objCandidates.keySet()){
			guids.add(new GUID(WS_STORAGE_CODE, wsId.intValue(), objId.toString(), null, null, null));
		}
	
		// Mark objects that were already indexed before
		List<ObjectStatus> objPrevStates = objStatusStorage.find(WS_STORAGE_CODE, false, guids);
		for(ObjectStatus op: objPrevStates){
			List<ObjDescriptor> candidates = objCandidates.get(op.getAccessGroupObjectId());
			if(candidates != null){
				for(ObjDescriptor od: candidates){
					if(od.version.equals(op.getVersion())){
						od.isProcessed = true;
					}
				}
			}
		}
		
		// Define eventType for all nonindexed ones and raise event
		for(Map.Entry<Long, List<ObjDescriptor>> entry: objCandidates.entrySet()){
			for(ObjDescriptor od: entry.getValue()){
				if(od.isProcessed ) continue;
				
				if(od.isDeleted){
					raiseEvent(od, ObjectStatusEventType.DELETED);
				} else{
					if(od.version.intValue() == 1){
						raiseEvent(od, ObjectStatusEventType.CREATED);
					} else{
						raiseEvent(od, ObjectStatusEventType.NEW_VERSION);						
					}
				}
			} 
		}		
	}

	private void raiseEvent(ObjDescriptor od, ObjectStatusEventType eventType) throws IOException {
		ObjectStatus os = new ObjectStatus(null,WS_STORAGE_CODE,od.wsId.intValue(), od.objId.toString(), od.version.intValue(), od.dataType, eventType);
		for(ObjectStatusEventListener listener: eventListeners){
			listener.statusChanged(os);
		}		
	}

	private void processDataPalettes(List<ObjDescriptor> dataPalettes) {
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
