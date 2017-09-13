package kbasesearchengine.events.reconstructor;

import java.util.List;

import us.kbase.common.service.UObject;
import workspace.ObjectData;

public class DataPalette implements Comparable<DataPalette> {
	
	
	long wsId;
	long objId;
	long version;
	long timestamp;
	String[] refs;
	DataPalette(ObjectData data){
		this.wsId = data.getInfo().getE7();
		this.objId = data.getInfo().getE1();
		this.version = data.getInfo().getE5();
		this.timestamp = Util.DATE_PARSER.parseDateTime(data.getInfo().getE4()).getMillis();
		List<UObject> urefs = data.getData().asMap().get("data").asList();
		refs = new String[urefs.size()];
    	for(int i = 0; i < urefs.size(); i++){
    		refs[i] = (String) urefs.get(i).asMap().get("ref").asScalar();
    	}

	}
	@Override
	public int compareTo(DataPalette o) {
		if(wsId < o.wsId) return -1;
		else if(wsId > o.wsId) return 1;
		
		//We expect that worksapce can have DataPallete objects with the same objId
		//thus we do not have to check obj ids 
		return version < o.version ? -1 : (version >o.version ? 1 :0);
	}
}
