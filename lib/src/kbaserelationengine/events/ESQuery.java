package kbaserelationengine.events;

import java.util.LinkedHashMap;
import java.util.Map;

public class ESQuery {
	ESTerm root = new ESTerm(null, null);
	ESTerm current = root;
	
	class ESTerm {
		
		ESTerm parent;
		String name;
		Map<String, Object> data = new LinkedHashMap<String, Object>();
		ESTerm(ESTerm parent, String name){
			this.parent = parent;
			this.name = name;
			if(parent != null){
				parent.put(name, this);
			}
		}		
		
		void put(String key, Object value){
			data.put(key, value);
		}
	} 
				
	
	public ESQuery term(String name){
		ESTerm t = new ESTerm(current, name);
		current = t;
		return this;
	}
	
	public ESQuery value(String key, Object value){
		current.put(key, value);
		return this;		
	}
	
	public ESQuery back(){
		current = current.parent;
		return this;
	}
	
	public Map<String, Object> document(){
		return root.data;
	}
	
}
