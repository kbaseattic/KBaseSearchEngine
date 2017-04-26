package kbaserelationengine.events.storage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ESQuery {
	ESTerm root = new ESTerm(null, null, TermType.MAP);
	ESTerm current = root;
	
	enum TermType{
		MAP, ARRAY
	}
	
	
	class ESTerm {
		
		ESTerm parent;
		String name;
		TermType type;
		Object data; 
		
		
		ESTerm(ESTerm parent, String name, TermType type){
			this.parent = parent;
			this.name = name;
			this.type = type;
			
			if(type == TermType.MAP){
				data =  new LinkedHashMap<String, Object>();
			} else if(type == TermType.ARRAY){
				data =  new ArrayList<Object>();
			}			
			if(parent != null){
				parent.put(name, data);
			}
		}		
		
		@SuppressWarnings("unchecked")
		void put(String key, Object value){
			if(type == TermType.MAP){
				((Map<String, Object>)data).put(key, value);
			} else if (type == TermType.ARRAY){
				((List<Object>)data).add(value);			
			}
		}
	} 
				
	
	public ESQuery map(String name){
		ESTerm t = new ESTerm(current, name, TermType.MAP);
		current = t;
		return this;
	}
	
	public ESQuery array(String name){
		ESTerm t = new ESTerm(current, name, TermType.ARRAY);
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
	
	@SuppressWarnings("unchecked")
	public Map<String, Object> document(){
		return (Map<String, Object>)root.data;
	}
	
}
