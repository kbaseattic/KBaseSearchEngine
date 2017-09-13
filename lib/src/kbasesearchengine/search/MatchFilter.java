package kbasesearchengine.search;

import java.util.LinkedHashMap;
import java.util.Map;

import kbasesearchengine.common.GUID;

public class MatchFilter {
    public String fullTextInAll = null;
    public Integer accessGroupId = null;
    public String objectName = null;
    public GUID parentGuid = null;
    public MatchValue timestamp = null;
    public Map<String, MatchValue> lookupInKeys = null;

    public MatchFilter() {}
    
    public static MatchFilter create() {
        return new MatchFilter();
    }
    
    public MatchFilter withFullTextInAll(String text) {
        this.fullTextInAll = text;
        return this;
    }
    
    public MatchFilter withAccessGroupId(Integer accessGroupId) {
        this.accessGroupId = accessGroupId;
        return this;
    }
    
    public MatchFilter withObjectName(String partOfName) {
        this.objectName = partOfName;
        return this;
    }
    
    public MatchFilter withParentGuid(GUID parentGuid) {
        this.parentGuid = parentGuid;
        return this;
    }
    
    public MatchFilter withTimestamp(MatchValue value) {
        this.timestamp = value;
        return this;
    }
    
    public MatchFilter withLookupInKeys(Map<String, MatchValue> keys) {
        if (lookupInKeys == null) {
            this.lookupInKeys = new LinkedHashMap<>(keys);
        } else {
            this.lookupInKeys.putAll(keys);
        }
        return this;
    }

    public MatchFilter withLookupInKey(String keyName, String value) {
        return withLookupInKey(keyName, new MatchValue(value));
    }

    public MatchFilter withLookupInKey(String keyName, MatchValue value) {
        if (this.lookupInKeys == null) {
            this.lookupInKeys = new LinkedHashMap<>();
        }
        this.lookupInKeys.put(keyName, value);
        return this;
    }
}
