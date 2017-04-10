package kbaserelationengine.system;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import kbaserelationengine.common.GUID;

public interface SystemStorage {
    
    public ObjectTypeParsingRules getObjectType(String type) throws IOException;

    public List<ObjectTypeParsingRules> listObjectTypes() throws IOException;

    public Set<GUID> filterObjectIdsForUser(String user, Set<GUID> ids) throws IOException;
    
    public Set<GUID> collapseVersions(Set<GUID> ids) throws IOException;
    
    public String getTypeForObjectId(GUID id) throws IOException;
    
    public Set<GUID> normalizeObjectIds(Set<Object> typedIds, String type) throws IOException;
}
