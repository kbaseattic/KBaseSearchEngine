package kbaserelationengine.system;

import java.io.IOException;
import java.util.List;

public interface SystemStorage {
    
    ObjectTypeParsingRules getObjectType(String type) throws IOException;

    List<ObjectTypeParsingRules> listObjectTypes() throws IOException;
    
    List<ObjectTypeParsingRules> listObjectTypesByStorageObjectType(String storageObjectType)
            throws IOException;
}
