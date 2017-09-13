package kbasesearchengine.system;

import java.io.IOException;
import java.util.List;

public interface TypeStorage {
    
    ObjectTypeParsingRules getObjectType(String type) throws IOException;

    List<ObjectTypeParsingRules> listObjectTypes() throws IOException;
    
    List<ObjectTypeParsingRules> listObjectTypesByStorageObjectType(
            StorageObjectType storageObjectType) throws IOException;
}
