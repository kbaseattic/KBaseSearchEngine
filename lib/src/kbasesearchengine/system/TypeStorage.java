package kbasesearchengine.system;

import java.util.List;

import kbasesearchengine.events.exceptions.IndexingException;

public interface TypeStorage {
    
    ObjectTypeParsingRules getObjectType(SearchObjectType type)
            throws IndexingException, NoSuchTypeException;

    List<ObjectTypeParsingRules> listObjectTypes() throws IndexingException;
    
    List<ObjectTypeParsingRules> listObjectTypesByStorageObjectType(
            StorageObjectType storageObjectType)
            throws IndexingException;
}
