package kbaserelationengine.system;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import kbaserelationengine.parse.ObjectParseException;

public class DefaultSystemStorage implements SystemStorage {
    
    //TODO JAVADOC
    //TODO TEST
    
    private List<ObjectTypeParsingRules> parsingRules;
    
    public DefaultSystemStorage(final Path typesDir) 
            throws ObjectParseException, IOException {
        this.parsingRules = new ArrayList<>();
        // this is gross, but works. https://stackoverflow.com/a/20130475/643675
        for (Path file : (Iterable<Path>) Files.list(typesDir)::iterator) {
            if (Files.isRegularFile(file) && file.toString().endsWith(".json")) {
                parsingRules.add(ObjectTypeParsingRules.fromFile(file.toFile()));
            }
        }
    }
    
    public DefaultSystemStorage(
            final Path typesDir,
            final Path mappingsDir,
            final Map<String, TypeMappingParser> parsers) {
        
        // TODO NOW Auto-generated constructor stub
    }
    
    @Override
    public List<ObjectTypeParsingRules> listObjectTypes() throws IOException {
        return parsingRules;
    }
    
    @Override
    public ObjectTypeParsingRules getObjectType(String type)
            throws IOException {
        for (ObjectTypeParsingRules rule : parsingRules) {
            if (rule.getGlobalObjectType().equals(type)) {
                return rule;
            }
        }
        return null;
    }
    
    @Override
    public List<ObjectTypeParsingRules> listObjectTypesByStorageObjectType(
            final StorageObjectType storageObjectType) throws IOException {
        final List<ObjectTypeParsingRules> ret = new LinkedList<>();
        for (ObjectTypeParsingRules rule : parsingRules) {
            if (rule.getStorageObjectType().equals(storageObjectType)) {
                ret.add(rule);
            }
        }
        return ret;
    }
}
