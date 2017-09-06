package kbaserelationengine.system;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import kbaserelationengine.parse.ObjectParseException;

public class DefaultSystemStorage implements SystemStorage {
    
    /* Not totally clear what this class is supposed to do other than handle the parsing rules
     * files. It seems like it needs more functionality based on the unimplemented methods below
     * but I'm not clear on what that functionality is supposed to be. It seems like this class
     * should be focused on handling the parsing rules files and any other functionality should
     * go in a different class.
     */
    private List<ObjectTypeParsingRules> parsingRules;
    
    public DefaultSystemStorage(File typesDir) 
            throws ObjectParseException, IOException {
        this.parsingRules = new ArrayList<>();
        for (File file : typesDir.listFiles()) {
            if (file.isFile() && file.getName().endsWith(".json")) {
                parsingRules.add(ObjectTypeParsingRules.fromFile(file));
            }
        }
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
            String storageObjectType) throws IOException {
        final List<ObjectTypeParsingRules> ret = new LinkedList<>();
        for (ObjectTypeParsingRules rule : parsingRules) {
            if (rule.getStorageObjectType() == null) {
                continue;
            }
            if (rule.getStorageObjectType().equals(storageObjectType)) {
                ret.add(rule);
            }
        }
        return ret;
    }
}
