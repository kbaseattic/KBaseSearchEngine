package kbaserelationengine.system;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import kbaserelationengine.common.GUID;
import kbaserelationengine.parse.ObjectParseException;
import us.kbase.auth.AuthToken;

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
        List<ObjectTypeParsingRules> ret = null;
        for (ObjectTypeParsingRules rule : parsingRules) {
            if (rule.getStorageObjectType() == null) {
                continue;
            }
            if (rule.getStorageObjectType().equals(storageObjectType)) {
                if (ret == null) {
                    ret = new ArrayList<>();
                }
                ret.add(rule);
            }
        }
        return ret;
    }
    
    @Override
    public String getTypeForObjectId(GUID id) throws IOException {
        throw new IllegalStateException("Method is not supported yet");
    }
    
    @Override
    public Set<GUID> normalizeObjectIds(Set<Object> typedIds, String type)
            throws IOException {
        throw new IllegalStateException("Method is not supported yet");
    }
    
    @Override
    public Set<GUID> collapseVersions(Set<GUID> ids) throws IOException {
        throw new IllegalStateException("Method is not supported yet");
    }
    
    @Override
    public Set<GUID> filterObjectIdsForUser(String user, AuthToken userToken,
            Set<GUID> ids) throws IOException {
        throw new IllegalStateException("Method is not supported yet");
    }
}
