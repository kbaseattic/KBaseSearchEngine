package kbaserelationengine.system;

import kbaserelationengine.common.ObjectJsonPath;

public class IndexingRules {
    private ObjectJsonPath path;
    private boolean fullText = false;
    private String keywordType = null;
    private String keyName = null;
    
    public ObjectJsonPath getPath() {
        return path;
    }
    
    public void setPath(ObjectJsonPath path) {
        this.path = path;
    }
    
    public boolean isFullText() {
        return fullText;
    }
    
    public void setFullText(boolean fullText) {
        this.fullText = fullText;
    }

    public String getKeywordType() {
        return keywordType;
    }
    
    public void setKeywordType(String keywordType) {
        this.keywordType = keywordType;
    }
    
    public String getKeyName() {
        return keyName;
    }
    
    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }
}
