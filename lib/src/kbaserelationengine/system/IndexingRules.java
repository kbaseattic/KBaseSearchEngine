package kbaserelationengine.system;

public class IndexingRules {
    private boolean fullText;
    private String keywordType;
    
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
}
