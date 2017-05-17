package kbaserelationengine.system;

import kbaserelationengine.common.ObjectJsonPath;

public class IndexingRules {
    private ObjectJsonPath path = null;
    private boolean fullText = false;
    private String keywordType = null;
    private String keyName = null;
    private String transform = null;
    private boolean fromParent = false;
    private boolean derivedKey = false;
    private boolean notIndexed = false;
    private String sourceKey = null;
    private String targetObjectType = null;
    private String subobjectIdKey = null;
    private Object optionalDefaultValue = null;
    private Object constantValue = null;
    private String uiName = null;
    private boolean uiHidden = false;
    private String uiLinkKey = null;
    
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
    
    public String getTransform() {
        return transform;
    }
    
    public void setTransform(String transform) {
        this.transform = transform;
    }
    
    public boolean isFromParent() {
        return fromParent;
    }
    
    public void setFromParent(boolean fromParent) {
        this.fromParent = fromParent;
    }
    
    public boolean isDerivedKey() {
        return derivedKey;
    }
    
    public void setDerivedKey(boolean derivedKey) {
        this.derivedKey = derivedKey;
    }
    
    public boolean isNotIndexed() {
        return notIndexed;
    }
    
    public void setNotIndexed(boolean notIndexed) {
        this.notIndexed = notIndexed;
    }
    
    public String getSourceKey() {
        return sourceKey;
    }
    
    public void setSourceKey(String sourceKey) {
        this.sourceKey = sourceKey;
    }
    
    public String getTargetObjectType() {
        return targetObjectType;
    }
    
    public void setTargetObjectType(String targetObjectType) {
        this.targetObjectType = targetObjectType;
    }
    
    public String getSubobjectIdKey() {
        return subobjectIdKey;
    }
    
    public void setSubobjectIdKey(String subobjectIdKey) {
        this.subobjectIdKey = subobjectIdKey;
    }
    
    public Object getConstantValue() {
        return constantValue;
    }
    
    public void setConstantValue(Object constantValue) {
        this.constantValue = constantValue;
    }
    
    public Object getOptionalDefaultValue() {
        return optionalDefaultValue;
    }
    
    public void setOptionalDefaultValue(Object optionalDefaultValue) {
        this.optionalDefaultValue = optionalDefaultValue;
    }
    
    public String getUiName() {
        return uiName;
    }
    
    public void setUiName(String uiName) {
        this.uiName = uiName;
    }
    
    public boolean isUiHidden() {
        return uiHidden;
    }
    
    public void setUiHidden(boolean uiHidden) {
        this.uiHidden = uiHidden;
    }
    
    public String getUiLinkKey() {
        return uiLinkKey;
    }
    
    public void setUiLinkKey(String uiLinkKey) {
        this.uiLinkKey = uiLinkKey;
    }

    @Override
    public String toString() {
        return "IndexingRules [path=" + path + ", fullText=" + fullText
                + ", keywordType=" + keywordType + ", keyName=" + keyName
                + ", transform=" + transform + ", fromParent=" + fromParent
                + ", derivedKey=" + derivedKey + ", notIndexed=" + notIndexed
                + ", sourceKey=" + sourceKey + ", targetObjectType="
                + targetObjectType + ", subobjectIdKey=" + subobjectIdKey
                + ", optionalDefaultValue=" + optionalDefaultValue
                + ", constantValue=" + constantValue + ", uiName=" + uiName
                + ", uiHidden=" + uiHidden + ", uiLinkKey=" + uiLinkKey + "]";
    }
    
}
