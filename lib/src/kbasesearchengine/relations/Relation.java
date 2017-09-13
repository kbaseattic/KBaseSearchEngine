package kbasesearchengine.relations;

import java.util.Map;

import kbasesearchengine.common.GUID;

public class Relation {
    private GUID id1;
    private String type1; 
    private GUID id2;
    private String type2;
    private String linkType;
    private Map<String, Object> linkProps;
    private Long accessGroupId;
    
    public GUID getId1() {
        return id1;
    }
    
    public void setId1(GUID id1) {
        this.id1 = id1;
    }
    
    public String getType1() {
        return type1;
    }
    
    public void setType1(String type1) {
        this.type1 = type1;
    }
    
    public GUID getId2() {
        return id2;
    }
    
    public void setId2(GUID id2) {
        this.id2 = id2;
    }
    
    public String getType2() {
        return type2;
    }
    
    public void setType2(String type2) {
        this.type2 = type2;
    }
    
    public String getLinkType() {
        return linkType;
    }
    
    public void setLinkType(String linkType) {
        this.linkType = linkType;
    }
    
    public Map<String, Object> getLinkProps() {
        return linkProps;
    }
    
    public void setLinkProps(Map<String, Object> linkProps) {
        this.linkProps = linkProps;
    }
    
    public Long getAccessGroupId() {
        return accessGroupId;
    }
    
    public void setAccessGroupId(Long accessGroupId) {
        this.accessGroupId = accessGroupId;
    }
}
