package kbasesearchengine.relations;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import kbasesearchengine.common.GUID;

public interface RelationStorage {
    public void addRelations(List<Relation> links) throws IOException;
    
    public List<Relation> lookup(GUID id1, String type1,
            GUID id2, String type2, String linkType,
            Map<String, Object> linkProps) throws IOException;
}
