package kbaserelationengine.relations;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import kbaserelationengine.common.GUID;

public class DefaultRelationStorage implements RelationStorage {

    @Override
    public void addRelations(List<Relation> links) throws IOException {
        // TODO: implement
    }
    
    @Override
    public List<Relation> lookup(GUID id1, String type1, GUID id2, String type2,
            String linkType, Map<String, Object> linkProps) throws IOException {
        throw new IllegalStateException("Method is not supported yet");
    }
}
