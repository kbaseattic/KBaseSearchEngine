package kbaserelationengine.parse;

import java.io.IOException;

public interface RelationStorage {
    public void addRelation(String id1, String id2, String relationType) throws IOException;
}
