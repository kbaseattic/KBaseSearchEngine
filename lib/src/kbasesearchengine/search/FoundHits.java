package kbasesearchengine.search;

import java.util.List;
import java.util.Set;

import kbasesearchengine.common.GUID;

public class FoundHits {
    
    //TODO CODE everthing about this class
    
    public Pagination pagination;
    public List<SortingRule> sortingRules;
    public Set<GUID> guids;
    public List<ObjectData> objects;
    public int total;
}
