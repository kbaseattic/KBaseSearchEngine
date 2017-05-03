package kbaserelationengine.search;

import java.util.List;
import java.util.Set;

import kbaserelationengine.common.GUID;

public class FoundIds {
    public Pagination pagination;
    public List<SortingRule> sortingRules;
    public Set<GUID> guids;
    public int total;
}
