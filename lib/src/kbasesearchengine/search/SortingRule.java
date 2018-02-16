package kbasesearchengine.search;

public class SortingRule {

    //object name is not sortable right now. Just going to use keyName
//    public boolean isObjectName;

    public boolean isTimestamp;
    //cheat right not b/c workspaceId or access_group_id is not exposed to front end
    public boolean isWorkspaceId;

    //TODO some fields are not sortable right now. Will need to add checks and validations later
    public String keyName;
    public boolean ascending;
}
