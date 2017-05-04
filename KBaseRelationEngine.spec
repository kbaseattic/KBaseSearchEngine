/*
A KBase module: KBaseRelationEngine
*/

module KBaseRelationEngine {

    /* A boolean. 0 = false, other = true. */
    typedef int boolean;

    /* 
      Global user identificator. It has structure like this:
        <data-source-code>:<full-reference>[:<sub-type>/<sub-id>]
    */
    typedef string GUID;

    typedef structure {
        string ws_name;
        int ws_id;
    } AddWorkspaceToIndexInput;

    /*
      This operation means that given workspace will be shared with
      system indexing user with write access. User calling this
      function should be owner of this workspace.
    */
    funcdef add_workspace_to_index(AddWorkspaceToIndexInput params) 
        returns () authentication required;

    typedef structure {
        string value;
        int int_value;
        float double_value;
        boolean bool_value;
        int min_int;
        int max_int;
        int min_date;
        int max_date;
        float min_double;
        float max_double;
    } MatchValue;

    typedef structure {
        string full_text_in_all;
        int access_group_id;
        string object_name;
        GUID parent_guid;
        MatchValue timestamp;
        mapping<string, MatchValue> lookupInKeys;
    } MatchFilter;

    typedef structure {
        boolean with_private;
        boolean with_public;
        boolean with_all_history;
    } AccessFilter;

    typedef structure {
        MatchFilter match_filter;
        AccessFilter access_filter;
    } SearchTypesInput;

    typedef structure {
        mapping<string, int> type_to_count;
        int search_time;
    } SearchTypesOutput;

    funcdef search_types(SearchTypesInput params) 
        returns (SearchTypesOutput) authentication required;

    typedef structure {
        boolean is_timestamp;
        boolean is_object_name;
        string key_name;
        boolean descending;
    } SortingRule;

    typedef structure {
        int start;
        int count;
    } Pagination;

    /*
      ids_only - shortcut to mark all three skips as true.
    */
    typedef structure {
        boolean ids_only;
        boolean skip_info;
        boolean skip_keys;
        boolean skip_data;
        list<string> data_includes;
    } PostProcessing;

    typedef structure {
        string object_type;
        MatchFilter match_filter;
        list<SortingRule> sorting_rules;
        AccessFilter access_filter;
        Pagination pagination;
        PostProcessing post_processing;
    } SearchObjectsInput;

    typedef structure {
        GUID guid;
        GUID parent_guid;
        string object_name;
        int timestamp;
        UnspecifiedObject parent_data;
        UnspecifiedObject data;
        mapping<string, string> key_props;
    } ObjectData;

    typedef structure {
        Pagination pagination;
        list<SortingRule> sorting_rules;
        list<ObjectData> objects;
        int total;
        int search_time;
    } SearchObjectsOutput;

    funcdef search_objects(SearchObjectsInput params)
        returns (SearchObjectsOutput) authentication required;

    typedef structure {
        list<GUID> guids;
        PostProcessing post_processing;
    } GetObjectsInput;

    typedef structure {
        list<ObjectData> objects;
        int search_time;
    } GetObjectsOutput;

    funcdef get_objects(GetObjectsInput params)
        returns (GetObjectsOutput) authentication required;

    /*
        type_name - optional parameter; if not specified all types are described.
    */
    typedef structure {
        string type_name;
    } ListTypesInput;

    typedef structure {
        string key_name;
        string key_ui_title;
        string key_value_type;
    } KeyDescription;

    /*
      TODO: add more details like parent type, relations, primary key, ...
    */
    typedef structure {
        string type_name;
        string type_ui_title;
        list<KeyDescription> keys;
    } TypeDescriptor;

    typedef structure {
        mapping<string, TypeDescriptor> types;
    } ListTypesOutput;

    funcdef list_types(ListTypesInput params)
        returns (ListTypesOutput);
};
