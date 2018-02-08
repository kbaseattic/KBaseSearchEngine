/*
A KBase module: KBaseSearchEngine
*/

module KBaseSearchEngine {

    /* A boolean. 0 = false, other = true. */
    typedef int boolean;

    /* 
      Global user identificator. It has structure like this:
        <data-source-code>:<full-reference>[:<sub-type>/<sub-id>]
    */
    typedef string GUID;

    /*
      Optional rules of defining constraints for values of particular
      term (keyword). Appropriate field depends on type of keyword.
      For instance in case of integer type 'int_value' should be used.
      In case of range constraint rather than single value 'min_*' 
      and 'max_*' fields should be used. You may omit one of ends of
      range to achieve '<=' or '>=' comparison. Ends are always
      included for range constraints.
    */
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

    /*
      Optional rules of defining constrains for object properties
      including values of keywords or metadata/system properties (like
      object name, creation time range) or full-text search in all
      properties.
      
      boolean exclude_subobjects - don't return any subobjects in the search results if true.
          Default false.
      list<string> source_tags - source tags are arbitrary strings applied to data at the data
          source (for example, the workspace service). The source_tags list may optionally
          populated with a set of tags that will determine what data is returned in a search.
          By default, the list behaves as a whitelist and only data with at least one of the
          tags will be returned.
      source_tags_blacklist - if true, the source_tags list behaves as a blacklist and any
          data with at least one of the tags will be excluded from the search results. If missing
          or false, the default behavior is maintained.
    */
    typedef structure {
        string full_text_in_all;
        string object_name;
        MatchValue timestamp;
        boolean exclude_subobjects;
        mapping<string, MatchValue> lookupInKeys;
        list<string> source_tags;
        boolean source_tags_blacklist;
    } MatchFilter;

    /*
      Optional rules of access constraints.
        - with_private - include data found in workspaces not marked 
            as public, default value is true,
        - with_public - include data found in public workspaces,
            default value is false,
        - with_all_history - include all versions (last one and all
            old versions) of objects matching constrains, default
            value is false.
    */
    typedef structure {
        boolean with_private;
        boolean with_public;
        boolean with_all_history;
    } AccessFilter;

    /*
      Input parameters for search_types method.
    */
    typedef structure {
        MatchFilter match_filter;
        AccessFilter access_filter;
    } SearchTypesInput;

    /*
      Output results of search_types method.
    */
    typedef structure {
        mapping<string, int> type_to_count;
        int search_time;
    } SearchTypesOutput;

    /*
      Search for number of objects of each type matching constraints.
    */
    funcdef search_types(SearchTypesInput params) 
        returns (SearchTypesOutput) authentication required;

    /*
      Rule for sorting found results. 'key_name', 'is_timestamp' and
      'is_object_name' are alternative way of defining what property
      if used for sorting. Default order is ascending (if 
      'descending' field is not set).
    */
    typedef structure {
        boolean is_timestamp;
        boolean is_object_name;
        string key_name;
        boolean descending;
    } SortingRule;

    /*
      Pagination rules. Default values are: start = 0, count = 50.
    */
    typedef structure {
        int start;
        int count;
    } Pagination;

    /*
      Rules for what to return about found objects.
      skip_info - do not include brief info for object ('guid,
          'parent_guid', 'object_name' and 'timestamp' fields in
          ObjectData structure),
      skip_keys - do not include keyword values for object 
          ('key_props' field in ObjectData structure),
      skip_data - do not include raw data for object ('data' and 
          'parent_data' fields in ObjectData structure),
      ids_only - shortcut to mark all three skips as true.
    */
    typedef structure {
        boolean ids_only;
        boolean skip_info;
        boolean skip_keys;
        boolean skip_data;
        list<string> data_includes;
    } PostProcessing;

    /*
      Input parameters for 'search_objects' method.
      object_types - list of the types of objects to search on (optional). The
                     function will search on all objects if the list is not specified
                     or is empty. The list size must be less than 50.
      match_filter - see MatchFilter (optional).
      sorting_rules - see SortingRule (optional).
      access_filter - see AccessFilter (optional).
      pagination - see Pagination (optional).
      post_processing - see PostProcessing (optional).
    */
    typedef structure {
        list<string> object_types;
        MatchFilter match_filter;
        list<SortingRule> sorting_rules;
        AccessFilter access_filter;
        Pagination pagination;
        PostProcessing post_processing;
    } SearchObjectsInput;

    /*
      Properties of found object including metadata, raw data and
          keywords.
          
      mapping<string, string> object_props - general properties for all objects. This mapping
          contains the keys 'creator', 'copied', 'module', 'method', 'module_ver', and 'commit' -
          respectively the user that originally created the object, the user that copied this
          incarnation of the object, and the module and method used to create the object and
          their version and version control commit hash. Not all keys may be present; if not
          their values were not available in the search data.
    */
    typedef structure {
        GUID guid;
        GUID parent_guid;
        string object_name;
        int timestamp;
        UnspecifiedObject parent_data;
        UnspecifiedObject data;
        mapping<string, string> key_props;
        mapping<string, string> object_props;
    } ObjectData;

    /*
      Output results for 'search_objects' method.
      'pagination' and 'sorting_rules' fields show actual input for
          pagination and sorting.
      total - total number of found objects.
      search_time - common time in milliseconds spent.
    */
    typedef structure {
        Pagination pagination;
        list<SortingRule> sorting_rules;
        list<ObjectData> objects;
        int total;
        int search_time;
    } SearchObjectsOutput;

    /*
      Search for objects of particular type matching constraints.
    */
    funcdef search_objects(SearchObjectsInput params)
        returns (SearchObjectsOutput) authentication required;

    /*
      Input parameters for get_objects method.
    */
    typedef structure {
        list<GUID> guids;
        PostProcessing post_processing;
    } GetObjectsInput;

    /*
      Output results of get_objects method.
    */
    typedef structure {
        list<ObjectData> objects;
        int search_time;
    } GetObjectsOutput;

    /*
      Retrieve objects by their GUIDs.
    */
    funcdef get_objects(GetObjectsInput params)
        returns (GetObjectsOutput) authentication required;

    /*
      Input parameters for list_types method.
      type_name - optional parameter; if not specified all types are described.
    */
    typedef structure {
        string type_name;
    } ListTypesInput;

    /*
      Description of searchable type keyword. 
          - key_value_type can be one of {'string', 'integer', 'double', 
            'boolean'},
          - hidden - if true then this keyword provides values for other
            keywords (like in 'link_key') and is not supposed to be shown.
          - link_key - optional field pointing to another keyword (which is
            often hidden) providing GUID to build external URL to.
    */
    typedef structure {
        string key_name;
        string key_ui_title;
        string key_value_type;
        boolean hidden;
        string link_key;
    } KeyDescription;

    /*
      Description of searchable object type including details about keywords.
      TODO: add more details like parent type, primary key, ...
    */
    typedef structure {
        string type_name;
        string type_ui_title;
        list<KeyDescription> keys;
    } TypeDescriptor;

    /*
      Output results of list_types method.
    */
    typedef structure {
        mapping<string, TypeDescriptor> types;
    } ListTypesOutput;

    /*
      List registered searchable object types.
    */
    funcdef list_types(ListTypesInput params)
        returns (ListTypesOutput);
};
