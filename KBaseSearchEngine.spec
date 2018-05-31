/*
A KBase module: KBaseSearchEngine
*/

#include <workspace.spec>

module KBaseSearchEngine {

    /* A boolean. 0 = false, other = true. */
    typedef int boolean;

    /* An X/Y/Z style reference */
    typedef string obj_ref;

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
          source (for example, the workspace service). The source_tags list may optionally be
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
        mapping<string, MatchValue> lookup_in_keys;
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
      Rule for sorting results. 
      
      string property - the property to sort on. This may be a an object property - e.g. a 
          field inside the object - or a standard property possessed by all objects, like a
          timestamp or creator.
      boolean is_object_property - true (the default) to specify an object property, false to
          specify a standard property.
      boolean ascending - true (the default) to sort ascending, false to sort descending.
    */
    typedef structure {
        string property;
        boolean is_object_property;
        boolean ascending;
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
      skip_keys - do not include keyword values for object 
          ('key_props' field in ObjectData structure),
      skip_data - do not include raw data for object ('data' and 
          'parent_data' fields in ObjectData structure),
      include_highlight - include highlights of fields that
           matched query,
      ids_only - shortcut to mark both skips as true and 
           include_highlight as false.
      add_narrative_info - if true, narrative info gets added to the
           search results. Default is false.
      add_access_group_info - if true, access groups and objects info get added
           to the search results. Default is false.
    */
    typedef structure {
        boolean ids_only;
        boolean skip_keys;
        boolean skip_data;
        boolean include_highlight;
        boolean add_narrative_info;
        boolean add_access_group_info;
    } PostProcessing;

    /*
      Input parameters for 'search_objects' method.
      object_types - list of the types of objects to search on (optional). The
                     function will search on all objects if the list is not specified
                     or is empty. The list size must be less than 50.
      match_filter - see MatchFilter.
      sorting_rules - see SortingRule (optional).
      access_filter - see AccessFilter.
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
      Properties of an object including metadata, raw data and keywords.
      GUID guid - the object's guid.
      GUID parent_guid - the guid of the object's parent if the object is a subobject (e.g.
          features for genomes).
      object_name - the object's name.
      timestamp - the creation date for the object in milliseconds since the epoch.
      string type - the type of the data in the search index.
      int type_ver - the version of the search type.
      string creator - the username of the user that created that data.
      string copier - if this instance of the data is a copy, the username of the user that
          copied the data.
      string mod - the name of the KBase SDK module that was used to create the data.
      string method - the name of the method in the KBase SDK module that was used to create the
          data.
      string module_ver - the version of the KBase SDK module that was used to create the data.
      string commit - the version control commit hash of the KBase SDK module that was used to
          create the data. 
      parent_data - raw data extracted from the subobject's parent object. The data contents will
          vary from object to object. Null if the object is not a subobject.
      data - raw data extracted from the object. The data contents will vary from object to object.
      key_props - keyword properties of the object. These fields have been extracted from the object
         and possibly transformed based on the search specification for the object.
         The contents will vary from object to object.
      mapping<string, list<string>> highlight - The keys are the field names and the list 
          contains the sections in each field that matched the search query. Fields with no
          hits will not be available. Short fields that matched are shown in their entirety.
          Longer fields are shown as snippets preceded or followed by "...".     
    */
    typedef structure {
        GUID guid;
        GUID parent_guid;
        string object_name;
        int timestamp;
        string type;
        int type_ver;
        string creator;
        string copier;
        string mod;
        string method;
        string module_ver;
        string commit;
        
        UnspecifiedObject parent_data;
        UnspecifiedObject data;
        mapping<string, string> key_props;
        mapping<string, list<string>> highlight;
    } ObjectData;

    /* A data source access group ID (for instance, the integer ID of a workspace). */
    typedef int access_group_id;
    
    /* A timestamp in milliseconds since the epoch. */
    typedef int timestamp;

    /* Information about a workspace, which may or may not contain a KBase Narrative.
       This data is specific for data from the Workspace Service.
       
       string narrative_name - the name of the narrative contained in the workspace, or null if
           the workspace does not contain a narrative.
       int narrative_id - the id of the narrative contained in the workspace, or null.
       timestamp time_last_saved - the modification date of the workspace.
       string ws_owner_username - the unique user name of the workspace's owner.
       string ws_owner_displayname - the display name of the workspace's owner.
    */
    typedef tuple<string narrative_name,
                  int narrative_id,
                  timestamp time_last_saved,
                  string ws_owner_username,
                  string ws_owner_displayname
                  > narrative_info;

    /*
    The access_group_info and object_info are meant to be abstractions for info from multiple data sources.
    Until other data sources become available, definitions pertaining to Workspace are being used.
    When other data sources are available, the following variables will be moved from
    this concrete workspace definitions, to structures with higher level abstractions.
    */

    typedef Workspace.workspace_info  access_group_info;
    typedef Workspace.object_info     object_info;

    /*
      Output results for 'search_objects' method.
      'pagination' and 'sorting_rules' fields show actual input for
          pagination and sorting.
      total - total number of found objects.
      search_time - common time in milliseconds spent.
      mapping<access_group_id, narrative_info> access_group_narrative_info - information about
         the workspaces in which the objects in the results reside. This data only applies to
         workspace objects.
      mapping<access_group_id, access_group_info> access_groups_info - information about
         the access groups in which the objects in the results reside. Currently this data only applies to
         workspace objects. The tuple9 value returned by get_workspace_info() for each workspace
         in the search results is saved in this mapping. In future the access_group_info will be
         replaced with a higher level abstraction.
      mapping<obj_ref, object_info> objects_info - information about each object in the
         search results. Currently this data only applies to workspace objects. The tuple11 value
         returned by get_object_info3() for each object in the search results is saved in the mapping.
         In future the object_info will be replaced with a higher level abstraction.
    */
    typedef structure {
        Pagination pagination;
        list<SortingRule> sorting_rules;
        list<ObjectData> objects;
        int total;
        int search_time;
        mapping<access_group_id, narrative_info> access_group_narrative_info;
        mapping<access_group_id, access_group_info> access_groups_info;
        mapping<obj_ref, object_info> objects_info;
    } SearchObjectsOutput;

    /*
      Search for objects of particular type matching constraints.
    */
    funcdef search_objects(SearchObjectsInput params)
        returns (SearchObjectsOutput) authentication required;

    /*
      Input parameters for get_objects method.
          guids - list of guids
          post_processing - see PostProcessing (optional).
          match_filter - see MatchFilter (optional).
    */
    typedef structure {
        list<GUID> guids;
        PostProcessing post_processing;
        MatchFilter match_filter;
    } GetObjectsInput;

    /*
      Output results of get_objects method.
      
      mapping<access_group_id, narrative_info> access_group_narrative_info - information about
         the workspaces in which the objects in the results reside. This data only applies to
         workspace objects.
      mapping<access_group_id, Workspace.workspace_info> workspaces_info - information about
         the workspaces in which the objects in the results reside. This data only applies to
         workspace objects. The tuple9 value returned by get_workspace_info() for each workspace
         in the search results is saved in this mapping.
      mapping<obj_ref, Workspace.object_info> objects_info - information about each object in the
         search results. This data only applies to workspace objects. The tuple11 value returned by
         get_object_info3() for each object in the search results is saved in the mapping.
    */
    typedef structure {
        list<ObjectData> objects;
        int search_time;
        mapping<access_group_id, narrative_info> access_group_narrative_info;
        mapping<access_group_id, Workspace.workspace_info> workspaces_info;
        mapping<obj_ref, Workspace.object_info> objects_info;
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
