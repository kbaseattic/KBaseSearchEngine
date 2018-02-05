package kbasesearchengine.main;

import kbasesearchengine.*;
import kbasesearchengine.search.IndexingStorage;
import workspace.WorkspaceClient;

import java.util.Map;

/**
 * Created by umaganapathy on 1/26/18.
 */
public interface SearchInterface {

    SearchTypesOutput searchTypes(SearchTypesInput params, String user) throws Exception;

    SearchObjectsOutput searchObjects(SearchObjectsInput params, String user)
            throws Exception;

    GetObjectsOutput getObjects(GetObjectsInput params, String user)
                    throws Exception;

    Map<String, TypeDescriptor> listTypes(String uniqueType) throws Exception;
}
