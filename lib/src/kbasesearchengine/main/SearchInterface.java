package kbasesearchengine.main;

import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.TypeDescriptor;

import java.util.Map;

public interface SearchInterface {
    
    // TODO JAVADOC

    SearchTypesOutput searchTypes(SearchTypesInput params, String user) throws Exception;

    SearchObjectsOutput searchObjects(SearchObjectsInput params, String user)
            throws Exception;

    GetObjectsOutput getObjects(GetObjectsInput params, String user)
                    throws Exception;

    Map<String, TypeDescriptor> listTypes(String uniqueType) throws Exception;
}
