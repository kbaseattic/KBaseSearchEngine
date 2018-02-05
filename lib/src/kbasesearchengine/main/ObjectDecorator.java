package kbasesearchengine.main;

import kbasesearchengine.*;
import kbasesearchengine.search.IndexingStorage;

import java.util.Map;

/**
 * Created by umaganapathy on 1/26/18.
 */

abstract class ObjectDecorator implements SearchInterface {

    protected SearchInterface searchInterface;

    public ObjectDecorator(SearchInterface si) {
        searchInterface = si;
    }

    public SearchObjectsOutput searchObjects(SearchObjectsInput params, String user)
            throws Exception {
        return searchInterface.searchObjects(params, user);
    }

    public GetObjectsOutput getObjects(GetObjectsInput params, String user)
            throws Exception {
        return searchInterface.getObjects(params, user);
    }

    public SearchTypesOutput searchTypes(SearchTypesInput params, String user)
            throws Exception {
        return searchInterface.searchTypes(params, user);
    }

    public IndexingStorage getIndexingStorage(String objectType) {
        return searchInterface.getIndexingStorage(objectType);
    }

    public Map<String, TypeDescriptor> listTypes(String uniqueType)
            throws Exception {
        return searchInterface.listTypes(uniqueType);
    }
}