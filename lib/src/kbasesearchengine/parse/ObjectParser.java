package kbasesearchengine.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;

import kbasesearchengine.common.GUID;
import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.ObjectTypeParsingRules;
import us.kbase.common.service.UObject;

public class ObjectParser {

    /**
     * Extracts fragments from object JSON file where the corresponding fragment
     * defined in 'indexing-rules' in resources/types does not have
     * 'from-parent' field or 'from-parent' is false
     * 
     * e.g. { "path": "id", "keyword-type": "string" }
     * (resources/types/GenomeFeature.json)
     * 
     * @param obj
     *            instance of SourceData used to store original object JSON file
     * @param guid
     *            parent GUID object used to form GUID for each fragment
     * @param parsingRules
     *            instance of ObjectTypeParsingRules used to form subObjectType
     *            instance variable of GUID object for each fragment
     * @return
     * @throws IOException
     * @throws ObjectParseException
     * @throws IndexingException
     * @throws InterruptedException
     */
    public static Map<GUID, String> parseSubObjects(
            final SourceData obj,
            final GUID guid, 
            final ObjectTypeParsingRules parsingRules)
            throws IOException, ObjectParseException, IndexingException, InterruptedException {
        Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        SubObjectConsumer subObjConsumer = new SimpleSubObjectConsumer(pathToJson);
        try (JsonParser jts = obj.getData().getPlacedStream()) {
            extractSubObjects(parsingRules, subObjConsumer, jts);
        }
        Map<GUID, String> guidToJson = new LinkedHashMap<>();
        for (ObjectJsonPath path : pathToJson.keySet()) {
            String subJson = pathToJson.get(path);
            SimpleIdConsumer idConsumer = new SimpleIdConsumer();
            if (parsingRules.getPrimaryKeyPath() != null) {
                try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                    IdMapper.mapKeys(parsingRules.getPrimaryKeyPath(), subJts, idConsumer);
                }
            }
            GUID subid = prepareGUID(parsingRules, guid, path, idConsumer);
            guidToJson.put(subid, subJson);
        }
        return guidToJson;
    }

    /**
     * Returns a new GUID object keeping parent_guid's storageCode,
     * accessGroupId, accessGroupObjectId and version instance variables.
     * Assigns instance variable subObjectType and subObjectId of the new GUID
     * from @param parsingRules, path and idConsumer
     * 
     * @param parsingRules
     *            instance of ObjectTypeParsingRules used to form subObjectType
     *            instance variable of returned GUID object
     * @param parent_guid
     *            parent GUID object
     * @param path
     *            instance of ObjectJsonPath used to form subObjectId instance
     *            variable of returned GUID object
     * @param idConsumer
     *            instance of SimpleIdConsumer used to form subObjectId instance
     *            variable of returned GUID object
     * @return GUID object
     */
    public static GUID prepareGUID(ObjectTypeParsingRules parsingRules,
            GUID parent_guid, ObjectJsonPath path,
            SimpleIdConsumer idConsumer) {
        String subObjectType = null;
        String subObjectId = null;
        if (parsingRules.getPathToSubObjects() != null) {
            subObjectId = idConsumer.getPrimaryKey() == null ? path.toString() :
                String.valueOf(idConsumer.getPrimaryKey());
            subObjectType = parsingRules.getInnerSubType() == null ? "_" :
                parsingRules.getInnerSubType();
        }
        
        return new GUID(parent_guid, subObjectType, subObjectId);
    }
    
    /**
     * Extracts fragments from object JSON file where the corresponding fragment
     * defined in 'indexing-rules' in resources/types has 'from-parent' is true
     * 
     * e.g. { "from-parent": true, "path": "scientific_name", "full-text": true,
     * "key-name": "genome_scientific_name" }
     * (resources/types/GenomeFeature.json)
     * 
     * 
     * @param parsingRules
     *            ObjectTypeParsingRules object used to form all rules from
     *            resources/types files
     * @param jts
     *            JsonParser object used to pass JSON info to helper functions
     * @return JSON string contains fragment information from object JSON
     * @throws ObjectParseException
     *             if SubObjectExtractor instance throws an exception
     * @throws IOException
     *             if stream to a File cannot be written to or closed
     */
    public static String extractParentFragment(ObjectTypeParsingRules parsingRules,
            JsonParser jts) throws ObjectParseException, IOException {
        if (parsingRules.getPathToSubObjects() == null) {
            return null;
        }
        List<ObjectJsonPath> indexingPaths = new ArrayList<>();
        for (IndexingRules rules : parsingRules.getIndexingRules()) {
            if (!rules.isFromParent()) {
                continue;
            }
            indexingPaths.add(rules.getPath());
        }
        if (indexingPaths.size() == 0) {
            return null;
        }
        Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        SubObjectConsumer parentConsumer = new SimpleSubObjectConsumer(pathToJson);
        ObjectJsonPath pathToSubObjects = new ObjectJsonPath("/");
        SubObjectExtractor.extract(pathToSubObjects, indexingPaths, jts, parentConsumer);
        return pathToJson.get(pathToJson.keySet().iterator().next());
    }

    public static void extractSubObjects(ObjectTypeParsingRules parsingRules,
            SubObjectConsumer subObjConsumer, JsonParser jts)
            throws ObjectParseException, IOException {
        List<ObjectJsonPath> indexingPaths = new ArrayList<>();
        for (IndexingRules rules : parsingRules.getIndexingRules()) {
            if (rules.isFromParent()) {
                continue;
            }
            if (rules.getPath() != null) {
                indexingPaths.add(rules.getPath());
            }
        }
        ObjectJsonPath pathToSubObjects = parsingRules.getPathToSubObjects() == null ?
                new ObjectJsonPath("/") : parsingRules.getPathToSubObjects();
        SubObjectExtractor.extract(pathToSubObjects, indexingPaths, jts, subObjConsumer);
    }
}
