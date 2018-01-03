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

    /** Uses the specified parsing rules to extract data from the given source data
     * object.
     *
     * @param obj source data from which to extract
     * @param guid GUID of the source given source data
     * @param parsingRules parsing rules
     * @return A map from GUID to extracted data.
     * @throws IOException
     * @throws ObjectParseException
     * @throws IndexingException
     * @throws InterruptedException
     */
    public static Map<GUID, String> parseSubObjects(
            final SourceData obj,
            final GUID guid, 
            final ObjectTypeParsingRules parsingRules)
            throws IOException,
                   ObjectParseException,
                   IndexingException,
                   InterruptedException {
        /* note that in opposition to the name, objects with no subobject specs get run through
         * this method.
         */
        Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        try (JsonParser jts = obj.getData().getPlacedStream()) {
            extractSubObjects(parsingRules, new SimpleSubObjectConsumer(pathToJson), jts);
        }
        Map<GUID, String> guidToJson = new LinkedHashMap<>();
        for (ObjectJsonPath path : pathToJson.keySet()) {
            String subJson = pathToJson.get(path);
            SimpleIdConsumer idConsumer = new SimpleIdConsumer();
            if (parsingRules.getSubObjectIDPath().isPresent()) {
                try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                    IdMapper.mapKeys(parsingRules.getSubObjectIDPath().get(), subJts, idConsumer);
                }
                /* if this if block is outside the parent if block, standard objects without
                 * subobjects fail to parse
                 */
                if (idConsumer.getPrimaryKey() == null) {
                    throw new ObjectParseException(String.format(
                            "Could not find the subobject id for one or more of the subobjects " +
                                    "for object %s when applying search specification %s_%s",
                                    guid, parsingRules.getGlobalObjectType().getType(),
                                    parsingRules.getGlobalObjectType().getVersion())); 
                }
            }
            GUID subid = prepareGUID(parsingRules, guid, path, idConsumer);
            guidToJson.put(subid, subJson);
        }
        return guidToJson;
    }

    /** Appends the sub-object path and id to the specified GUID if the parsing
     * rules define the extraction of data from a sub-object.
     *
     *
     * @param parsingRules parsing rules
     * @param guid GUID without the sub-object path and id
     * @param path
     * @param idConsumer
     * @return GUID with sub-object path and id if data needs to be extracted
     * from sub-object, lese GUID with just main object information.
     */
    public static GUID prepareGUID(
            ObjectTypeParsingRules parsingRules,
            GUID guid,
            ObjectJsonPath path,
            SimpleIdConsumer idConsumer) {
        String subObjectType = null;
        String subObjectID = null;
        if (parsingRules.getSubObjectPath().isPresent()) {
            subObjectID = idConsumer.getPrimaryKey() == null ? path.toString() :
                String.valueOf(idConsumer.getPrimaryKey());
            subObjectType = parsingRules.getSubObjectType().get();
        }
        return new GUID(guid, subObjectType, subObjectID);
    }

    /** Returns null if the specified parsing rules do not have rules for
     * extracting parent elements. Else returns a JSON string containing the
     * extracted portions of the parent object.
     *
     * @param parsingRules
     * @param jts
     * @return
     * @throws ObjectParseException
     * @throws IOException
     */
    public static String extractParentFragment(
            ObjectTypeParsingRules parsingRules,
            JsonParser jts)
            throws ObjectParseException,
                   IOException {
        if (!parsingRules.getSubObjectPath().isPresent()) {
            return null;
        }
        List<ObjectJsonPath> indexingPaths = new ArrayList<>();
        for (IndexingRules rules : parsingRules.getIndexingRules()) {
            if (!rules.isFromParent()) {
                continue;
            }
            //TODO CODE this seems wrong. Why adding null paths?
            indexingPaths.add(rules.getPath().orNull());
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

    /** Uses the given JsonParser to extract sub-object data based on the specified
     * parsing rules and populates the given SubObjectConsumer object with the
     * extracted data.
     *
     * @param parsingRules
     * @param subObjConsumer
     * @param jts
     * @throws ObjectParseException
     * @throws IOException
     */
    public static void extractSubObjects(
            ObjectTypeParsingRules parsingRules,
            SubObjectConsumer subObjConsumer,
            JsonParser jts)
            throws ObjectParseException,
                   IOException {
        List<ObjectJsonPath> indexingPaths = new ArrayList<>();
        for (IndexingRules rules : parsingRules.getIndexingRules()) {
            if (rules.isFromParent()) {
                continue;
            }
            if (rules.getPath().isPresent()) {
                indexingPaths.add(rules.getPath().get());
            }
        }
        ObjectJsonPath pathToSubObjects = parsingRules.getSubObjectPath()
                .or(new ObjectJsonPath("/"));
        SubObjectExtractor.extract(pathToSubObjects, indexingPaths, jts, subObjConsumer);
    }
}
