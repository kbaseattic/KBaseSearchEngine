package kbasesearchengine.parse;

import java.io.File;
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
    
    public static File prepareTempFile(File tempDir) throws IOException {
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        File tempFile = File.createTempFile("ws_srv_response_", ".json", tempDir);
        return tempFile;
    }

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
            if (parsingRules.getPrimaryKeyPath().isPresent()) {
                try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                    IdMapper.mapKeys(parsingRules.getPrimaryKeyPath().get(), subJts, idConsumer);
                }
            }
            GUID subid = prepareGUID(parsingRules, guid, path, idConsumer);
            guidToJson.put(subid, subJson);
        }
        return guidToJson;
    }

    public static GUID prepareGUID(ObjectTypeParsingRules parsingRules,
            GUID guid, ObjectJsonPath path,
            SimpleIdConsumer idConsumer) {
        String innerSubType = null;
        String innerID = null;
        if (parsingRules.getPathToSubObjects().isPresent()) {
            innerID = idConsumer.getPrimaryKey() == null ? path.toString() : 
                String.valueOf(idConsumer.getPrimaryKey());
            innerSubType = parsingRules.getInnerSubType().get();
        }
        return new GUID(guid, innerSubType, innerID);
    }
    
    public static String extractParentFragment(ObjectTypeParsingRules parsingRules,
            JsonParser jts) throws ObjectParseException, IOException {
        if (!parsingRules.getPathToSubObjects().isPresent()) {
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

    public static void extractSubObjects(ObjectTypeParsingRules parsingRules,
            SubObjectConsumer subObjConsumer, JsonParser jts)
            throws ObjectParseException, IOException {
        List<ObjectJsonPath> indexingPaths = new ArrayList<>();
        for (IndexingRules rules : parsingRules.getIndexingRules()) {
            if (rules.isFromParent()) {
                continue;
            }
            if (rules.getPath().isPresent()) {
                indexingPaths.add(rules.getPath().get());
            }
        }
        ObjectJsonPath pathToSubObjects = parsingRules.getPathToSubObjects()
                .or(new ObjectJsonPath("/"));
        SubObjectExtractor.extract(pathToSubObjects, indexingPaths, jts, subObjConsumer);
    }
}
