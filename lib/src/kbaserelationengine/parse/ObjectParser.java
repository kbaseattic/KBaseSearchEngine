package kbaserelationengine.parse;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;

import kbaserelationengine.common.GUID;
import kbaserelationengine.common.ObjectJsonPath;
import kbaserelationengine.relations.Relation;
import kbaserelationengine.relations.RelationStorage;
import kbaserelationengine.search.IndexingStorage;
import kbaserelationengine.system.RelationRules;
import kbaserelationengine.system.IndexingRules;
import kbaserelationengine.system.ObjectTypeParsingRules;
import kbaserelationengine.system.SystemStorage;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.UObject;
import workspace.GetObjects2Params;
import workspace.ObjectData;
import workspace.ObjectSpecification;
import workspace.WorkspaceClient;

public class ObjectParser {
    public void filterSubObjects(URL wsUrl, File tempDir, AuthToken token, 
            String objRef, ObjectTypeParsingRules parsingRules, SystemStorage system,
            IndexingStorage indexStorage, RelationStorage relationStorage) throws Exception {
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        File tempFile = File.createTempFile("ws_srv_response_", ".json", tempDir);
        WorkspaceClient wc = new WorkspaceClient(wsUrl, token);
        wc.setIsInsecureHttpConnectionAllowed(true);
        wc.setStreamingModeOn(true);
        wc._setFileForNextRpcResponse(tempFile);
        ObjectData obj = wc.getObjects2(new GetObjects2Params().withObjects(
                Arrays.asList(new ObjectSpecification().withRef(objRef)))).getData().get(0);
        String resolvedRef = getRefFromObjectInfo(obj.getInfo());
        List<ObjectJsonPath> indexingPaths = new ArrayList<>();
        for (IndexingRules rules : parsingRules.getIndexingRules()) {
            indexingPaths.add(rules.getPath());
        }
        Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        SubObjectConsumer subObjConsumer = new SimpleSubObjectConsumer(pathToJson);
        try (JsonParser jts = obj.getData().getPlacedStream()) {
            ObjectJsonPath pathToSubObjects = parsingRules.getPathToSubObjects() == null ?
                    new ObjectJsonPath("/") : parsingRules.getPathToSubObjects();
            SubObjectExtractor.extract(pathToSubObjects, indexingPaths, jts, subObjConsumer);
        }
        for (ObjectJsonPath path : pathToJson.keySet()) {
            String subJson = pathToJson.get(path);
            SimpleIdConsumer idConsumer = new SimpleIdConsumer();
            try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                IdMapper.mapKeys(parsingRules.getPrimaryKeyPath(), 
                        parsingRules.getRelationPathToRules(), subJts, idConsumer);
            }
            String storageType = parsingRules.getStorageType();
            if (storageType == null) {
                storageType = "WS";
            }
            String textId = storageType + ":" + resolvedRef;
            if (parsingRules.getPathToSubObjects() != null) {
                String innerTextId = idConsumer.getPrimaryKey() == null ? path.toString() : 
                    ("/" + String.valueOf(idConsumer.getPrimaryKey()));
                String innerSubType = parsingRules.getInnerSubType() == null ? "_" : 
                    parsingRules.getInnerSubType();
                textId += ":" + innerSubType + innerTextId;
            }
            GUID id = new GUID(textId);
            String objectType = parsingRules.getGlobalObjectType();
            indexStorage.indexObject(id, objectType, subJson, parsingRules.getIndexingRules());
            for (RelationRules lookupRules : idConsumer.getRulesToForeignKeys().keySet()) {
                Set<Object> foreignIds = idConsumer.getRulesToForeignKeys().get(lookupRules);
                Set<GUID> normedIds = system.normalizeObjectIds(foreignIds, 
                        lookupRules.getTargetObjectType());
                List<Relation> links = new ArrayList<>();
                for (GUID id2 : normedIds) {
                    Relation link = new Relation();
                    link.setId1(id);
                    link.setType1(objectType);
                    link.setId2(id2);
                    link.setType2(lookupRules.getTargetObjectType());
                    link.setLinkType(lookupRules.getRelationType());
                }
                relationStorage.addRelations(links);
            }
        }
    }
    
    public static String getRefFromObjectInfo(Tuple11<Long, String, String, String, 
            Long, String, Long, String, String, Long, Map<String,String>> info) {
        return info.getE7() + "/" + info.getE1() + "/" + info.getE5();
    }
}
