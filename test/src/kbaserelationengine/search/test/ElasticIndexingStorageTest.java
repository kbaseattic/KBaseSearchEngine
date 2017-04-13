package kbaserelationengine.search.test;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;

import kbaserelationengine.common.GUID;
import kbaserelationengine.common.ObjectJsonPath;
import kbaserelationengine.parse.IdMapper;
import kbaserelationengine.parse.ObjectParser;
import kbaserelationengine.parse.SimpleIdConsumer;
import kbaserelationengine.parse.SimpleSubObjectConsumer;
import kbaserelationengine.parse.SubObjectConsumer;
import kbaserelationengine.parse.test.SubObjectExtractorTest;
import kbaserelationengine.search.ElasticIndexingStorage;
import kbaserelationengine.system.ObjectTypeParsingRules;
import us.kbase.common.service.UObject;

public class ElasticIndexingStorageTest {

    @Test
    public void testSave() throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> parsingRulesObj = UObject.getMapper().readValue(
                new File("resources/types/GenomeFeature.json"), Map.class);
        ObjectTypeParsingRules parsingRules = ObjectTypeParsingRules.fromObject(parsingRulesObj);
        Map<ObjectJsonPath, String> pathToJson = new LinkedHashMap<>();
        SubObjectConsumer subObjConsumer = new SimpleSubObjectConsumer(pathToJson);
        try (JsonParser jts = SubObjectExtractorTest.getParsedJsonResource("genome01")) {
            ObjectParser.extractSubObjects(parsingRules, subObjConsumer, jts);
        }
        for (ObjectJsonPath path : pathToJson.keySet()) {
            String subJson = pathToJson.get(path);
            SimpleIdConsumer idConsumer = new SimpleIdConsumer();
            try (JsonParser subJts = UObject.getMapper().getFactory().createParser(subJson)) {
                IdMapper.mapKeys(parsingRules.getPrimaryKeyPath(), 
                        parsingRules.getRelationPathToRules(), subJts, idConsumer);
            }
            GUID id = ObjectParser.prepareGUID(parsingRules, "1/1/1", path, idConsumer);
            String esIndexName = "test_" + System.currentTimeMillis();
            ElasticIndexingStorage indexStorage = new ElasticIndexingStorage("localhost:9300", 
                    esIndexName);
            indexStorage.indexObject(id, parsingRules.getGlobalObjectType(), subJson, 
                    parsingRules.getIndexingRules());
        }
    }
}
