package kbaserelationengine.parse;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;

import kbaserelationengine.common.ObjectJsonPath;
import kbaserelationengine.system.RelationRules;

/**
 * Extraction of primary/foreign key values based on JSON token stream.
 * @author rsutormin
 */
public class IdMapper {

	/**
	 * Extract the fields listed in selection from the element and add them to the subset.
	 * 
	 * selection must either be an object containing structure field names to extract, '*' in the case of
	 * extracting a mapping, or '[*]' for extracting a list.  if the selection is empty, nothing is added.
	 * If extractKeysOf is set, and the element is an Object (ie a kidl mapping), then an array of the keys
	 * is added instead of the entire mapping.
	 * 
	 * we assume here that selection has already been validated against the structure of the document, so that
	 * if we get true on extractKeysOf, it really is a mapping, and if we get a '*' or '[*]', it really is
	 * a mapping or array.
	 * @throws ObjectParseException 
	 */
	public static void mapKeys(ObjectJsonPath pathToPrimary, 
	        Map<ObjectJsonPath, RelationRules> foreignKeyRules, 
	        JsonParser jts, IdConsumer consumer) throws IOException, ObjectParseException {
		//if the selection is empty, we return without adding anything
		ValueCollectingNode<IdMappingRules> root = new ValueCollectingNode<>();
		root.addPath(pathToPrimary, new IdMappingRules(true));
		for (ObjectJsonPath path: foreignKeyRules.keySet()) {
		    root.addPath(path, new IdMappingRules(foreignKeyRules.get(path)));
		}
		new ValueCollector<IdMappingRules>().mapKeys(root, jts, consumer);
	}
	
}
