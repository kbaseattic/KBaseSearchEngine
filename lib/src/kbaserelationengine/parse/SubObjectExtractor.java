package kbaserelationengine.parse;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Extraction of ws-searchable subset based on json token stream.
 * @author rsutormin
 */
public class SubObjectExtractor {

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
	public static void extract(ObjectJsonPath pathToSub, List<ObjectJsonPath> objpaths, 
	        JsonParser jts, SubObjectConsumer consumer) throws IOException, ObjectParseException {
		//if the selection is empty, we return without adding anything
		SubObjectExtractionNode root = new SubObjectExtractionNode();
		SubObjectExtractionNode sub = root.addPath(pathToSub, true, false);
		for (ObjectJsonPath path: objpaths) {
		    sub.addPath(trimPath(path), false, true);
		}
		extract(root, jts, consumer);
	}
	
	public static void extract(SubObjectExtractionNode tree, JsonParser jts, 
	        SubObjectConsumer consumer) throws IOException, ObjectParseException {
		JsonToken t = jts.nextToken();
		extractFieldsWithOpenToken(jts, t, tree, consumer, new ArrayList<String>(), 
		        false, false, true);
		consumer.flush();
	}
	
	// remove trailing '*' and '[*]', because these select everything
	private static ObjectJsonPath trimPath(ObjectJsonPath jsonPath) {
	    String[] pathToken = jsonPath.getPathItems();
		int end = pathToken.length;
		for(int k=pathToken.length-1; k>0; k--) {
			if(pathToken[k].equals("*") || pathToken[k].equals("[*]")) {
				// but we do not remove the first one, because this means we select everything...
				if(k==0) {break;} 
				end--;
			} else {break;}
		}
		String[] parsedPath = new String[end];
		for(int k=0; k<end; k++) {
			parsedPath[k] = pathToken[k];
		}
		return new ObjectJsonPath(parsedPath);
	}

	
	/*
	 * This method is recursively processing block of json data (map, array of scalar) when
	 * first token of this block was already taken and stored in current variable. This is
	 * typical for processing array elements because we need to read first token in order to
	 * know is it the end of array of not. For maps/objects there is such problem because
	 * we read field token before processing value block.
	 */
	private static void writeTokensFromCurrent(final JsonParser jts, final JsonToken current,
			final JsonGenerator jgen) throws IOException, ObjectParseException {
		JsonToken t = current;
		writeCurrentToken(jts, t, jgen);
		if (t == JsonToken.START_OBJECT) {
			while (true) {
				t = jts.nextToken();
				writeCurrentToken(jts, t, jgen);
				if (t == JsonToken.END_OBJECT) {
					break;
				}
				if (t != JsonToken.FIELD_NAME) {
					throw new IOException(
							"Error parsing json format: " + t.asString());
				}
				t = jts.nextToken();
				writeTokensFromCurrent(jts, t, jgen);
			}
		} else if (t == JsonToken.START_ARRAY) {
			while (true) {
				t = jts.nextToken();
				if (t == JsonToken.END_ARRAY) {
					writeCurrentToken(jts, t, jgen);
					break;
				}
				writeTokensFromCurrent(jts, t, jgen);
			}
		}
	}

	/*
	 * Method processes (writes into output token stream - jgen) only one token.
	 */
	private static JsonToken writeCurrentToken(JsonParser jts, JsonToken current, 
			JsonGenerator jgen) throws IOException, ObjectParseException {
		JsonToken t = current;
		if (t == JsonToken.START_ARRAY) {
			jgen.writeStartArray();
		} else if (t == JsonToken.START_OBJECT) {
			jgen.writeStartObject();
		} else if (t == JsonToken.END_ARRAY) {
			jgen.writeEndArray();
		} else if (t == JsonToken.END_OBJECT) {
			jgen.writeEndObject();
		} else if (t == JsonToken.FIELD_NAME) {
			jgen.writeFieldName(jts.getText());
		} else if (t == JsonToken.VALUE_NUMBER_INT) {
			Number value = jts.getNumberValue();
			if (value instanceof Short) {
				jgen.writeNumber((Short)value);
			} else if (value instanceof Integer) {
				jgen.writeNumber((Integer)value);
			} else if (value instanceof Long) {
				jgen.writeNumber((Long)value);
			} else if (value instanceof BigInteger) {
				jgen.writeNumber((BigInteger)value);
			} else {
				jgen.writeNumber(value.longValue());
			}
		} else if (t == JsonToken.VALUE_NUMBER_FLOAT) {
			Number value = jts.getNumberValue();
			if (value instanceof Float) {
				jgen.writeNumber((Float)value);
			} else if (value instanceof Double) {
				jgen.writeNumber((Double)value);
			} else if (value instanceof BigDecimal) {
				jgen.writeNumber((BigDecimal)value);
			} else {
				jgen.writeNumber(value.doubleValue());
			}
		} else if (t == JsonToken.VALUE_STRING) {
			jgen.writeString(jts.getText());
		} else if (t == JsonToken.VALUE_NULL) {
			jgen.writeNull();
		} else if (t == JsonToken.VALUE_FALSE) {
			jgen.writeBoolean(false);
		} else if (t == JsonToken.VALUE_TRUE) {
			jgen.writeBoolean(true);
		} else {
			throw new IOException("Unexpected token type: " + t);
		}
		return t;
	}

	/*
	 * If some part of real json data is not mention in subset schema tree we need to skip tokens of it
	 */
	private static void skipChildren(JsonParser jts, JsonToken current) throws IOException {
		JsonToken t = current;
		if (t == JsonToken.START_OBJECT) {
			while (true) {
				t = jts.nextToken();
				if (t == JsonToken.END_OBJECT)
					break;
				t = jts.nextToken();
				skipChildren(jts, t);
			}
		} else if (t == JsonToken.START_ARRAY) {
			while (true) {
				t = jts.nextToken();
				if (t == JsonToken.END_ARRAY)
					break;
				skipChildren(jts, t);
			}
		}
	}

	/*
	 * This is main recursive method for tracking current token place in subset schema tree
	 * and making decisions whether or not we need to process this token or block of tokens or
	 * just skip it.
	 */
	private static void extractFieldsWithOpenToken(JsonParser jts, JsonToken current, 
			SubObjectExtractionNode selection, SubObjectConsumer consumer, 
			List<String> path, boolean strictMaps, boolean strictArrays,
			boolean fromSkippedLevel) throws IOException, ObjectParseException {
	    if (fromSkippedLevel && !selection.isSkipLevel()) {
	        // It means we're starting sub-object (or whole object is needed)
	        consumer.nextObject(ObjectJsonPath.getPathText(path));
	    }
		JsonToken t = current;
		boolean skipLvl = selection.isSkipLevel();
		if (t == JsonToken.START_OBJECT) {	// we observe open of mapping/object in real json data
			if (selection.hasChildren()) {	// we have some restrictions for this object in selection
				// we will remove visited keys from selectedFields and check emptiness at object end
				Set<String> selectedFields = new LinkedHashSet<String>(selection.getChildren().keySet());
				boolean all = false;
				SubObjectExtractionNode allChild = null;
				if (selectedFields.contains("*")) {
					all = true;
					selectedFields.remove("*");
					allChild = selection.getChildren().get("*");
					if (selectedFields.size() > 0)
						throw new ObjectParseException("Invalid selection: the selection path " +
								"contains both '*' to select all fields and selection of " +
								"specific fields (" + selectedFields + "), at: " + 
								ObjectJsonPath.getPathText(path));
				}
				// process first token standing for start of object
				if (!skipLvl) {
				    writeCurrentToken(jts, t, consumer.getOutput());
				}
				while (true) {
					t = jts.nextToken();
					if (t == JsonToken.END_OBJECT) {
					    if (!skipLvl) {
		                    writeCurrentToken(jts, t, consumer.getOutput());
					    }
						break;
					}
					if (t != JsonToken.FIELD_NAME)
						throw new ObjectParseException("Error parsing json format " + 
								t.asString() + ", at: " + ObjectJsonPath.getPathText(path));
					String fieldName = jts.getText();
					if (all || selectedFields.contains(fieldName)) {
						// if we need all fields or the field is present in list of necessary fields 
						// we process it and value following after that
						if (!all)
							selectedFields.remove(fieldName);
						if (!skipLvl) {
		                    writeCurrentToken(jts, t, consumer.getOutput());
						}
						// read first token of value block in order to prepare state for recursive 
						// extractFieldsWithOpenToken call
						t = jts.nextToken();
						// add field to the tail of path branch
						path.add(fieldName);
						// process value corresponding to this field recursively
						extractFieldsWithOpenToken(jts, t, all ? allChild : 
							selection.getChildren().get(fieldName), consumer, path, 
							strictMaps, strictArrays, selection.isSkipLevel());
						// remove field from tail of path branch
						path.remove(path.size() - 1);
					} else {
						// otherwise we skip value following after field
						t = jts.nextToken();
						skipChildren(jts, t);
					}
				}
				// let's check have we visited all selected fields in this map
				// we will not visit them in real data and hence will not delete them from selection
				if (strictMaps && !selectedFields.isEmpty()) {
					String notFound = selectedFields.iterator().next();
					throw new ObjectParseException("Invalid selection: data does not contain " +
							"a field or key named '" + notFound + "', at: " + 
					        getPathText(path, notFound));
				}
			} else {  // need all fields and values
			    if (selection.isNeedAll()) {
			        writeTokensFromCurrent(jts, t, consumer.getOutput());
			    } else {
			        skipChildren(jts, t);
			    }
			}
		} else if (t == JsonToken.START_ARRAY) {	// we observe open of array/list in real json data
			if (selection.hasChildren()) {  // we have some restrictions for array item positions in selection
				Set<String> selectedFields = new LinkedHashSet<String>(
				        selection.getChildren().keySet());
				SubObjectExtractionNode allChild = null;
				// now we support only '[*]' which means all elements and set of numbers in case of 
				// certain item positions are selected in array
				if (!selectedFields.contains("[*]")) {
					for (String item : selectedFields) {
						try {
							Integer.parseInt(item);
						} catch (NumberFormatException ex) {
							throw new ObjectParseException("Invalid selection: data at '" + 
							        ObjectJsonPath.getPathText(path) + "' is an array, so " +
							        "element selection must be an integer. You requested element" +
							        " '" + item + "', at: " + ObjectJsonPath.getPathText(path));
						}
					}
				}
				if (selectedFields.contains("[*]")) {
					selectedFields.remove("[*]");
					allChild = selection.getChildren().get("[*]");
					// if there is [*] keyword selected there shouldn't be anything else in selection
					if (selectedFields.size() > 0)
						throw new ObjectParseException("Invalid selection: the selection path " +
								"contains both '[*]' to select all elements and selection of " +
								"specific elements (" + selectedFields + "), at: " + 
								ObjectJsonPath.getPathText(path));
				}
				if (!skipLvl) {
                    writeCurrentToken(jts, t, consumer.getOutput());  // write start of array into output
				}
				for (int pos = 0; ; pos++) {
					t = jts.nextToken();
					if (t == JsonToken.END_ARRAY) {
					    if (!skipLvl) {
		                    writeCurrentToken(jts, t, consumer.getOutput());
					    }
						break;
					}
					SubObjectExtractionNode child = null;
					if (allChild != null) {
						child = allChild; 
					} else {
						String key = "" + pos;
						if (selection.getChildren().containsKey(key)) {
							child = selection.getChildren().get(key);
							selectedFields.remove(key);
						}
					}
					if (child == null) {
						// this element of array is not selected, skip it
						skipChildren(jts, t);
					} else {
						// add element position to the tail of path branch
						path.add("" + pos);
						// process value of this element recursively
						extractFieldsWithOpenToken(jts, t, child, consumer, path, strictMaps, 
						        strictArrays, selection.isSkipLevel());
						// remove field from tail of path branch
						path.remove(path.size() - 1);
					}
				}
				// let's check have we visited all selected items in this array
				if (strictArrays && !selectedFields.isEmpty()) {
					String notFound = selectedFields.iterator().next();
					throw new ObjectParseException("Invalid selection: no array element exists " +
							"at position '" + notFound + "', at: " + getPathText(path, notFound));
				}
			} else {
			    if (selection.isNeedAll()) {
			        // need all elements
			        writeTokensFromCurrent(jts, t, consumer.getOutput());
			    } else {
			        skipChildren(jts, t);
			    }
			}
		} else {	// we observe scalar value (text, integer, double, boolean, null) in real json data
			if (selection.hasChildren())
				throw new ObjectParseException("Invalid selection: the path given specifies " +
						"fields or elements that do not exist because data at this location is " +
						"a scalar value (i.e. string, integer, float), at: " + 
						ObjectJsonPath.getPathText(path));
			writeCurrentToken(jts, t, consumer.getOutput());
		}
	}

	public static String getPathText(List<String> path, String add) {
		path.add(add);
		String ret = ObjectJsonPath.getPathText(path);
		path.remove(path.size() - 1);
		return ret;
	}
	
}
