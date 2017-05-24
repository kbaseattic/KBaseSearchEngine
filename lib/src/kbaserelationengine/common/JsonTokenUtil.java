package kbaserelationengine.common;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import kbaserelationengine.parse.ObjectParseException;

public class JsonTokenUtil {

    /**
     * If some part of real json data is not involved in the process we need to skip tokens of it
     */
    public static void skipChildren(JsonParser jts, JsonToken current) throws IOException {
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

    /**
     * Remove trailing '*' and '[*]', because these select everything
     */
    public static ObjectJsonPath trimPath(ObjectJsonPath jsonPath) {
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

    /**
     * This method is recursively processing block of json data (map, array of scalar) when
     * first token of this block was already taken and stored in current variable. This is
     * typical for processing array elements because we need to read first token in order to
     * know is it the end of array of not. For maps/objects there is such problem because
     * we read field token before processing value block.
     */
    public static void writeTokensFromCurrent(final JsonParser jts, final JsonToken current,
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

    /**
     * Method processes (writes into output token stream - jts) only one token.
     */
    public static JsonToken writeCurrentToken(JsonParser jts, JsonToken current, 
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

    public static Object getCurrentTokenPrimitive(JsonParser jts, 
            JsonToken current) throws IOException {
        JsonToken t = current;
        if (t == JsonToken.VALUE_NUMBER_INT || t == JsonToken.VALUE_NUMBER_FLOAT) {
            return jts.getNumberValue();
        } else if (t == JsonToken.VALUE_STRING) {
            return jts.getText();
        } else if (t == JsonToken.VALUE_NULL) {
            return null;
        } else if (t == JsonToken.VALUE_FALSE) {
            return false;
        } else if (t == JsonToken.VALUE_TRUE) {
            return true;
        } else {
            throw new IOException("Unexpected token type: " + t);
        }
    }

    public static String prettyPrint(Object obj) {
        try {
            StringWriter ret = new StringWriter();
            ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
            mapper.writeValue(ret, obj);
            ret.close();
            return ret.toString();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
