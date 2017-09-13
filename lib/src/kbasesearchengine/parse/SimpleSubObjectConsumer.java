package kbasesearchengine.parse;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;

import kbasesearchengine.common.ObjectJsonPath;
import us.kbase.common.service.UObject;

public class SimpleSubObjectConsumer implements SubObjectConsumer {
    private Map<ObjectJsonPath, String> data;
    private String nextPath = null;
    private StringWriter outChars = null;
    private JsonGenerator nextGen = null;
    
    public SimpleSubObjectConsumer(Map<ObjectJsonPath, String> data) {
        this.data = data;
    }
    
    @Override
    public void nextObject(String path) throws IOException,
            ObjectParseException {
        flush();
        nextPath = path;
        outChars = new StringWriter();
        nextGen = UObject.getMapper().getFactory().createGenerator(outChars);
    }
    
    @Override
    public JsonGenerator getOutput() throws IOException, ObjectParseException {
        if (nextGen == null) {
            throw new ObjectParseException("JsonGenerator wasn't initialized");
        }
        return nextGen;
    }
    
    @Override
    public void flush() throws IOException, ObjectParseException {
        if (nextPath != null && nextGen != null && outChars != null) {
            nextGen.close();
            data.put(new ObjectJsonPath(nextPath), outChars.toString());
            nextPath = null;
            outChars = null;
            nextGen = null;
        }
    }
}
