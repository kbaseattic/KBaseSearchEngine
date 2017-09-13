package kbasesearchengine.parse;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;

public interface SubObjectConsumer {
    public void nextObject(String path) throws IOException, ObjectParseException;
    public JsonGenerator getOutput() throws IOException, ObjectParseException;
    public void flush() throws IOException, ObjectParseException;
}
