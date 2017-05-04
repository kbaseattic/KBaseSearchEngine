package kbaserelationengine.main;

public interface LineLogger {
    public void logInfo(String line);
    public void logError(Throwable error);
    public void logError(String line);
}
