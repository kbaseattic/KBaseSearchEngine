package kbaserelationengine.main;

import kbaserelationengine.common.GUID;

public interface LineLogger {
    public void logInfo(String line);
    public void logError(Throwable error);
    public void logError(String line);
    public void timeStat(GUID guid, long loadMs, long parseMs, long indexMs);
}
