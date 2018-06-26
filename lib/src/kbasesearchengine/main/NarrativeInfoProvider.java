package kbasesearchengine.main;

import kbasesearchengine.main.NarrativeInfo;

public interface NarrativeInfoProvider {
    public NarrativeInfo findNarrativeInfo(Long wsId);
}
