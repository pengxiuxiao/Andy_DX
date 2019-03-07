package utils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author: pxx
 * @Date: 2019/3/6 21:22
 * @Version 1.0
 */
public class LURCache extends LinkedHashMap<String, Integer> {

    private static final long serialVersionUID = 6556832593014173722L;
    protected int maxElements;

    public LURCache(int maxSize) {
        super(maxSize,0.75F,true);
        this.maxElements = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
        return (size() > this.maxElements);
    }

}
