package io.malachai.datafaker.util;

public class ParamEntry {

    final String type;
    final Object obj;

    ParamEntry(final String type, final Object obj) {
        // check parameters
        this.type = type;
        this.obj = obj;
    }

    public String getType() {
        return type;
    }

    public Object getObj() {
        return obj;
    }
}
