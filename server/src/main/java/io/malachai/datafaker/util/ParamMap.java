package io.malachai.datafaker.util;

import java.util.HashMap;

public class ParamMap<K, T, V> extends HashMap<K, ParamMap.ParamEntry<T, V>> {

    public void put(K key, T type, V value) {
        ParamEntry<T, V> p = new ParamEntry<>();
        p.put(type, value);
        this.put(key, p);
    }

    public static class ParamEntry<T, V> {

        private T type;
        private V obj;

        public void put(T type, V obj) {
            this.type = type;
            this.obj = obj;
        }

        public T getType() {
            return type;
        }

        public V getObj() {
            return obj;
        }
    }

}
