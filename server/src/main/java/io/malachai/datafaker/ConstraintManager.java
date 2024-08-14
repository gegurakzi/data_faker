package io.malachai.datafaker;

import io.malachai.datafaker.exception.TableNotInitializedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConstraintManager {

    private final EntityManager entityManager;
    private volatile Map<Long, Long> primaryKeys = new ConcurrentHashMap<>();

    public ConstraintManager(EntityManager entityManager) {
        this.entityManager = entityManager;
        initialize(new ArrayList<>(entityManager.getTables().keySet()));
    }

    public void initialize(List<Long> keys) {
        primaryKeys.clear();
        keys.forEach(key -> primaryKeys.put(key, -1L));
    }

    public void increasePrimaryKey(Long tableKey) {
        Long primaryKey = primaryKeys.get(tableKey);
        if (primaryKey < 0) {
            primaryKey = 0L;
        }
        primaryKey += 1;
        primaryKeys.put(tableKey, primaryKey);
    }

    public Long getPrimaryKey(Long tableKey) throws TableNotInitializedException {
        Long primaryKey = primaryKeys.get(tableKey);
        if (primaryKey < 0L) {
            throw new TableNotInitializedException();
        }
        return primaryKeys.get(tableKey);
    }

}
