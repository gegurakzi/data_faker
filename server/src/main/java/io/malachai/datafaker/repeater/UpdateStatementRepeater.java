package io.malachai.datafaker.repeater;

import io.malachai.datafaker.Column;
import io.malachai.datafaker.DataKindFaker;
import io.malachai.datafaker.DataSourceManager;
import io.malachai.datafaker.EntityManager;
import io.malachai.datafaker.PrimaryKeyManager;
import io.malachai.datafaker.TableReserve;
import io.malachai.datafaker.exception.TableNotInitializedException;
import io.malachai.datafaker.util.CastUtil;
import io.malachai.datafaker.util.ParamMap;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class UpdateStatementRepeater extends StatementRepeater {

    private final Long tableKey;
    private final DataKindFaker faker = new DataKindFaker();

    public UpdateStatementRepeater(TableReserve tableReserve, EntityManager entityManager,
        PrimaryKeyManager primaryKeyManager,
        DataSourceManager dataSourceManager) {
        super(tableReserve, entityManager, primaryKeyManager, dataSourceManager);
        this.tableKey = this.entityManager.getTableKey(tableReserve.getTable().getFullName());
    }

    @Override
    protected ParamMap<String, String, Object> prepareParameters() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        ParamMap<String, String, Object> params = new ParamMap<>();
        if (tableReserve.getPrimaryKey() != null) {
            Long key;
            try {
                key = primaryKeyManager.getPrimaryKey(tableKey);
            } catch (TableNotInitializedException e) {
                key = 0L;
            }
            params.put(
                tableReserve.getPrimaryKey().getName(),
                tableReserve.getPrimaryKey().getType(),
                key != 0L ? random.nextLong(key) : 0L);
        }
        for (Column updated : tableReserve.getUpdatedAt()) {
            params.put(
                updated.getName(),
                updated.getType(),
                CastUtil.retrieve(LocalDateTime.now(), updated.getType())
            );
        }
        // field columns randomizer
        for (Column nonFinal : tableReserve.getNonFinalColumns()) {
            params.put(nonFinal.getName(), nonFinal.getType(), faker.get(nonFinal.getKind()));
        }
        return params;
    }

    @Override
    protected String mapStatement(ParamMap<String, String, Object> params) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(tableReserve.getTable().getName());
        sb.append(" SET ");
        for (Map.Entry<String, ParamMap.ParamEntry<String, Object>> param : params.entrySet()) {
            if (param.getKey().equals(tableReserve.getPrimaryKey().getName())) {
                continue;
            }
            sb.append(param.getKey());
            sb.append("=");
            sb.append(CastUtil.toToken(param.getValue().getObj(), param.getValue().getType()));
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(" WHERE ");
        sb.append(tableReserve.getPrimaryKey().getName());
        sb.append(" = ");
        sb.append(params.get(tableReserve.getPrimaryKey().getName()).getObj().toString());
        return sb.toString();
    }

    @Override
    protected void postExecution() {
        sleepWithoutException(tableReserve.getTable().getUpdateInterval());
    }

    private void sleepWithoutException(Long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            //connection.close();
            throw new RuntimeException(e);
        }
    }
}
