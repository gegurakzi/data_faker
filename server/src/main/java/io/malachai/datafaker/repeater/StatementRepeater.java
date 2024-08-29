package io.malachai.datafaker.repeater;

import io.malachai.datafaker.DataSourceManager;
import io.malachai.datafaker.EntityManager;
import io.malachai.datafaker.PrimaryKeyManager;
import io.malachai.datafaker.TableReserve;
import io.malachai.datafaker.util.ParamMap;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public abstract class StatementRepeater extends Thread {

    private static Logger LOG = Logger.getLogger(StatementRepeater.class.getName());

    protected final TableReserve tableReserve;
    protected final EntityManager entityManager;
    protected final PrimaryKeyManager primaryKeyManager;
    protected final DataSourceManager dataSourceManager;

    public StatementRepeater(TableReserve tableReserve, EntityManager entityManager,
        PrimaryKeyManager primaryKeyManager,
        DataSourceManager dataSourceManager) {
        this.tableReserve = tableReserve;
        this.entityManager = entityManager;
        this.primaryKeyManager = primaryKeyManager;
        this.dataSourceManager = dataSourceManager;
    }

    protected abstract ParamMap<String, String, Object> prepareParameters();

    protected abstract String mapStatement(ParamMap<String, String, Object> params);

    protected abstract void postExecution();

    @Override
    public void run() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (true) {
            ParamMap<String, String, Object> params = prepareParameters();
            String query = mapStatement(params);
            try {
                LOG.info("Executing: " + query);
                dataSourceManager.execute(
                    entityManager.getSourceKey(tableReserve.getTable().getSourceName()), query);
            } catch (SQLException e) {
                LOG.warning("SQLException:" + e.getMessage());
            }
            postExecution();
        }
    }

}
