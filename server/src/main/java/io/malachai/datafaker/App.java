package io.malachai.datafaker;

import io.malachai.datafaker.repeater.InsertStatementRepeater;
import io.malachai.datafaker.repeater.StatementRepeater;
import io.malachai.datafaker.repeater.UpdateStatementRepeater;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class App {

    public static void main(String[] args) {
        EntityManagerLoader loader = new EntityManagerLoader();
        EntityManager entityManager;
        try {
            entityManager = loader.load(new InputStreamReader(
                Objects.requireNonNull(App.class.getResourceAsStream("/config.json"))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        PrimaryKeyManager primaryKeyManager = new PrimaryKeyManager(entityManager);
        DataSourceManager dataSourceManager = new DataSourceManager(entityManager);

        // 테이블 별 스레드 생성
        List<StatementRepeater> repeaters = new ArrayList<>();
        try {
            for (Map.Entry<Long, Table> tableEntry : entityManager.getTables().entrySet()) {
                // INSERT
                StatementRepeater appender = new InsertStatementRepeater(
                    new TableReserve(tableEntry.getValue()),
                    entityManager,
                    primaryKeyManager,
                    dataSourceManager);
                repeaters.add(appender);

                // UPDATE
                String updateMode = tableEntry.getValue().getUpdateMode();
                switch (updateMode) {
                    case "random":
                        StatementRepeater updater = new UpdateStatementRepeater(
                            new TableReserve(tableEntry.getValue()),
                            entityManager,
                            primaryKeyManager,
                            dataSourceManager);
                        repeaters.add(updater);
                        break;
                }
            }

            // 적재 시작
            repeaters.forEach(Thread::start);
        } catch (Exception e) {
            repeaters.forEach(Thread::interrupt);
            throw new RuntimeException(e);
        }

        // 종료

    }
}
