package io.malachai.datafaker;

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
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        ConstraintManager constraintManager = new ConstraintManager(entityManager);
        DataSourceManager dataSourceManager = new DataSourceManager(entityManager);

        // 테이블 별 스레드 생성
        List<TableAppendThread> appenders = new ArrayList<>();
        List<TableUpdateThread> updaters = new ArrayList<>();
        try {
            // INSERT 커맨드
            for (Map.Entry<Long, Table> tableEntry : entityManager.getTables().entrySet()) {
                TableAppendThread appender = new TableAppendThread(tableEntry.getValue(),
                    entityManager,
                    constraintManager,
                    dataSourceManager);
                appenders.add(appender);
            }
            // UPDATE 커맨드
            for (Map.Entry<Long, Table> tableEntry : entityManager.getTables().entrySet()) {
                String updateMode = tableEntry.getValue().getUpdateMode();
                switch (updateMode) {
                    case "random":
                        TableUpdateThread updater = new TableUpdateThread(tableEntry.getValue(),
                            entityManager,
                            constraintManager,
                            dataSourceManager);
                        updaters.add(updater);
                        break;
                }
            }

            // 적재 시작
            appenders.forEach(Thread::start);
            updaters.forEach(Thread::start);
        } catch (Exception e) {
            e.printStackTrace();
            appenders.forEach(Thread::interrupt);
            throw new RuntimeException(e);
        }

        // 종료

    }
}
