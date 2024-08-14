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
        List<TableAppender> appenders = new ArrayList<>();
        try {
            for (Map.Entry<Long, Table> tableEntry : entityManager.getTables().entrySet()) {
                TableAppender appender = new TableAppender(tableEntry.getValue(), entityManager,
                    constraintManager,
                    dataSourceManager);
                appenders.add(appender);
            }
            // 적재 시작
            appenders.forEach(Thread::start);
        } catch (Exception e) {
            e.printStackTrace();
            appenders.forEach(Thread::interrupt);
            throw new RuntimeException(e);
        }

        // 종료

    }
}
