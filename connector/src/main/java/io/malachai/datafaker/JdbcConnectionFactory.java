package io.malachai.datafaker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcConnectionFactory implements ConnectionFactory {

    private final SourceType type = SourceType.JDBC;
    private final String jdbcUrl;
    private final String user;
    private final String password;

    public JdbcConnectionFactory(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }
}
