package org.example.functions.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class DbConnectionProvider {

    private static final String ENV_SQL_JDBC_URL = "SQL_JDBC_URL";

    private DbConnectionProvider() {}

    public static Connection open() throws SQLException {
        String jdbcUrl = System.getenv(ENV_SQL_JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            throw new IllegalStateException(ENV_SQL_JDBC_URL + " no configurado");
        }
        return DriverManager.getConnection(jdbcUrl);
    }
}
