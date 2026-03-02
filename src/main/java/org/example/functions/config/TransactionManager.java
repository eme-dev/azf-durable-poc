package org.example.functions.config;

import java.sql.Connection;
import java.sql.SQLException;


public final class TransactionManager implements AutoCloseable {

    private static final ThreadLocal<Connection> CONNECTION_HOLDER = new ThreadLocal<>();
    private boolean committed = false;

    public TransactionManager begin() throws SQLException {
        if (CONNECTION_HOLDER.get() != null) {
            throw new IllegalStateException("Transaccion ya activa en este hilo.");
        }
        Connection conn = DbConnectionProvider.open();
        conn.setAutoCommit(false);
        CONNECTION_HOLDER.set(conn);
        return this; // Permite encadenar en el try-with-resources
    }

    public Connection getConnection() {
        Connection conn = CONNECTION_HOLDER.get();
        if (conn == null) throw new IllegalStateException("No hay conexion activa.");
        return conn;
    }

    public void commit() throws SQLException {
        Connection conn = getConnection();
        conn.commit();
        this.committed = true; // Marcamos que el negocio terminó bien
    }

    @Override
    public void close() throws SQLException {
        Connection conn = CONNECTION_HOLDER.get();
        if (conn == null) return;

        try {
            if (!committed) {
                // Si llegamos aquí y no hubo commit, algo falló: ROLLBACK
                conn.rollback();
            }
        } finally {
            try {
                conn.setAutoCommit(true);
                conn.close();
            } finally {
                CONNECTION_HOLDER.remove();
            }
        }
    }
}