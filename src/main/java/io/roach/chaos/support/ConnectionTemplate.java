package io.roach.chaos.support;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public abstract class ConnectionTemplate {
    private ConnectionTemplate() {
    }

    public static <T> T execute(DataSource ds, ConnectionCallback<T> action) {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(true);

            T result;
            try {
                result = action.doInConnection(conn);
            } catch (RuntimeException | Error ex) {
                throw ex;
            } catch (Throwable ex) {
                throw new UndeclaredThrowableException(ex,
                        "TransactionCallback threw undeclared checked exception");
            }
            return result;
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }
}
