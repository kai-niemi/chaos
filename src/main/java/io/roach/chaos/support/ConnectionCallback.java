package io.roach.chaos.support;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface ConnectionCallback<T> {
    T doInConnection(Connection conn) throws SQLException;
}
