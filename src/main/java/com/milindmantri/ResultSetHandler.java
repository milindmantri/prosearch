package com.milindmantri;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface ResultSetHandler<T> {
  T accept(ResultSet rs) throws SQLException;
}
