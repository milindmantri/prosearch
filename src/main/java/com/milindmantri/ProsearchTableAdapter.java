package com.milindmantri;

import com.norconex.collector.core.store.DataStoreException;
import com.norconex.commons.lang.map.MapUtil;
import com.norconex.commons.lang.text.StringUtil;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * Utility wrapper to help with minimal database data type compatibility without having to rely on
 * ORM software.
 * </p>
 *
 * Created because TableAdapter is a final class and package private. This was necessary as a part
 * of jdbc store and engine
 *
 */
final class ProsearchTableAdapter {

  private static final ProsearchTableAdapter DEFAULT =
    of("VARCHAR", "TIMESTAMP", "TEXT");

  // TODO: can get rid of stuff apart from postgres
  private static final Map<String, ProsearchTableAdapter> ADAPTERS = MapUtil.toMap(
    "DERBY", DEFAULT.withJsonType("CLOB"),
    "DB2", DEFAULT.withJsonType("CLOB"),
    "H2", DEFAULT.withJsonType("CLOB"),
    "MYSQL", DEFAULT.withJsonType("LONGTEXT"),
    "ORACLE", of("VARCHAR2", "TIMESTAMP", "CLOB"),
    "POSTGRESQL", DEFAULT,
    "SQLSERVER", of("VARCHAR", "DATETIME", "NTEXT"),
    "SYBASE", DEFAULT
  );

  private static final int ID_MAX_LENGTH = 2048;

  private final String idType;
  private final String modifiedType;
  private final String jsonType;

  private ProsearchTableAdapter(String idType, String modifiedType, String jsonType) {
    this.idType = idType;
    this.modifiedType = modifiedType;
    this.jsonType = jsonType;
  }

  String serializableId(String id) {
    try {
      return StringUtil.truncateBytesWithHash(
        id, StandardCharsets.UTF_8, ID_MAX_LENGTH);
    } catch (CharacterCodingException e) {
      throw new DataStoreException("Could not truncate ID: " + id, e);
    }
  }

  String idType() {
    return idType + "(" + ID_MAX_LENGTH + ")";
  }

  String modifiedType() {
    return modifiedType;
  }

  String jsonType() {
    return jsonType;
  }

  ProsearchTableAdapter withIdType(String idType) {
    if (StringUtils.isBlank(idType)) {
      return this;
    }
    return new ProsearchTableAdapter(idType, modifiedType,
      jsonType);
  }

  ProsearchTableAdapter withModifiedType(String modifiedType) {
    if (StringUtils.isBlank(modifiedType)) {
      return this;
    }
    return new ProsearchTableAdapter(idType, modifiedType,
      jsonType);
  }

  ProsearchTableAdapter withJsonType(String jsonType) {
    if (StringUtils.isBlank(jsonType)) {
      return this;
    }
    return new ProsearchTableAdapter(idType, modifiedType,
      jsonType);
  }

  static ProsearchTableAdapter of(
    String idType, String modifiedType, String jsonType) {
    return new ProsearchTableAdapter(idType, modifiedType,
      jsonType);
  }

  static ProsearchTableAdapter detect(
    String jdbcUrlOrDataSource) {
    if (jdbcUrlOrDataSource == null) {
      return DEFAULT;
    }
    String upper = jdbcUrlOrDataSource.toUpperCase();
    return ADAPTERS
      .entrySet()
      .stream()
      .filter(en -> upper.contains(en.getKey()))
      .findFirst()
      .map(Entry::getValue)
      .orElse(DEFAULT);
  }
}

