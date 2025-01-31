package com.milindmantri;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ZonedDateTimeTypeAdapter
    implements JsonSerializer<ZonedDateTime>, JsonDeserializer<ZonedDateTime> {

  @Override
  public JsonElement serialize(
      ZonedDateTime zonedDateTime, Type srcType, JsonSerializationContext context) {

    return new JsonPrimitive(DateTimeFormatter.ISO_DATE_TIME.format(zonedDateTime));
  }

  @Override
  public ZonedDateTime deserialize(
      JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {

    return ZonedDateTime.parse(json.getAsString(), DateTimeFormatter.ISO_DATE_TIME);
  }
}
