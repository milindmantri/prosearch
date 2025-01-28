package com.milindmantri;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.ZoneId;

// Required for ZoneId which was failing when as it didn't allow empty constructor for init
public class ZoneIdAdapter implements JsonSerializer<ZoneId>, JsonDeserializer<ZoneId> {

  @Override
  public ZoneId deserialize(
      final JsonElement json, final Type typeOfT, final JsonDeserializationContext context)
      throws JsonParseException {

    var val = json.getAsJsonObject().get("id");

    if (val.isJsonNull()) {
      return null;
    } else {
      return ZoneId.of(val.getAsString());
    }
  }

  @Override
  public JsonElement serialize(
      final ZoneId src, final Type typeOfSrc, final JsonSerializationContext context) {

    var obj = new JsonObject();
    obj.addProperty("id", src.getId());
    return obj;
  }
}
