package com.milindmantri;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.time.ZoneId;

// Required for ZoneId which was failing when as it didn't allow empty constructor for init
public class ZoneIdAdapter extends TypeAdapter<ZoneId> {

  @Override
  public void write(final JsonWriter out, final ZoneId value) throws IOException {
    out.beginObject();
    out.name("id");

    if (value == null) {
      out.nullValue();
    } else {
      out.value(value.getId());
    }

    out.endObject();
  }

  @Override
  public ZoneId read(final JsonReader in) throws IOException {

    ZoneId ret = null;

    in.beginObject();
    if (in.hasNext()) {
      var _ = in.nextName(); // id
      if (in.peek() == JsonToken.STRING) {
        var str = in.nextString();
        ret =  ZoneId.of(str);
      } else {
        in.nextNull();
        ret = null;
      }

      in.endObject();
      return ret;
    }

    return null;
  }
}
