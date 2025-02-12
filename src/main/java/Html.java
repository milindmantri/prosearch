import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public interface Html {

  record Tag(String name, Map<String, String> attributes, Stream<Html> innerTags) implements Html {
    /** empty tag */
    public Tag(final String name) {
      this(name, Collections.emptyMap(), Stream.empty());
    }

    /** inner elements only */
    public Tag(final String name, Stream<Html> innerTags) {
      this(name, Collections.emptyMap(), innerTags);
    }

    public Tag(final String name, Map<String, String> attributes) {
      this(name, attributes, Stream.empty());
    }

    public String toString() {
      // TODO: This can be done in a nice wrapper over string builder
      var sb = new StringBuilder();
      sb.append('<');
      sb.append(name);
      sb.append('\n');
      attributes.forEach(
          (key, value) -> {
            sb.append(key);
            sb.append('=');
            sb.append("\"");
            sb.append(value);
            sb.append("\"");
            sb.append('\n');
          });
      sb.append(">");
      sb.append('\n');

      innerTags
          .map(Html::toString)
          .forEach(
              str -> {
                sb.append(str);
                sb.append('\n');
              });

      sb.append("</");
      sb.append(name);
      sb.append(">");

      return sb.toString();
    }
  }

  record Void(String name, Map<String, String> attributes) implements Html {

    public Void(final String name) {
      this(name, Collections.emptyMap());
    }

    public String toString() {
      // TODO: This can be done in a nice wrapper over string builder
      var sb = new StringBuilder();
      sb.append('<');
      sb.append(name);
      sb.append('\n');
      attributes.forEach(
          (key, value) -> {
            sb.append(key);
            sb.append('=');
            sb.append("\"");
            sb.append(value);
            sb.append("\"");
            sb.append('\n');
          });
      sb.append("/>");

      return sb.toString();
    }
  }
}
