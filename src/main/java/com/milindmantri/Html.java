package com.milindmantri;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@FunctionalInterface
public interface Html {

  String toHtml();

  record Tag(String name, Map<String, String> attributes, Stream<Html> innerTags) implements Html {
    /** empty tag */
    public Tag(final String name) {
      this(name, Collections.emptyMap(), Stream.empty());
    }

    public Tag(final String name, final String innerText) {
      this(name, Collections.emptyMap(), Stream.of(new Inner(innerText)));
    }

    /** inner elements only */
    public Tag(final String name, Stream<Html> innerTags) {
      this(name, Collections.emptyMap(), innerTags);
    }

    public Tag(final String name, Map<String, String> attributes) {
      this(name, attributes, Stream.empty());
    }

    public String toHtml() {
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
          .map(Html::toHtml)
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

  record VoidTag(String name, Map<String, String> attributes) implements Html {

    public VoidTag(final String name) {
      this(name, Collections.emptyMap());
    }

    public String toHtml() {
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

  record Inner(Stream<String> text) implements Html {

    public Inner(String txt) {
      this(Stream.of(txt));
    }

    public String toHtml() {
      return text.collect(Collectors.joining());
    }
  }

  static Html title(String text) {
    return new Tag("title", text);
  }

  static Html h1(String text) {
    return new Tag("h1", text);
  }

  static Html h2(String text) {
    return new Tag("h2", text);
  }

  static Html h3(String text) {
    return new Tag("h3", text);
  }

  static Html head(Stream<Html> elements) {
    return new Tag("head", elements);
  }

  static Html body(Stream<Html> elements) {
    return new Tag("body", elements);
  }

  static Html div(Stream<Html> elements) {
    return new Tag("div", elements);
  }

  static Html a(String url, boolean openInNew, Stream<Html> elements) {
    return new Tag("a", Map.of("href", url, "target", openInNew ? "_blank" : "_self"), elements);
  }

  static Html p(String text) {
    return new Tag("p", text);
  }

  static Html pStrong(String text) {
    return new Tag("p", Stream.of(new Tag("strong", text)));
  }

  static Html section(Stream<Html> elements) {
    return new Tag("section", elements);
  }

  static Html formGET(String action, String submitText, Stream<Html> elements) {
    return new Tag(
        "form",
        Map.of("method", "get", "action", action),
        Stream.concat(
            elements, Stream.of(new Tag("input", Map.of("type", "submit", "value", submitText)))));
  }

  static Html inputText(String name, String value, String placeholder) {
    return new Tag(
        "input", Map.of("type", "text", "name", name, "value", value, "placeholder", placeholder));
  }

  static Html meta(Map<String, String> attributes) {
    return new Tag("meta", attributes);
  }

  static Html style(String css) {
    return new Tag("style", css);
  }

  static Html html(Stream<Html> elements) {
    return new Tag("html", Map.of("lang", "en"), elements);
  }
}
