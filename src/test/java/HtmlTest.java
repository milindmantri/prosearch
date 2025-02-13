import static org.junit.jupiter.api.Assertions.*;

import com.milindmantri.Html;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class HtmlTest {

  @Test
  void voidTag() {
    var ordered = new LinkedHashMap<String, String>();
    ordered.putFirst("charset", "UTF-8");
    ordered.putLast("author", "Person");

    Html tag = new Html.VoidTag("meta", ordered);

    assertEquals(
        """
      <meta
      charset="UTF-8"
      author="Person"
      />\
      """,
        tag.toString());
  }

  @Test
  void brTag() {
    Html br = new Html.VoidTag("br");
    assertEquals("<br\n/>", br.toString());
  }

  @Test
  void div() {
    Html div = new Html.Tag("div");
    assertEquals("<div\n>\n</div>", div.toString());
  }

  @Test
  void divAttr() {
    Html div = new Html.Tag("div", Map.of("id", "div-id"));
    assertEquals(
        """
    <div
    id="div-id"
    >
    </div>\
    """,
        div.toString());
  }

  @Test
  void divBr() {
    Html div =
        new Html.Tag("div", Map.of("id", "div-id"), Stream.of(new Html.VoidTag("br")));
    assertEquals(
        """
  <div
  id="div-id"
  >
  <br
  />
  </div>\
  """,
        div.toString());
  }

  @Test
  void divInDiv() {
    Html div =
        new Html.Tag(
            "div",
            Map.of("id", "div-id"),
            Stream.of(
                new Html.VoidTag("br"), new Html.Tag("div", Map.of("id", "inner-div"))));
    assertEquals(
        """
      <div
      id="div-id"
      >
      <br
      />
      <div
      id="inner-div"
      >
      </div>
      </div>\
      """,
        div.toString());
  }
}
