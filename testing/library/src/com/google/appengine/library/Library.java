package com.google.appengine.library;

import com.google.appengine.library.Util.NullToEmptyMapWrapper;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This is a test servlet ported from //apphosting/testing/testshop/library.py.
 * I try to maintain the same user interface so the same Selenium tests can run
 * for this Java port. The implementation can be different. For example, ORM API
 * can be used instead of the Python datastore API.
 * 
 * There are four action buttons (and a clear form button which is implemented
 * in javascript) on the Prometheus Library.
 * 
 * Import - Updates the working 'Book' entities with copies from the Classics or
 * TechBooks catalog as specified by the Catalog form entry.
 * 
 * Query - Performs a query on 'Book' entities depending on the field values and
 * checkbox values.
 * 
 * Add - Adds a 'Book' entity with values from the lastname, firstname, year and
 * title fields.
 * 
 * Delete - Similar to query, but instead of returning the results of the query,
 * it deletes those entities.
 * 
 * TODO(kjin): use JSP for HTML content.
 * 
 * @author kjin@google.com (Kevin Jin)
 */
public class Library extends HttpServlet {
  private static final Logger log = Logger.getLogger(Library.class.getName());
  static {
    log.setLevel(Level.FINE);
  }

  @Override
  public void service(HttpServletRequest req, HttpServletResponse resp) throws IOException,
      ServletException {
    try {
      super.service(req, resp);
    } finally {
      if (bds != null) {
        bds.close();
        bds = null;
      }
    }
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException,
      ServletException {
    if (bds == null) {
      // doGet may be called by doPost. Two EntityManagers don't sync when using
      // JPA.
      bds = BookDataServiceFactory.getBookDataService();
    }
    formFields = Util.wrapFormFields(req);
    String query = " WHERE category='test'";
    query = handleQueryOrder(query);
    render(query, resp);
  }

  /**
   * Renders Each book entity in the query which has been built unless
   * displaying count-only has been specified (checkmarked). Also uses getNumber
   * if specified to expand test coverage.
   * 
   * @throws IOException
   */
  private void render(String query, HttpServletResponse resp)
      throws IOException {
    PrintWriter out = resp.getWriter();

    out.println(HEADER);
    if (formFields.get("DispCountOnly").equals("countonly")) {
      int total = bds.countEntities(query);
      out.println(" <p class=\"entry\"> Found " + total + " books </p> ");
    } else {
      int limit = Integer.MAX_VALUE;
      if (formFields.get("UseGetQuery").equals("UseGet")) {
        limit = Integer.parseInt(formFields.get("getNumber"));
      }

      for (Book book : bds.asIterable(query, limit, 0)) {
        out.println(getBookHtml(book));
      }
    }
    out.println(FOOTER);
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException,
      ServletException {
    bds = BookDataServiceFactory.getBookDataService();
    formFields = Util.wrapFormFields(req);
    String action_type = formFields.get("action_type");

    // Handle any buttons that got pushed on the form
    if (action_type.equals("Add")) {
      String category = formFields.get("entity");
      if (category.equals("")) {
        category = "test";
      }
      String title = formFields.get("title");
      addBook(category, formFields.get("lastname"), formFields.get("firstname"), title,
          Integer.parseInt(formFields.get("year")),
          !title.contains("'"));        //hack for ' not supported in JPQL
      doGet(req, resp);
    } else if (action_type.equals("Query")) {
      handleQuery(req, resp, false);
    } else if (action_type.equals("Delete")) {
      handleQuery(req, resp, true);
    } else if (action_type.equals("Import")) {
      String catalogtype = formFields.get("catalogtype");
      if (catalogtype.equals("SyntheticSmall")) {
        createSyntheticCatalog(20);
      } else if (catalogtype.equals("TechBooks") || catalogtype.equals("Classics")) {
        copyCatalog(catalogtype, "test");
      }
      doGet(req, resp);
    }
  }

  /*
   * We will use a script that calls wget (or curl) to initially populate two
   * sets of entities called 'Classics', and 'TechBooks'. Pushing the import
   * button will copy the appropriate entities into Book entities by using this
   * method to retrieve them from Classics or TechBooks and calling addBook to
   * put them in Book.
   */
  private void copyCatalog(String src, String dest) {
    long start = System.currentTimeMillis();
    String query = " WHERE category='" + src + "'";
    query += " ORDER BY title DESC";
    long endBuildQuery = System.currentTimeMillis();
    log.fine("build Query time = " + (endBuildQuery - start));
    Iterable<Book> it = bds.asIterable(query);
    long endQuery = System.currentTimeMillis();
    log.fine("asIterable time = " + (endQuery - endBuildQuery));

    int ii = 0;
    long startii = System.currentTimeMillis();
    for (Book book : it) {
      long endii = System.currentTimeMillis();
      log.fine("iterator " + (ii++) + " time = " + (endii - startii));
      startii = endii;
      addBook(dest, book.getLastname(), book.getFirstname(), book.getTitle(), book.getYear(), false);
    }
    long end = System.currentTimeMillis();
    log.fine("copyCatalog time = " + (end - start));
  }

  private void createSyntheticCatalog(int count) {
    for (int i = 0; i < count; i++) {
      addBook("test", "Last " + i, "First " + i, "Title " + i, 1800 + i % 200, false);
    }
  }

  /*
   * Handle various types of user queries using all of or selected properties.
   * The form will allow the user to type arithmetic queries in the year field
   * such as >, >=, <, <= or ==, = No relational operator defaults to =. If the
   * delete button was pushed instead of query, then delete the specified
   * entities.
   */
  private void handleQuery(HttpServletRequest req, HttpServletResponse resp, boolean delete)
      throws IOException, ServletException {
    String query = " WHERE category='test'";

    query = handleInList("lastname", query);
    query = handleInList("firstname", query);

    query = addFilter("title", query);
    query = addFilter("year", query);

//    String queryHint = formFields.get("queryHint");
//    if (queryHint.equals("order_first")) {
//      query.setHint(Query.Hint.ORDER_FIRST);
//    } else if (queryHint.equals("ancestor_first")) {
//      query.setHint(Query.Hint.ANCESTOR_FIRST);
//    } else if (queryHint.equals("filter_first")) {
//      query.setHint(Query.Hint.FILTER_FIRST);
//    }

    if (delete) {
      // sometimes NPE is thrown at
      // com.google.apphosting.api.datastore.dev.LocalDatastoreService
      // .next(LocalDatastoreService.java:659) when using JPA.
      for (Book book : bds.asIterable(query)) {
        bds.delete(book);
      }
      doGet(req, resp);
    } else {
      query = handleQueryOrder(query);
      render(query, resp);
    }
  }

  private String handleQueryOrder(String query) {
    String selType = formFields.get("selType");
    if (selType.length() != 0 && !selType.equals("none")) {
      query +=
          " ORDER BY " + selType + " " + (formFields.get("selOrder").equals("ascending") ? "ASC" : "DESC");

      String secType = formFields.get("secType");
      if (secType.length() != 0 && !secType.equals("none")) {
        query +=
            ", " + secType + " "
                + (formFields.get("secOrder").equals("ascending") ? "ASC" : "DESC");
      }
    }
    return query;
  }

  /*
   * in operator is not supported, but anyway I ported this function for future
   * when in operator is supported. Form fields such as lastname or firstname
   * can contain multiple words which can be used to create queries with the in
   * operator.
   * 
   * query.update is not supported in Java.
   */
  private String handleInList(String propertyName, String query) {
    String UseQueryUpdate = formFields.get("UseQueryUpdate");
    if (UseQueryUpdate.equals("UseUpdate")) {
      log.warning("Query.update not supported");
    }
    String propertyValue = formFields.get(propertyName);
    if (propertyValue != null && propertyValue.length() != 0) {
      query += " AND " + propertyName + "='" + propertyValue + "'";
    }
    return query;
  }

  private String addFilter(String propertyName, String query) {
    String op = "=";
    String propertyValue = formFields.get(propertyName);
    if (propertyValue != null && propertyValue.length() != 0) {
      if (propertyName.equals("year")) {
        final String[] operators = {">=", ">", "<=", "<", "==", "="};
        for (final String operator : operators) {
          int index = propertyValue.indexOf(operator);
          if (index != -1) {
            propertyValue = propertyValue.substring(index + operator.length()).trim();
            if (!operator.equals("==")) {
              op = operator;
            }
            // hack for ApplicationError: 1: The first sort property must be the
            // same as the property to which the inequality filter is applied.
            formFields.put("selType", "year");
            break;
          }
        }
      } else { // treat as String literal in JPQL
        propertyValue = "'" + propertyValue + "'";
      }
      query += " AND " + propertyName + op + propertyValue;
    }
    return query;
  }

  /**
   * Puts the Book entity into datastore and does a check for duplicates if
   * {@code check == true}.
   */
  private void addBook(String category, String lastname, String firstname, String title, int year,
      boolean check) {
    if (title == null || title.length() == 0) {
      return;
    }

    if (check) {
      String query = " WHERE category='" + category + "' AND title='" + title + "'";
      if (bds.countEntities(query) > 0) {
        return;
      }
    }
    
    long start = System.currentTimeMillis();
    Book book = new Book(category, new Date(),firstname, lastname, title, year);
    bds.put(book);
    log.fine("put time = " + (System.currentTimeMillis()-start));
}
  
  private String getBookHtml(Book book) {
    return String.format(
      "<p class=\"entry\"> %s \n" +
      "<br />\n" +
      "&nbsp;&nbsp;%s,<i> %s</i>  &copy;%s\n" +
      "<br /> \n" +
      "&nbsp;&nbsp;Created on %s\n" +
      "</p>",
      book.getLastname(), book.getFirstname(), book.getTitle(), book.getYear(), book
        .getCreated());
  }

  private static final String HEADER = ""+
  "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\"\n" +
  "\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">\n" +
  "<html>\n" +
  "<head>\n" +
  "<script type=\"text/javascript\">\n" +
  "function disable()\n" +
    "{\n" +
    "document.getElementById(\"getNumber\").disabled=true\n" +
    "}\n" +
  "function enable()\n" +
    "{\n" +
    "document.getElementById(\"getNumber\").disabled=false\n" +
    "}\n" +
  "</script>\n" +
  "<title> Prometheus Library </title>\n" +
  "<style type=\"text/css\">\n" +
  "body {\n" +
    "width: 900px;\n" +
    "margin: 25px;\n" +
  "}\n" +

  "p.entry, div.form {\n" +
    "padding: 10px;\n" +
    "border: 1px solid Navy;\n" +
    "width: auto;\n" +
  "}\n" +

  "p.entry { background-color: Ivory; }\n" +
  "div.form { background-color: LightSteelBlue; }\n" +
  "</style>\n" +
  "</head>\n" +

  "<body onload=\"disable()\">\n" +
  "<div id=\"main\">\n" +
  "<div id=\"body\">\n" +

  "<h1> Prometheus Library </h1>\n" +

  "<p style=\"font-style: italic\">\n" +
  "This is an example app for\n" +
  "<a href=\"http://wiki/Main/Prometheus\"> Prometheus</a>.\n" +
  "It is based on the sample guestbook app by Ryan Barrett.\n" +
  "It attempts to use all of datastore types supported by\n" +
  "the\n" +
  "<a href=\"http://wiki/Main/PrometheusDatastoreAPI\">\n" +
  "datastore api.\n" +
  "</a>\n" +
  "You can see the items in the datastore by clicking on\n" +
  "<a href=\"/admin/\">\n" +
  "the datastore viewer.\n" +
  "</a>\n" +
  "<br />\n" +
  "To run the automated test suite under Selenium Core\n" +
  "which is also being hosted by Prometheus\n" +
  "<a href=\"/core/TestRunner.html\"> click here</a>.\n" +
  "</p>\n" +
  "<p>\n" +
  "</p>\n" +
  "<hr />\n" +
  "<div class=\"form\">\n" +
  "<B>Admin or Query the Library</B>\n" +
  "<p></p>\n" +
  "<form action=\"/library\" method=\"post\">\n" +
  "<table>\n" +
  "<tr><td> Catalog </td>\n" +
  "<td> <select name=\"catalogtype\">\n" +
  "<option selected=\"selected\" value=\"Classics\">Select Catalog</option>\n" +
  "<option value=\"Classics\">Classics</option>\n" +
  "<option value=\"TechBooks\">Technical Books</option>\n" +
  "<option value=\"SyntheticSmall\">Synthetically Generated (Small ~20)</option>\n" +
  "</select> </td>\n" +
  "<tr></tr>\n" +
  "<tr><td> Author (Last): </td><td><input type=\"text\" name=\"lastname\" size=\"20\"</td>\n" +
  "<tr><td> Author (First): </td><td><input type=\"text\" name=\"firstname\" size=\"20\"</td>\n" +
  "<tr><td> Year: </td><td><input type=\"text\" name=\"year\" size=\"10\"</td>\n" +
  "<tr><td> ISBN: </td><td><input type=\"text\" name=\"isbn\" value=\"NOT USED\" size=\"10\"" +
  "</td>\n" +
  "<tr><td> Title:</td><td><input type=\"text\" name=\"title\" size=\"50\"</td></tr>\n" +
  "<tr><td> Description: </td><td><textarea name=\"message\" rows=\"10\" cols=\"50\">NOT USED " +
  "</textarea> </td></tr>\n" +
  "<tr><td></td>\n" +
  "<td><input type=\"Reset\" value=\"Reset Form\">\n" +
  "<input type=\"submit\" value=\"Import\"   name=\"action_type\">\n" +
  "<input type=\"submit\" value=\"Query\" name=\"action_type\">\n" +
  "<input type=\"submit\" value=\"Add\"   name=\"action_type\">\n" +
  "<input type=\"submit\" value=\"Delete\"   name=\"action_type\"></td></tr>\n" +
  "<tr><td></td>\n" +
  "<td><select name=\"selType\">\n" +
  "<option selected=\"selected\" value=\"lastname\">Select Order Key</option>\n" +
  "<option value=\"lastname\">by Last Name</option>\n" +
  "<option value=\"firstname\">by First Name</option>\n" +
  "<option value=\"year\">by Year</option>\n" +
  "<option value=\"title\">by Title</option>\n" +
  "<option value=\"created\">by creation date</option>\n" +
  "</select>\n" +
  "<select name=\"selOrder\">\n" +
  "<option selected=\"selected\" value=\"Descending\">Select Query Order</option>\n" +
  "<option value=\"descending\">Descending</option>\n" +
  "<option value=\"ascending\">Ascending</option>\n" +
  "</select>\n" +
  "</td>\n" +
  "</tr>\n" +
  "<tr><td></td>\n" +
  "</select>\n" +
  "<td><select name=\"secType\">\n" +
  "<option selected=\"selected\" value=\"none\">Secondary Order Key</option>\n" +
  "<option value=\"none\">NONE</option>\n" +
  "<option value=\"lastname\">by Last Name</option>\n" +
  "<option value=\"firstname\">by First Name</option>\n" +
  "<option value=\"year\">by Year</option>\n" +
  "<option value=\"title\">by Title</option>\n" +
  "</select>\n" +
  "<select name=\"secOrder\">\n" +
  "<option selected=\"selected\" value=\"none\">Secondary Query Order</option>\n" +
  "<option value=\"none\">NONE</option>\n" +
  "<option value=\"descending\">Descending</option>\n" +
  "<option value=\"ascending\">Ascending</option>\n" +
  "</select>\n" +
  "<select name=\"queryHint\">\n" +
  "<option selected=\"selected\" value=\"none\">Query Hint</option>\n" +
  "<option value=\"none\">NONE</option>\n" +
  "<option value=\"order_first\">ORDER_FIRST</option>\n" +
  "<option value=\"ancestor_first\">ANCESTOR_FIRST</option>\n" +
  "<option value=\"filter_first\">FILTER_FIRST</option>\n" +
  "</select>\n" +
  "</td>\n" +
  "</tr>\n" +
  "<tr><td></td>\n" +
  "<td>\n" +
  "<input type=\"checkbox\" name=\"DispCountOnly\" value=\"countonly\">\n" +
  "Display Count Only\n" +
  "</td>\n" +
  "</tr>\n" +
  "<tr><td></td>\n" +
  "<td>\n" +
  "<input type=\"checkbox\" name=\"UseQueryUpdate\" value=\"UseUpdate\">\n" +
  "Use Query update method to set property filters\n" +
  "</td>\n" +
  "</tr>\n" +
  "<tr><td></td>\n" +
  "<td>\n" +
  "<input type=\"checkBox\" onclick=\"if (this.checked) (enable()); else (disable())\" " +
  "name=\"UseGetQuery\" value=\"UseGet\" >\n" +
  "Use Get() instead of Run() for up to\n" +
  "<select name=\"getNumber\" id=\"getNumber\">\n" +
  "<option value=\"1\">1</option>\n" +
  "<option value=\"5\">5</option>\n" +
  "<option value=\"10\">10</option>\n" +
  "<option value=\"20\">20</option> \n" +
  "</select> results\n" +
  "</td>\n" +
  "</tr>\n" +
  "</table>\n" +
  "</form>\n" +
  "</div>\n";

  private static final String FOOTER =
  "</div></div>\n" +
  "</body>\n" +
  "</html>\n";

  private NullToEmptyMapWrapper formFields;
  private BookDataService bds;
}
