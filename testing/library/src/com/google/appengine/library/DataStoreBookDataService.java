// Copyright 2008 Google Inc. All Rights Reserved.

package com.google.appengine.library;

import static com.google.apphosting.api.datastore.FetchOptions.Builder.withLimit;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.DatastoreServiceFactory;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.FetchOptions;
import com.google.apphosting.api.datastore.Query;
import com.google.apphosting.api.datastore.Query.FilterOperator;
import com.google.apphosting.api.datastore.Query.SortDirection;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * @author kjin@google.com (Kevin Jin)
 * 
 */
final class DataStoreBookDataService implements BookDataService {
  private final DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();

  @Override
  public Iterable<Book> asIterable(String jpqlQuery) {
    return new BookIterable(datastoreService.prepare(new QueryConverter().convertQuery(jpqlQuery))
        .asIterable());
  }

  @Override
  public Iterable<Book> asIterable(String jpqlQuery, int limit, int offset) {
    FetchOptions fo = withLimit(limit).offset(offset);
    return new BookIterable(datastoreService.prepare(new QueryConverter().convertQuery(jpqlQuery))
        .asIterable(fo));
  }

  @Override
  public int countEntities(String jpqlQuery) {
    return datastoreService.prepare(new QueryConverter().convertQuery(jpqlQuery)).countEntities();
  }

  @Override
  public void delete(Book book) {
    datastoreService.delete(((BookWithEntity) book).e.getKey());
  }

  @Override
  public void put(Book book) {
    final Entity e;
    if (book instanceof BookWithEntity) {
      e = ((BookWithEntity) book).e;
    } else {
      e = new Entity("Book");
    }
    e.setProperty("category", book.getCategory());
    e.setProperty("lastname", book.getLastname());
    e.setProperty("firstname", book.getFirstname());
    e.setProperty("title", book.getTitle());
    e.setProperty("created", book.getCreated());
    e.setProperty("year", book.getYear());
    datastoreService.put(e);
  }

  private static final class BookWithEntity extends Book {
    private final Entity e;

    private BookWithEntity(String category, Date created, String firstname, String lastname,
        String title, long year, Entity e) {
      super(category, created, firstname, lastname, title, year);
      this.e = e;
    }
  }

  private static final class BookIterable implements Iterable<Book> {

    @Override
    public Iterator<Book> iterator() {
      return new BookIterator(it.iterator());
    }

    private BookIterable(Iterable<Entity> it) {
      super();
      this.it = it;
    }

    private final Iterable<Entity> it;
  }

  private static final class BookIterator implements Iterator<Book> {

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public Book next() {
      Book book = null;
      final Entity e = it.next();
      if (e != null) {
        book =
            new BookWithEntity((String) e.getProperty("category"), (Date) e.getProperty("created"),
                (String) e.getProperty("firstname"), (String) e.getProperty("lastname"), (String) e
                    .getProperty("title"), ((Long) e.getProperty("year")).longValue(), e);
      }
      return book;
    }

    @Override
    public void remove() {
      it.remove();
    }

    private BookIterator(Iterator<Entity> it) {
      super();
      this.it = it;
    }

    private final Iterator<Entity> it;
  }

  private static final class QueryConverter {
    /**
     * Converts a JPQL string into {@code Query}. Only parses the format output
     * by {@code Library}, e.g. WHERE category='test' AND year>1800 ORDER BY
     * year.
     */
    private Query convertQuery(String jpqlQuery) {
      query = new Query("Book");

      tokenizer = new StringTokenizer(jpqlQuery);
      if (tokenizer.hasMoreTokens()) {
        String lastToken = tokenizer.nextToken();
        if (lastToken.equalsIgnoreCase("WHERE")) {
          do {
            parseFilter();
            if (!tokenizer.hasMoreTokens()) {
              break;
            }
            lastToken = tokenizer.nextToken();
          } while (lastToken.equals("AND"));
        }
        if (lastToken.equals("ORDER")) {
          lastToken = tokenizer.nextToken(); // skip "BY"
          assert lastToken.equals("BY");
          while (parseSort()) {
          }
        }
      }

      return query;
    }

    private boolean parseSort() {
      String propName = tokenizer.nextToken();
      String direction = tokenizer.nextToken();
      boolean moreSort = direction.endsWith(",");
      if (moreSort) {
        direction = direction.substring(0, direction.length() - 1);
      }

      SortDirection sd = null;
      if (direction.equals("ASC")) {
        sd = SortDirection.ASCENDING;
      } else if (direction.equals("DESC")) {
        sd = SortDirection.DESCENDING;
      } else {
        assert false : direction + " is not ASC or DESC";
      }
      query.addSort(propName, sd);
      return moreSort;
    }

    private void parseFilter() {
      String filter = tokenizer.nextToken();
      int opIndex = -1;
      for (String op : OPERATORS) {
        opIndex = filter.indexOf(op);
        if (opIndex != -1) {
          String propName = filter.substring(0, opIndex);
          FilterOperator operator = OPERATOR_MAP.get(op);
          String propValue = filter.substring(opIndex + op.length());
          propValue = parseValue(propValue);

          // special case: year has Long value
          if (propName.equals("year")) {
            query.addFilter(propName, operator, Long.parseLong(propValue));
          } else {
            query.addFilter(propName, operator, propValue);
          }
          break;
        }
      }
      assert opIndex != -1 : filter + "missing comparison operator";
    }

    private String parseValue(String propValue) {
      // not a literal
      if (!propValue.startsWith("'")) {
        return propValue;
      }

      while (!propValue.endsWith("'")) {
        // an incomplete literal
        propValue += " " + tokenizer.nextToken();
      }
      propValue = propValue.substring(1, propValue.length() - 1);
      return propValue;
    }

    private StringTokenizer tokenizer;
    private Query query;

    private static final String[] OPERATORS = {">=", ">", "<=", "<", "="};
    private static final FilterOperator[] OPERATOR_ENUMS =
        {FilterOperator.GREATER_THAN_OR_EQUAL, FilterOperator.GREATER_THAN,
            FilterOperator.LESS_THAN_OR_EQUAL, FilterOperator.LESS_THAN, FilterOperator.EQUAL};

    private static final Map<String, FilterOperator> OPERATOR_MAP =
        new HashMap<String, FilterOperator>();
    static {
      for (int ii = 0; ii < OPERATORS.length; ii++) {
        OPERATOR_MAP.put(OPERATORS[ii], OPERATOR_ENUMS[ii]);
      }
    }
  }

  @Override
  public void close() {
    // no clean-up needed for DataStore.
  }
}
