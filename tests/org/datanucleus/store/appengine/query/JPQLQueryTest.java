package org.datanucleus.store.appengine.query;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.TestCase;

import org.datanucleus.jpa.JPAQuery;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.appengine.LocalDatastoreTestHelper;
import org.datanucleus.test.Book;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.DatastoreServiceFactory;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Query;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class JPQLQueryTest extends TestCase {

  private static final List<AddedSort> NO_SORTS = Collections.emptyList();
  private static final List<AddedFilter> NO_FILTERS = Collections.emptyList();

  private static final AddedFilter TITLE_EQ_2 =
      new AddedFilter("title", Query.FilterOperator.EQUAL, 2L);
  private static final AddedFilter TITLE_EQ_2STR =
      new AddedFilter("title", Query.FilterOperator.EQUAL, "2");
  private static final AddedFilter ISBN_EQ_4 =
      new AddedFilter("isbn", Query.FilterOperator.EQUAL, 4L);
  private static final AddedFilter TITLE_GT_2 =
      new AddedFilter("title", Query.FilterOperator.GREATER_THAN, 2L);
  private static final AddedFilter TITLE_GTE_2 =
      new AddedFilter("title", Query.FilterOperator.GREATER_THAN_OR_EQUAL, 2L);
  private static final AddedFilter ISBN_LT_4 =
      new AddedFilter("isbn", Query.FilterOperator.LESS_THAN, 4L);
  private static final AddedFilter ISBN_LTE_4 =
      new AddedFilter("isbn", Query.FilterOperator.LESS_THAN_OR_EQUAL, 4L);
  private static final AddedSort TITLE_ASC = new AddedSort("title", Query.SortDirection.ASCENDING);
  private static final AddedSort ISBN_DESC = new AddedSort("isbn", Query.SortDirection.DESCENDING);

  private LocalDatastoreTestHelper ldth = new LocalDatastoreTestHelper();
  private EntityManagerFactory emf;
  private EntityManager em;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    File f = new File("local_db.bin");
    f.delete();
    emf = Persistence.createEntityManagerFactory("test");
    em = emf.createEntityManager();
    ldth.setUp();
  }

  public void testUnsupportedFilters() {
    String baseQuery = "SELECT FROM " + Book.class.getName() + " ";

    assertQueryUnsupported(baseQuery + "GROUP BY author", DatastoreQuery.GROUP_BY_OP);
    // Can't actually test having because the parser doesn't recognize it unless there is a
    // group by, and the group by gets seen first.
    assertQueryUnsupported(baseQuery + "GROUP BY author HAVING title = 'foo'", DatastoreQuery.GROUP_BY_OP);

    Set<Expression.Operator> unsupportedOps = Sets.newHashSet(DatastoreQuery.UNSUPPORTED_OPERATORS);
    baseQuery += "WHERE ";
    assertQueryUnsupported(baseQuery + "title = 'foo' OR title = 'bar'", Expression.OP_OR, unsupportedOps);
    assertQueryUnsupported(baseQuery + "NOT title = 'foo'", Expression.OP_NOT, unsupportedOps);
    assertQueryUnsupported(baseQuery + "(title + author) = 'foo'", Expression.OP_ADD, unsupportedOps);
    assertQueryUnsupported(baseQuery + "title + author = 'foo'", Expression.OP_ADD, unsupportedOps);
    assertQueryUnsupported(baseQuery + "(title - author) = 'foo'", Expression.OP_SUB, unsupportedOps);
    assertQueryUnsupported(baseQuery + "title - author = 'foo'", Expression.OP_SUB, unsupportedOps);
    assertQueryUnsupported(baseQuery + "(title / author) = 'foo'", Expression.OP_DIV, unsupportedOps);
    assertQueryUnsupported(baseQuery + "title / author = 'foo'", Expression.OP_DIV, unsupportedOps);
    assertQueryUnsupported(baseQuery + "(title * author) = 'foo'", Expression.OP_MUL, unsupportedOps);
    assertQueryUnsupported(baseQuery + "title * author = 'foo'", Expression.OP_MUL, unsupportedOps);
    assertQueryUnsupported(baseQuery + "(title % author) = 'foo'", Expression.OP_MOD, unsupportedOps);
    assertQueryUnsupported(baseQuery + "title % author = 'foo'", Expression.OP_MOD, unsupportedOps);
    assertQueryUnsupported(baseQuery + "title LIKE 'foo%'", Expression.OP_LIKE, unsupportedOps);

    assertEquals(Sets.newHashSet(Expression.OP_CONCAT, Expression.OP_COM,
        Expression.OP_NEG, Expression.OP_IS, Expression.OP_BETWEEN,
        Expression.OP_ISNOT), unsupportedOps);
  }

  public void testSupportedFilters() {
    String baseQuery = "SELECT FROM " + Book.class.getName() + " ";

    assertQuerySupported(baseQuery, NO_FILTERS, NO_SORTS);

    baseQuery += "WHERE ";
    assertQuerySupported(baseQuery + "title = 2", Lists.newArrayList(TITLE_EQ_2), NO_SORTS);
    assertQuerySupported(baseQuery + "title = \"2\"", Lists.newArrayList(TITLE_EQ_2STR), NO_SORTS);
    assertQuerySupported(baseQuery + "(title = 2)", Lists.newArrayList(TITLE_EQ_2), NO_SORTS);
    assertQuerySupported(baseQuery + "title = 2 AND isbn = 4", Lists.newArrayList(TITLE_EQ_2,
        ISBN_EQ_4), NO_SORTS);
    assertQuerySupported(baseQuery + "(title = 2 AND isbn = 4)", Lists.newArrayList(TITLE_EQ_2,
        ISBN_EQ_4), NO_SORTS);
    assertQuerySupported(baseQuery + "(title = 2) AND (isbn = 4)", Lists.newArrayList(
        TITLE_EQ_2, ISBN_EQ_4), NO_SORTS);
    assertQuerySupported(baseQuery + "title > 2", Lists.newArrayList(TITLE_GT_2), NO_SORTS);
    assertQuerySupported(baseQuery + "title >= 2", Lists.newArrayList(TITLE_GTE_2), NO_SORTS);
    assertQuerySupported(baseQuery + "isbn < 4", Lists.newArrayList(ISBN_LT_4), NO_SORTS);
    assertQuerySupported(baseQuery + "isbn <= 4", Lists.newArrayList(ISBN_LTE_4), NO_SORTS);
    assertQuerySupported(baseQuery + "(title > 2 AND isbn < 4)", Lists.newArrayList(TITLE_GT_2,
        ISBN_LT_4), NO_SORTS);

    baseQuery = "SELECT FROM " + Book.class.getName() + " ";
    assertQuerySupported(baseQuery + "ORDER BY title ASC", NO_FILTERS, Lists.newArrayList(TITLE_ASC));
    assertQuerySupported(baseQuery + "ORDER BY isbn DESC", NO_FILTERS, Lists.newArrayList(ISBN_DESC));
    assertQuerySupported(baseQuery + "ORDER BY title ASC, isbn DESC", NO_FILTERS,
        Lists.newArrayList(TITLE_ASC, ISBN_DESC));

    assertQuerySupported(baseQuery + "WHERE title = 2 AND isbn = 4 ORDER BY title ASC, isbn DESC",
        Lists.newArrayList(TITLE_EQ_2, ISBN_EQ_4), Lists.newArrayList(TITLE_ASC, ISBN_DESC));
  }

  @SuppressWarnings("unchecked")
  public void test2Equals2OrderBy() {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    ds.put(newBook("Bar Book", "Joe Blow", "67890"));
    ds.put(newBook("Bar Book", "Joe Blow", "11111"));
    ds.put(newBook("Foo Book", "Joe Blow", "12345"));
    ds.put(newBook("A Book", "Joe Blow", "54321"));
    ds.put(newBook("Baz Book", "Jane Blow", "13579"));

    javax.persistence.Query q = em.createQuery("SELECT FROM " +
        Book.class.getName() +
        " WHERE author = 'Joe Blow'" +
        " ORDER BY title DESC, isbn ASC");

    List<Book> result = (List<Book>) q.getResultList();

    assertEquals(4, result.size());
    assertEquals("12345", result.get(0).getIsbn());
    assertEquals("11111", result.get(1).getIsbn());
    assertEquals("67890", result.get(2).getIsbn());
    assertEquals("54321", result.get(3).getIsbn());
  }

  private static Entity newBook(String title, String author, String isbn) {
    Entity e = new Entity(Book.class.getName());
    e.setProperty("title", title);
    e.setProperty("author", author);
    e.setProperty("isbn", isbn);
    return e;
  }

  private void assertQueryUnsupported(String query,
      Expression.Operator unsupportedOp) {
    javax.persistence.Query q = em.createQuery(query);
    try {
      q.getResultList();
      fail("expected UnsupportedOperationException for query <" + query + ">");
    } catch (DatastoreQuery.UnsupportedDatastoreOperatorException uoe) {
      // Good.
      assertEquals(unsupportedOp, uoe.getOperation());
    }
  }

  private void assertQueryUnsupported(String query,
      Expression.Operator unsupportedOp,
      Set<Expression.Operator> unsupportedOps) {
    assertQueryUnsupported(query, unsupportedOp);
    unsupportedOps.remove(unsupportedOp);
  }

  private void assertQuerySupported(String query, List<AddedFilter> addedFilters,
      List<AddedSort> addedSorts) {
    javax.persistence.Query q = em.createQuery(query);
    q.getResultList();
    DatastoreQuery dq = ((JPQLQuery)((JPAQuery)q).getInternalQuery()).getDatastoreQuery();

    assertEquals(addedFilters, dq.getAddedFilters());
    assertEquals(addedSorts, dq.getAddedSorts());
  }
}
