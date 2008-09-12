package org.datanucleus.store.appengine.query;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.DatastoreServiceFactory;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Query.FilterOperator;
import com.google.apphosting.api.datastore.Query.FilterPredicate;
import com.google.apphosting.api.datastore.Query.SortDirection;
import com.google.apphosting.api.datastore.Query.SortPredicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.datanucleus.jpa.JPAQuery;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.appengine.JPATestCase;
import org.datanucleus.test.Book;
import org.datanucleus.test.HasAncestorJPA;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.persistence.Query;

public class JPQLQueryTest extends JPATestCase {

  private static final List<SortPredicate> NO_SORTS = Collections.emptyList();
  private static final List<FilterPredicate> NO_FILTERS = Collections.emptyList();

  private static final FilterPredicate TITLE_EQ_2 =
      new FilterPredicate("title", FilterOperator.EQUAL, 2L);
  private static final FilterPredicate TITLE_EQ_2STR =
      new FilterPredicate("title", FilterOperator.EQUAL, "2");
  private static final FilterPredicate ISBN_EQ_4 =
      new FilterPredicate("isbn", FilterOperator.EQUAL, 4L);
  private static final FilterPredicate TITLE_GT_2 =
      new FilterPredicate("title", FilterOperator.GREATER_THAN, 2L);
  private static final FilterPredicate TITLE_GTE_2 =
      new FilterPredicate("title", FilterOperator.GREATER_THAN_OR_EQUAL, 2L);
  private static final FilterPredicate ISBN_LT_4 =
      new FilterPredicate("isbn", FilterOperator.LESS_THAN, 4L);
  private static final FilterPredicate ISBN_LTE_4 =
      new FilterPredicate("isbn", FilterOperator.LESS_THAN_OR_EQUAL, 4L);
  private static final SortPredicate TITLE_ASC = new SortPredicate("title", SortDirection.ASCENDING);
  private static final SortPredicate ISBN_DESC = new SortPredicate("isbn", SortDirection.DESCENDING);

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

  public void test2Equals2OrderBy() {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    ds.put(newBook("Bar Book", "Joe Blow", "67890"));
    ds.put(newBook("Bar Book", "Joe Blow", "11111"));
    ds.put(newBook("Foo Book", "Joe Blow", "12345"));
    ds.put(newBook("A Book", "Joe Blow", "54321"));
    ds.put(newBook("Baz Book", "Jane Blow", "13579"));

    Query q = em.createQuery("SELECT FROM " +
        Book.class.getName() +
        " WHERE author = 'Joe Blow'" +
        " ORDER BY title DESC, isbn ASC");

    @SuppressWarnings("unchecked")
    List<Book> result = (List<Book>) q.getResultList();

    assertEquals(4, result.size());
    assertEquals("12345", result.get(0).getIsbn());
    assertEquals("11111", result.get(1).getIsbn());
    assertEquals("67890", result.get(2).getIsbn());
    assertEquals("54321", result.get(3).getIsbn());
  }

  public void testBindVariables() {

    assertQuerySupported("select from " + Book.class.getName() + " where title = :title",
        Lists.newArrayList(TITLE_EQ_2), NO_SORTS, "title", 2L);

    assertQuerySupported("select from " + Book.class.getName()
        + " where title = :title AND isbn = :isbn",
        Lists.newArrayList(TITLE_EQ_2, ISBN_EQ_4), NO_SORTS, "title", 2L, "isbn", 4L);

    assertQuerySupported("select from " + Book.class.getName()
        + " where title = :title AND isbn = :isbn order by title asc, isbn desc",
        Lists.newArrayList(TITLE_EQ_2, ISBN_EQ_4),
        Lists.newArrayList(TITLE_ASC, ISBN_DESC), "title", 2L, "isbn", 4L);
  }

  public void testKeyQuery() {
    Entity bookEntity = newBook("Bar Book", "Joe Blow", "67890");
    ldth.ds.put(bookEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName()
            + " where id = :key");
    q.setParameter("key", KeyFactory.encodeKey(bookEntity.getKey()));
    @SuppressWarnings("unchecked")
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(bookEntity.getKey(), KeyFactory.decodeKey(books.get(0).getId()));
  }

  public void testKeyQueryWithSorts() {
    Entity bookEntity = newBook("Bar Book", "Joe Blow", "67890");
    ldth.ds.put(bookEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName()
            + " where id = :key order by isbn ASC");
    q.setParameter("key", KeyFactory.encodeKey(bookEntity.getKey()));
    @SuppressWarnings("unchecked")
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(bookEntity.getKey(), KeyFactory.decodeKey(books.get(0).getId()));
  }

  public void testIllegalKeyQuery_MultipleFilters() {
    Entity bookEntity = newBook("Bar Book", "Joe Blow", "67890");
    ldth.ds.put(bookEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName()
            + " where id = :key and isbn = \"4\"");
    q.setParameter("key", KeyFactory.encodeKey(bookEntity.getKey()));
    try {
      q.getResultList();
      fail("expected udfe");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }
  }

  public void testIllegalKeyQuery_NonEqualityFilter() {
    Entity bookEntity = newBook("Bar Book", "Joe Blow", "67890");
    ldth.ds.put(bookEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName()
            + " where id > :key");
    q.setParameter("key", KeyFactory.encodeKey(bookEntity.getKey()));
    try {
      q.getResultList();
      fail("expected udfe");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
    }
  }

  public void testAncestorQuery() {
    Entity bookEntity = newBook("Bar Book", "Joe Blow", "67890");
    ldth.ds.put(bookEntity);
    Entity hasAncestorEntity = new Entity(HasAncestorJPA.class.getName(), bookEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + HasAncestorJPA.class.getName() + " where ancestorId = :ancId");
    q.setParameter("ancId", KeyFactory.encodeKey(bookEntity.getKey()));

    @SuppressWarnings("unchecked")
    List<HasAncestorJPA> haList = (List<HasAncestorJPA>) q.getResultList();
    assertEquals(1, haList.size());
    assertEquals(bookEntity.getKey(), KeyFactory.decodeKey(haList.get(0).getAncestorId()));

    assertEquals(
        bookEntity.getKey(), getDatastoreQuery(q).getMostRecentDatastoreQuery().getAncestor());
    assertEquals(NO_FILTERS, getFilterPredicates(q));
    assertEquals(NO_SORTS, getSortPredicates(q));
  }

  public void testIllegalAncestorQuery() {
    Entity bookEntity = newBook("Bar Book", "Joe Blow", "67890");
    ldth.ds.put(bookEntity);
    Entity hasAncestorEntity = new Entity(HasAncestorJPA.class.getName(), bookEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + HasAncestorJPA.class.getName() + " where ancestorId > :ancId");
    q.setParameter("ancId", KeyFactory.encodeKey(bookEntity.getKey()));
    try {
      q.getResultList();
      fail ("expected udfe");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }
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
    Query q = em.createQuery(query);
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

  private void assertQuerySupported(String query, List<FilterPredicate> addedFilters,
      List<SortPredicate> addedSorts, Object... nameVals) {
    javax.persistence.Query q = em.createQuery(query);
    String name = null;
    for (Object nameOrVal : nameVals) {
      if (name == null) {
        name = (String) nameOrVal;
      } else {
        q.setParameter(name, nameOrVal);
        name = null;
      }
    }
    q.getResultList();

    assertEquals(addedFilters, getFilterPredicates(q));
    assertEquals(addedSorts, getSortPredicates(q));
  }

  private DatastoreQuery getDatastoreQuery(javax.persistence.Query q) {
    return ((JPQLQuery)((JPAQuery)q).getInternalQuery()).getDatastoreQuery();
  }

  private List<FilterPredicate> getFilterPredicates(javax.persistence.Query q) {
    return getDatastoreQuery(q).getMostRecentDatastoreQuery().getFilterPredicates();
  }

  private List<SortPredicate> getSortPredicates(javax.persistence.Query q) {
    return getDatastoreQuery(q).getMostRecentDatastoreQuery().getSortPredicates();
  }
}
