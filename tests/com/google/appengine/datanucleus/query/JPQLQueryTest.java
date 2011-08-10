/**********************************************************************
 Copyright (c) 2009 Google Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 **********************************************************************/
package com.google.appengine.datanucleus.query;

import static com.google.appengine.datanucleus.test.Flight.newFlightEntity;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.Query.SortPredicate;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.dev.LocalDatastoreService;
import com.google.appengine.datanucleus.DatastoreServiceFactoryInternal;
import com.google.appengine.datanucleus.DatastoreServiceInterceptor;
import com.google.appengine.datanucleus.ExceptionThrowingDatastoreDelegate;
import com.google.appengine.datanucleus.PrimitiveArrays;
import com.google.appengine.datanucleus.TestUtils;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.WriteBlocker;
import com.google.appengine.datanucleus.jpa.JPATestCase;
import com.google.appengine.datanucleus.test.BidirectionalChildListJPA;
import com.google.appengine.datanucleus.test.BidirectionalChildLongPkListJPA;
import com.google.appengine.datanucleus.test.BidirectionalGrandchildListJPA;
import com.google.appengine.datanucleus.test.Book;
import com.google.appengine.datanucleus.test.DetachableJPA;
import com.google.appengine.datanucleus.test.Flight;
import com.google.appengine.datanucleus.test.HasBytesJPA;
import com.google.appengine.datanucleus.test.HasDoubleJPA;
import com.google.appengine.datanucleus.test.HasEncodedStringPkJPA;
import com.google.appengine.datanucleus.test.HasEncodedStringPkSeparateIdFieldJPA;
import com.google.appengine.datanucleus.test.HasEncodedStringPkSeparateNameFieldJPA;
import com.google.appengine.datanucleus.test.HasEnumJPA;
import com.google.appengine.datanucleus.test.HasKeyAncestorStringPkJPA;
import com.google.appengine.datanucleus.test.HasKeyPkJPA;
import com.google.appengine.datanucleus.test.HasLongPkJPA;
import com.google.appengine.datanucleus.test.HasMultiValuePropsJPA;
import com.google.appengine.datanucleus.test.HasOneToManyKeyPkListJPA;
import com.google.appengine.datanucleus.test.HasOneToManyKeyPkSetJPA;
import com.google.appengine.datanucleus.test.HasOneToManyListJPA;
import com.google.appengine.datanucleus.test.HasOneToManyLongPkListJPA;
import com.google.appengine.datanucleus.test.HasOneToManyLongPkSetJPA;
import com.google.appengine.datanucleus.test.HasOneToManyUnencodedStringPkListJPA;
import com.google.appengine.datanucleus.test.HasOneToManyUnencodedStringPkSetJPA;
import com.google.appengine.datanucleus.test.HasOneToOneJPA;
import com.google.appengine.datanucleus.test.HasOneToOneParentJPA;
import com.google.appengine.datanucleus.test.HasStringAncestorStringPkJPA;
import com.google.appengine.datanucleus.test.HasUnencodedStringPkJPA;
import com.google.appengine.datanucleus.test.InTheHouseJPA;
import com.google.appengine.datanucleus.test.KitchenSink;
import com.google.appengine.datanucleus.test.NullDataJPA;
import com.google.appengine.datanucleus.test.Person;
import com.google.appengine.datanucleus.test.AbstractBaseClassesJPA.Base1;
import com.google.appengine.datanucleus.test.UnidirectionalSingeTableChildJPA.UnidirTop;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.DatastorePb;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.api.jpa.JPAQuery;
import org.datanucleus.query.expression.Expression;
import org.easymock.EasyMock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import javax.persistence.NoResultException;
import javax.persistence.NonUniqueResultException;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.QueryTimeoutException;

import junit.framework.Assert;

/**
 * @author Max Ross <maxr@google.com>
 */
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
  private static final FilterPredicate TITLE_NEQ_NULL_LITERAL =
      new FilterPredicate("title", FilterOperator.NOT_EQUAL, null);
  private static final FilterPredicate TITLE_NEQ_2_LITERAL =
      new FilterPredicate("title", FilterOperator.NOT_EQUAL, 2L);
  private static final SortPredicate TITLE_ASC =
      new SortPredicate("title", SortDirection.ASCENDING);
  private static final SortPredicate ISBN_DESC =
      new SortPredicate("isbn", SortDirection.DESCENDING);
  private static final FilterPredicate TITLE_IN_2_ARGS =
      new FilterPredicate("title", FilterOperator.IN, Arrays.asList("2", 2L));
  private static final FilterPredicate TITLE_IN_3_ARGS =
      new FilterPredicate("title", FilterOperator.IN, Arrays.asList("2", 2L, false));

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    DatastoreServiceInterceptor.install(getStoreManager(), new WriteBlocker());
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      super.tearDown();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
  }

  @Override
  protected EntityManagerFactoryName getEntityManagerFactoryName() {
    return EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed;
  }

  public void testUnsupportedFilters_NoResultExpr() {
    String baseQuery = "SELECT FROM " + Book.class.getName() + " b ";
    testUnsupportedFilters(baseQuery);
  }

  public void testUnsupportedFilters_PrimaryResultExpr() {
    String baseQuery = "SELECT b FROM " + Book.class.getName() + " b ";
    testUnsupportedFilters(baseQuery);
  }

  private void testUnsupportedFilters(String baseQuery) {
    assertQueryUnsupportedByOrm(baseQuery + "GROUP BY author", DatastoreQuery.GROUP_BY_OP);
    // Can't actually test having because the parser doesn't recognize it unless there is a
    // group by, and the group by gets seen first.
    assertQueryUnsupportedByOrm(baseQuery + "GROUP BY author HAVING title = 'foo'",
                                DatastoreQuery.GROUP_BY_OP);
    assertQueryUnsupportedByOrm(
        "select avg(firstPublished) from " + Book.class.getName() + " b ",
        new Expression.Operator("avg", 0));

    Set<Expression.Operator> unsupportedOps =
        new HashSet<Expression.Operator>(DatastoreQuery.UNSUPPORTED_OPERATORS);
    baseQuery += "WHERE ";
    assertQueryUnsupportedByOrm(baseQuery + "NOT title = 'foo'", Expression.OP_NOT, unsupportedOps);
    assertQueryUnsupportedByOrm(baseQuery + "(title + author) = 'foo'", Expression.OP_ADD,
                                unsupportedOps);
    assertQueryUnsupportedByOrm(baseQuery + "title + author = 'foo'", Expression.OP_ADD,
                                unsupportedOps);
    assertQueryUnsupportedByOrm(baseQuery + "(title - author) = 'foo'", Expression.OP_SUB,
                                unsupportedOps);
    assertQueryUnsupportedByOrm(baseQuery + "title - author = 'foo'", Expression.OP_SUB,
                                unsupportedOps);
    assertQueryUnsupportedByOrm(baseQuery + "(title / author) = 'foo'", Expression.OP_DIV,
                                unsupportedOps);
    assertQueryUnsupportedByOrm(baseQuery + "title / author = 'foo'", Expression.OP_DIV,
                                unsupportedOps);
    assertQueryUnsupportedByOrm(baseQuery + "(title * author) = 'foo'", Expression.OP_MUL,
                                unsupportedOps);
    assertQueryUnsupportedByOrm(baseQuery + "title * author = 'foo'", Expression.OP_MUL,
                                unsupportedOps);
    assertQueryUnsupportedByOrm(baseQuery + "(title % author) = 'foo'", Expression.OP_MOD,
                                unsupportedOps);
    assertQueryUnsupportedByOrm(baseQuery + "title % author = 'foo'", Expression.OP_MOD,
                                unsupportedOps);
    assertQueryRequiresUnsupportedDatastoreFeature(baseQuery + "title LIKE '%foo'");
    // can't have 'or' on multiple properties
    assertQueryRequiresUnsupportedDatastoreFeature(baseQuery + "title = 'yar' or author = null");
    assertQueryRequiresUnsupportedDatastoreFeature(baseQuery + "isbn = 4 and (title = 'yar' or author = 'yam')");
    assertQueryRequiresUnsupportedDatastoreFeature(baseQuery + "title IN('yar') or author = 'yam'");
    // can only check equality
    assertQueryRequiresUnsupportedDatastoreFeature(baseQuery + "title > 5 or title < 2");

    // multiple inequality filters
    // TODO(maxr) Make this pass against the real datastore.
    // We need to have it return BadRequest instead of NeedIndex for that to happen.
    assertQueryUnsupportedByDatastore(baseQuery + "(title > 2 AND isbn < 4)", IllegalArgumentException.class);
    // inequality filter prop is not the same as the first order by prop
    assertQueryUnsupportedByDatastore(baseQuery + "(title > 2) order by isbn", IllegalArgumentException.class);
    // gets split into multiple inequality props
    assertQueryUnsupportedByDatastore(baseQuery + "title <> 2 AND isbn <> 4", IllegalArgumentException.class);
    assertEquals(
        new HashSet<Expression.Operator>(Arrays.asList(Expression.OP_CONCAT, Expression.OP_COM,
                                                       Expression.OP_NEG, Expression.OP_IS,
                                                       Expression.OP_LIKE,
                                                       Expression.OP_ISNOT)), unsupportedOps);
  }

  public void testEvaluateInMemory() {
    ds.put(null, newFlightEntity("1", "yar", "bam", 1, 2));
    ds.put(null, newFlightEntity("1", "yam", null, 1, 2));

    // This is impossible in the datastore, so run totally in-memory
    String query = "SELECT f FROM " + Flight.class.getName() + " f WHERE origin = 'yar' OR dest IS null";
    Query q = em.createQuery(query);
    q.setHint("datanucleus.query.evaluateInMemory", "true");
    try {
       List<Flight> results = (List<Flight>) q.getResultList();
       Assert.assertEquals("Number of results was wrong", 2, results.size());
    } catch (RuntimeException e) {
      fail("Threw exception when evaluating query in-memory, but should have run");
    }
  }

  public void testSupportedFilters_NoResultExpr() {
    String baseQuery = "SELECT FROM " + Book.class.getName() + " b ";
    testSupportedFilters(baseQuery);
  }

  public void testSupportedFilters_PrimaryResultExpr() {
    String baseQuery = "SELECT b FROM " + Book.class.getName() + " b ";
    testSupportedFilters(baseQuery);
  }

  private void testSupportedFilters(String baseQuery) {

    assertQuerySupported(baseQuery, NO_FILTERS, NO_SORTS);

    baseQuery += "WHERE ";
    assertQuerySupported(baseQuery + "title = 2", Utils.newArrayList(TITLE_EQ_2), NO_SORTS);
    assertQuerySupported(baseQuery + "title = \"2\"", Utils.newArrayList(TITLE_EQ_2STR), NO_SORTS);
    assertQuerySupported(baseQuery + "(title = 2)", Utils.newArrayList(TITLE_EQ_2), NO_SORTS);
    assertQuerySupported(baseQuery + "title = 2 AND isbn = 4",
                         Utils.newArrayList(TITLE_EQ_2,ISBN_EQ_4),
                         NO_SORTS);
    assertQuerySupported(baseQuery + "(title = 2 AND isbn = 4)",
                         Utils.newArrayList(TITLE_EQ_2, ISBN_EQ_4),
                         NO_SORTS);
    assertQuerySupported(baseQuery + "(title = 2) AND (isbn = 4)", Utils.newArrayList(
        TITLE_EQ_2, ISBN_EQ_4), NO_SORTS);
    assertQuerySupported(baseQuery + "title > 2", Utils.newArrayList(TITLE_GT_2), NO_SORTS);
    assertQuerySupported(baseQuery + "title >= 2", Utils.newArrayList(TITLE_GTE_2), NO_SORTS);
    assertQuerySupported(baseQuery + "isbn < 4", Utils.newArrayList(ISBN_LT_4), NO_SORTS);
    assertQuerySupported(baseQuery + "isbn <= 4", Utils.newArrayList(ISBN_LTE_4), NO_SORTS);

    baseQuery = "SELECT FROM " + Book.class.getName() + " b ";
    assertQuerySupported(baseQuery + "ORDER BY title ASC", NO_FILTERS,
                         Utils.newArrayList(TITLE_ASC));
    assertQuerySupported(baseQuery + "ORDER BY isbn DESC", NO_FILTERS,
                         Utils.newArrayList(ISBN_DESC));
    assertQuerySupported(baseQuery + "ORDER BY title ASC, isbn DESC", NO_FILTERS,
                         Utils.newArrayList(TITLE_ASC, ISBN_DESC));

    assertQuerySupported(baseQuery + "WHERE title = 2 AND isbn = 4 ORDER BY title ASC, isbn DESC",
                         Utils.newArrayList(TITLE_EQ_2, ISBN_EQ_4),
                         Utils.newArrayList(TITLE_ASC, ISBN_DESC));
    assertQuerySupported(baseQuery + "WHERE title <> null",
                         Utils.newArrayList(TITLE_NEQ_NULL_LITERAL), NO_SORTS);
    assertQuerySupported(baseQuery + "WHERE title <> 2",
                         Utils.newArrayList(TITLE_NEQ_2_LITERAL), NO_SORTS);
    assertQuerySupported(baseQuery + "WHERE title = '2' OR title = 2",
                         Utils.newArrayList(TITLE_IN_2_ARGS), NO_SORTS);
    assertQuerySupported(baseQuery + "WHERE title = '2' OR title = 2 OR title = false",
                         Utils.newArrayList(TITLE_IN_3_ARGS), NO_SORTS);
    assertQuerySupported(baseQuery + "WHERE title IN ('2', 2)",
                         Utils.newArrayList(TITLE_IN_2_ARGS), NO_SORTS);
    assertQuerySupported(baseQuery + "WHERE title IN ('2', 2, false)",
                         Utils.newArrayList(TITLE_IN_3_ARGS), NO_SORTS);
    assertQuerySupported(baseQuery + "WHERE (title = '2' OR title = 2) AND isbn = 4",
                         Utils.newArrayList(ISBN_EQ_4, TITLE_IN_2_ARGS), NO_SORTS);
    assertQuerySupported(baseQuery + "WHERE title IN ('2', 2) AND isbn = 4",
                         Utils.newArrayList(ISBN_EQ_4, TITLE_IN_2_ARGS), NO_SORTS);
    assertQuerySupported(baseQuery + "WHERE (title = '2' OR title = 2 OR title = false) AND isbn = 4",
                         Utils.newArrayList(ISBN_EQ_4, TITLE_IN_3_ARGS), NO_SORTS);
    assertQuerySupported(baseQuery + "WHERE title IN ('2', 2, false) AND isbn = 4",
                         Utils.newArrayList(ISBN_EQ_4, TITLE_IN_3_ARGS), NO_SORTS);
  }

  public void test2Equals2OrderBy() {
    ds.put(Book.newBookEntity("Joe Blow", "67890", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "11111", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "12345", "Foo Book"));
    ds.put(Book.newBookEntity("Joe Blow", "54321", "A Book"));
    ds.put(Book.newBookEntity("Jane Blow", "13579", "Baz Book"));

    Query q = em.createQuery("SELECT FROM " +
                             Book.class.getName() + " b" +
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

  public void testDefaultOrderingIsAsc() {
    ds.put(Book.newBookEntity("Joe Blow", "67890", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "11111", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "12345", "Foo Book"));
    ds.put(Book.newBookEntity("Joe Blow", "54321", "A Book"));
    ds.put(Book.newBookEntity("Jane Blow", "13579", "Baz Book"));

    Query q = em.createQuery("SELECT FROM " +
                             Book.class.getName() + " b" +
                             " WHERE author = 'Joe Blow'" +
                             " ORDER BY title");

    @SuppressWarnings("unchecked")
    List<Book> result = (List<Book>) q.getResultList();

    assertEquals(4, result.size());
    assertEquals("54321", result.get(0).getIsbn());
    assertEquals("67890", result.get(1).getIsbn());
    assertEquals("11111", result.get(2).getIsbn());
    assertEquals("12345", result.get(3).getIsbn());
  }

  public void testLimitQuery() {
    ds.put(Book.newBookEntity("Joe Blow", "67890", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "11111", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "12345", "Foo Book"));
    ds.put(Book.newBookEntity("Joe Blow", "54321", "A Book"));
    ds.put(Book.newBookEntity("Jane Blow", "13579", "Baz Book"));

    Query q = em.createQuery("SELECT FROM " +
                             Book.class.getName() + " b" + 
                             " WHERE author = 'Joe Blow'" +
                             " ORDER BY title DESC, isbn ASC");

    q.setMaxResults(1);
    @SuppressWarnings("unchecked")
    List<Book> result1 = (List<Book>) q.getResultList();
    assertEquals(1, result1.size());
    assertEquals("12345", result1.get(0).getIsbn());

    q.setMaxResults(0);
    @SuppressWarnings("unchecked")
    List<Book> result2 = (List<Book>) q.getResultList();
    assertEquals(0, result2.size());

    try {
      q.setMaxResults(-1);
      fail("expected iae");
    } catch (IllegalArgumentException iae) {
      // good
    }
  }

  public void testOffsetQuery() {
    ds.put(Book.newBookEntity("Joe Blow", "67890", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "11111", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "12345", "Foo Book"));
    ds.put(Book.newBookEntity("Joe Blow", "54321", "A Book"));
    ds.put(Book.newBookEntity("Jane Blow", "13579", "Baz Book"));
    Query q = em.createQuery("SELECT FROM " +
                             Book.class.getName() + " b" + 
                             " WHERE author = 'Joe Blow'" +
                             " ORDER BY title DESC, isbn ASC");

    q.setFirstResult(0);
    @SuppressWarnings("unchecked")
    List<Book> result1 = (List<Book>) q.getResultList();
    assertEquals(4, result1.size());
    assertEquals("12345", result1.get(0).getIsbn());

    q.setFirstResult(1);
    @SuppressWarnings("unchecked")
    List<Book> result2 = (List<Book>) q.getResultList();
    assertEquals(3, result2.size());
    assertEquals("11111", result2.get(0).getIsbn());

    try {
      q.setFirstResult(-1);
      fail("expected iae");
    } catch (IllegalArgumentException iae) {
      // good
    }
  }

  public void testOffsetLimitQuery() {
    ds.put(Book.newBookEntity("Joe Blow", "67890", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "11111", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "12345", "Foo Book"));
    ds.put(Book.newBookEntity("Joe Blow", "54321", "A Book"));
    ds.put(Book.newBookEntity("Jane Blow", "13579", "Baz Book"));
    Query q = em.createQuery("SELECT FROM " +
                             Book.class.getName() + " b" + 
                             " WHERE author = 'Joe Blow'" +
                             " ORDER BY title DESC, isbn ASC");

    q.setFirstResult(0);
    q.setMaxResults(0);
    @SuppressWarnings("unchecked")
    List<Book> result1 = (List<Book>) q.getResultList();
    assertEquals(0, result1.size());

    q.setFirstResult(1);
    q.setMaxResults(0);
    @SuppressWarnings("unchecked")
    List<Book> result2 = (List<Book>) q.getResultList();
    assertEquals(0, result2.size());

    q.setFirstResult(0);
    q.setMaxResults(1);
    @SuppressWarnings("unchecked")
    List<Book> result3 = (List<Book>) q.getResultList();
    assertEquals(1, result3.size());

    q.setFirstResult(0);
    q.setMaxResults(2);
    @SuppressWarnings("unchecked")
    List<Book> result4 = (List<Book>) q.getResultList();
    assertEquals(2, result4.size());
    assertEquals("12345", result4.get(0).getIsbn());

    q.setFirstResult(1);
    q.setMaxResults(1);
    @SuppressWarnings("unchecked")
    List<Book> result5 = (List<Book>) q.getResultList();
    assertEquals(1, result5.size());
    assertEquals("11111", result5.get(0).getIsbn());

    q.setFirstResult(2);
    q.setMaxResults(5);
    @SuppressWarnings("unchecked")
    List<Book> result6 = (List<Book>) q.getResultList();
    assertEquals(2, result6.size());
    assertEquals("67890", result6.get(0).getIsbn());
  }

  public void testSerialization() throws IOException {
    Query q = em.createQuery("select from " + Book.class.getName() + " b ");
    q.getResultList();

    JPQLQuery innerQuery = (JPQLQuery) ((JPAQuery) q).getInternalQuery();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    // the fact that this doesn't blow up is the test
    oos.writeObject(innerQuery);
  }

  public void testBindVariables() {

    assertQuerySupported("select from " + Book.class.getName() + " b where title = :titleParam",
                         Utils.newArrayList(TITLE_EQ_2), NO_SORTS, "titleParam", 2L);

    assertQuerySupported("select from " + Book.class.getName() + " b" + 
                         " where title = :titleParam AND isbn = :isbnParam",
                         Utils.newArrayList(TITLE_EQ_2, ISBN_EQ_4), NO_SORTS, "titleParam", 2L, "isbnParam", 4L);

    assertQuerySupported("select from " + Book.class.getName() + " b" + 
                         " where title = :titleParam AND isbn = :isbnParam order by title asc, isbn desc",
                         Utils.newArrayList(TITLE_EQ_2, ISBN_EQ_4),
                         Utils.newArrayList(TITLE_ASC, ISBN_DESC), "titleParam", 2L, "isbnParam", 4L);
  }

  public void testKeyQuery() {
    Entity bookEntity = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName() + " b where id = :key");
    q.setParameter("key", KeyFactory.keyToString(bookEntity.getKey()));
    @SuppressWarnings("unchecked")
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(bookEntity.getKey(), KeyFactory.stringToKey(books.get(0).getId()));

    // now issue the same query, but instead of providing a String version of
    // the key, provide the Key itself.
    q.setParameter("key", bookEntity.getKey());
    @SuppressWarnings("unchecked")
    List<Book> books2 = (List<Book>) q.getResultList();
    assertEquals(1, books2.size());
    assertEquals(bookEntity.getKey(), KeyFactory.stringToKey(books2.get(0).getId()));
  }

  public void testKeyQuery_KeyPk() {
    Entity entityWithName = new Entity(HasKeyPkJPA.class.getSimpleName(), "blarg");
    Entity entityWithId = new Entity(HasKeyPkJPA.class.getSimpleName());
    ds.put(entityWithName);
    ds.put(entityWithId);

    Query q = em.createQuery(
        "select from " + HasKeyPkJPA.class.getName() + " b where id = :key");
    q.setParameter("key", entityWithName.getKey());
    HasKeyPkJPA result = (HasKeyPkJPA) q.getSingleResult();
    assertEquals(entityWithName.getKey(), result.getId());

    q = em.createQuery("select from " + HasKeyPkJPA.class.getName() + " b where id = :mykey");
    q.setParameter("mykey", entityWithId.getKey());
    result = (HasKeyPkJPA) q.getSingleResult();
    assertEquals(entityWithId.getKey(), result.getId());

    q = em.createQuery("select from " + HasKeyPkJPA.class.getName() + " b where id = :mykeyname");
    q.setParameter("mykeyname", entityWithName.getKey().getName());
    result = (HasKeyPkJPA) q.getSingleResult();
    assertEquals(entityWithName.getKey(), result.getId());

    q = em.createQuery("select from " + HasKeyPkJPA.class.getName() + " b where id = :mykeyid");
    q.setParameter("mykeyid", entityWithId.getKey().getId());
    result = (HasKeyPkJPA) q.getSingleResult();
    assertEquals(entityWithId.getKey(), result.getId());
  }

  public void testKeyQueryWithSorts() {
    Entity bookEntity = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName() + " c where id = :key order by isbn ASC");
    q.setParameter("key", KeyFactory.keyToString(bookEntity.getKey()));
    @SuppressWarnings("unchecked")
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(bookEntity.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
  }

  public void testKeyQuery_MultipleFilters() {
    Entity bookEntity = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName() + " c where id = :key and isbn = \"67890\"");
    q.setParameter("key", KeyFactory.keyToString(bookEntity.getKey()));
    @SuppressWarnings("unchecked")
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(bookEntity.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
  }

  public void testKeyQuery_NonEqualityFilter() {
    Entity bookEntity1 = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity1);

    Entity bookEntity2 = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity2);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName()
        + " b where id > :key");
    q.setParameter("key", KeyFactory.keyToString(bookEntity1.getKey()));
    @SuppressWarnings("unchecked")
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(bookEntity2.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
  }

  public void testKeyQuery_SortByKey() {
    Entity bookEntity1 = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity1);

    Entity bookEntity2 = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity2);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName()
        + " b order by id DESC");
    @SuppressWarnings("unchecked")
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(2, books.size());
    assertEquals(bookEntity2.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
  }

  public void testKeyQuery_FilterAndSortByKeyComponent() {
    // filter by pk-id
    assertQueryUnsupportedByDatastore(
        "select from " + HasEncodedStringPkSeparateIdFieldJPA.class.getName() + " b where id = 4",
        NucleusFatalUserException.class);
    // sort by pk-id
    assertQueryUnsupportedByDatastore(
        "select from " + HasEncodedStringPkSeparateIdFieldJPA.class.getName() + " b order by id",
        NucleusFatalUserException.class);
    // filter by pk-id
    assertQueryUnsupportedByDatastore(
        "select from " + HasEncodedStringPkSeparateNameFieldJPA.class.getName() + " b where name = 4",
        NucleusFatalUserException.class);
    // sort by pk-id
    assertQueryUnsupportedByDatastore(
        "select from " + HasEncodedStringPkSeparateNameFieldJPA.class.getName() + " b order by name",
        NucleusFatalUserException.class);
  }

  public void testAncestorQuery() {
    Entity bookEntity = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity);
    Entity
        hasAncestorEntity =
        new Entity(HasStringAncestorStringPkJPA.class.getSimpleName(), bookEntity.getKey());
    ds.put(hasAncestorEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + HasStringAncestorStringPkJPA.class.getName()
        + " b where ancestorId = :ancId");
    q.setParameter("ancId", KeyFactory.keyToString(bookEntity.getKey()));

    @SuppressWarnings("unchecked")
    List<HasStringAncestorStringPkJPA>
        haList =
        (List<HasStringAncestorStringPkJPA>) q.getResultList();
    assertEquals(1, haList.size());
    assertEquals(bookEntity.getKey(), KeyFactory.stringToKey(haList.get(0).getAncestorId()));

    assertEquals(
        bookEntity.getKey(), getDatastoreQuery(q).getLatestDatastoreQuery().getAncestor());
    assertEquals(NO_FILTERS, getFilterPredicates(q));
    assertEquals(NO_SORTS, getSortPredicates(q));
  }

  public void testIllegalAncestorQuery() {
    Entity bookEntity = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity);
    Entity
        hasAncestorEntity =
        new Entity(HasStringAncestorStringPkJPA.class.getName(), bookEntity.getKey());
    ds.put(hasAncestorEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + HasStringAncestorStringPkJPA.class.getName() + " b"
        + " where ancestorId > :ancId");
    q.setParameter("ancId", KeyFactory.keyToString(bookEntity.getKey()));
    try {
      q.getResultList();
      fail("expected udfe");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // good
        }
        else {
          throw pe;
        }
    }
  }

  public void testSortByFieldWithCustomColumn() {
    ds.put(Book.newBookEntity("Joe Blow", "67890", "Bar Book", 2003));
    ds.put(Book.newBookEntity("Joe Blow", "11111", "Bar Book", 2002));
    ds.put(Book.newBookEntity("Joe Blow", "12345", "Foo Book", 2001));

    Query q = em.createQuery("SELECT FROM " +
                             Book.class.getName() + " b" +
                             " WHERE b.author = 'Joe Blow'" +
                             " ORDER BY firstPublished ASC");

    @SuppressWarnings("unchecked")
    List<Book> result = (List<Book>) q.getResultList();

    assertEquals(3, result.size());
    assertEquals("12345", result.get(0).getIsbn());
    assertEquals("11111", result.get(1).getIsbn());
    assertEquals("67890", result.get(2).getIsbn());
  }

  private interface BookProvider {

    Book getBook(Key key);
  }

  private class AttachedBookProvider implements BookProvider {

    public Book getBook(Key key) {
      return em.find(Book.class, key);
    }
  }

  private class TransientBookProvider implements BookProvider {

    public Book getBook(Key key) {
      Book b = new Book();
      b.setId(KeyFactory.keyToString(key));
      return b;
    }
  }

  private void testFilterByChildObject(BookProvider bp) {
    Entity parentEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity bookEntity = Book.newBookEntity(parentEntity.getKey(), "Joe Blow", "11111", "Bar Book", 1929);
    ds.put(bookEntity);

    Book book = bp.getBook(bookEntity.getKey());
    Query q = em.createQuery(
        "select from " + HasOneToOneJPA.class.getName() + " c where book = :b");
    q.setParameter("b", book);
    List<HasOneToOneJPA> result = (List<HasOneToOneJPA>) q.getResultList();
    assertEquals(1, result.size());
    assertEquals(parentEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
  }

  public void testFilterByChildObject() {
    testFilterByChildObject(new AttachedBookProvider());
    testFilterByChildObject(new TransientBookProvider());
  }

  private void testFilterByChildObject_AdditionalFilterOnParent(BookProvider bp) {
    Entity parentEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity bookEntity = Book.newBookEntity(parentEntity.getKey(), "Joe Blow", "11111", "Bar Book", 1929);
    ds.put(bookEntity);

    Book book = bp.getBook(bookEntity.getKey());
    Query q = em.createQuery(
        "select from " + HasOneToOneJPA.class.getName() + " c where id = :parentId and book = :b");
    q.setParameter("parentId", KeyFactory.keyToString(bookEntity.getKey()));
    q.setParameter("b", book);
    List<HasOneToOneJPA> result = (List<HasOneToOneJPA>) q.getResultList();
    assertTrue(result.isEmpty());

    q.setParameter("parentId", KeyFactory.keyToString(parentEntity.getKey()));
    q.setParameter("b", book);
    result = (List<HasOneToOneJPA>) q.getResultList();
    assertEquals(1, result.size());
    assertEquals(parentEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
  }

  public void testFilterByChildObject_AdditionalFilterOnParent() {
    testFilterByChildObject_AdditionalFilterOnParent(new AttachedBookProvider());
    testFilterByChildObject_AdditionalFilterOnParent(new TransientBookProvider());
  }


  private void testFilterByChildObject_UnsupportedOperator(BookProvider bp) {
    Entity parentEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity bookEntity = Book.newBookEntity(parentEntity.getKey(), "Joe Blow", "11111", "Bar Book", 1929);
    ds.put(bookEntity);

    Book book = bp.getBook(bookEntity.getKey());
    Query q = em.createQuery(
        "select from " + HasOneToOneJPA.class.getName() + " c where book > :b");
    q.setParameter("b", book);
    try {
      q.getResultList();
      fail("expected udfe");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // good
        }
        else {
          throw pe;
        }
    }
  }

  public void testFilterByChildObject_UnsupportedOperator() {
    testFilterByChildObject_UnsupportedOperator(new AttachedBookProvider());
    testFilterByChildObject_UnsupportedOperator(new TransientBookProvider());
  }

  private void testFilterByChildObject_ValueWithoutAncestor(BookProvider bp) {
    Entity parentEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity bookEntity = Book.newBookEntity("Joe Blow", "11111", "Bar Book", 1929);
    ds.put(bookEntity);

    Book book = bp.getBook(bookEntity.getKey());
    Query q = em.createQuery(
        "select from " + HasOneToOneJPA.class.getName() + " c where book = :b");
    q.setParameter("b", book);
    try {
      q.getResultList();
      fail("expected JPAException");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testFilterByChildObject_ValueWithoutAncestor() {
    testFilterByChildObject_ValueWithoutAncestor(new AttachedBookProvider());
    testFilterByChildObject_ValueWithoutAncestor(new TransientBookProvider());
  }

  public void testFilterByChildObject_KeyIsWrongType() {
    Entity parentEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    ds.put(parentEntity);

    Query q = em.createQuery(
        "select from " + HasOneToOneJPA.class.getName() + " c where book = :b");
    q.setParameter("b", parentEntity.getKey());
    try {
      q.getResultList();
      fail("expected JPAException");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testFilterByChildObject_KeyParentIsWrongType() {
    Key parent = KeyFactory.createKey("yar", 44);
    Entity bookEntity = new Entity(Book.class.getSimpleName(), parent);

    Query q = em.createQuery(
        "select from " + HasOneToOneJPA.class.getName() + " c where book = :b");
    q.setParameter("b", bookEntity.getKey());

    assertTrue(q.getResultList().isEmpty());
  }

  public void testFilterByChildObject_ValueWithoutId() {
    Entity parentEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity bookEntity = Book.newBookEntity("Joe Blow", "11111", "Bar Book", 1929);
    ds.put(bookEntity);

    Book book = new Book();
    Query q = em.createQuery(
        "select from " + HasOneToOneJPA.class.getName() + " c where book = :b");
    q.setParameter("b", book);
    try {
      q.getResultList();
      fail("expected JPAException");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testFilterByParentObject() {
    Entity parentEntity = new Entity(HasOneToManyListJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity bidirEntity =
        new Entity(BidirectionalChildListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(bidirEntity);
    Entity bidirEntity2 =
        new Entity(BidirectionalChildListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(bidirEntity2);

    HasOneToManyListJPA parent =
        em.find(HasOneToManyListJPA.class, KeyFactory.keyToString(parentEntity.getKey()));
    Query q = em.createQuery("SELECT FROM " +
                             BidirectionalChildListJPA.class.getName() +
                             " c WHERE parent = :p");

    q.setParameter("p", parent);
    @SuppressWarnings("unchecked")
    List<BidirectionalChildListJPA> result = (List<BidirectionalChildListJPA>) q.getResultList();
    assertEquals(2, result.size());
    assertEquals(bidirEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
    assertEquals(bidirEntity2.getKey(), KeyFactory.stringToKey(result.get(1).getId()));
  }

  public void testFilterByParentLongObjectId() {
    Entity parentEntity = new Entity(HasOneToManyLongPkListJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity bidirEntity =
        new Entity(BidirectionalChildLongPkListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(bidirEntity);
    Entity bidirEntity2 =
        new Entity(BidirectionalChildLongPkListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(bidirEntity2);

    HasOneToManyLongPkListJPA parent =
        em.find(HasOneToManyLongPkListJPA.class, KeyFactory.keyToString(parentEntity.getKey()));
    Query q = em.createQuery("SELECT FROM " +
                             BidirectionalChildLongPkListJPA.class.getName() + " c WHERE parent = :p");

    q.setParameter("p", parent.getId());
    @SuppressWarnings("unchecked")
    List<BidirectionalChildLongPkListJPA> result = (List<BidirectionalChildLongPkListJPA>) q.getResultList();
    assertEquals(2, result.size());
    assertEquals(bidirEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
    assertEquals(bidirEntity2.getKey(), KeyFactory.stringToKey(result.get(1).getId()));
  }

  public void testFilterByParentIntObjectId() {
    Entity parentEntity = new Entity(HasOneToManyLongPkListJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity bidirEntity =
        new Entity(BidirectionalChildLongPkListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(bidirEntity);
    Entity bidirEntity2 =
        new Entity(BidirectionalChildLongPkListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(bidirEntity2);

    HasOneToManyLongPkListJPA parent =
        em.find(HasOneToManyLongPkListJPA.class, KeyFactory.keyToString(parentEntity.getKey()));
    Query q = em.createQuery("SELECT FROM " +
                             BidirectionalChildLongPkListJPA.class.getName() + " c WHERE parent = :p");

    q.setParameter("p", parent.getId().intValue());
    @SuppressWarnings("unchecked")
    List<BidirectionalChildLongPkListJPA> result = (List<BidirectionalChildLongPkListJPA>) q.getResultList();
    assertEquals(2, result.size());
    assertEquals(bidirEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
    assertEquals(bidirEntity2.getKey(), KeyFactory.stringToKey(result.get(1).getId()));
  }

  public void testFilterByParentObjectWhereParentIsAChild() {
    Entity parentEntity = new Entity(HasOneToManyListJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity childEntity = new Entity(BidirectionalChildListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(childEntity);
    Entity grandChildEntity1 =
        new Entity(BidirectionalGrandchildListJPA.class.getSimpleName(), childEntity.getKey());
    ds.put(grandChildEntity1);
    Entity grandChildEntity2 =
        new Entity(BidirectionalGrandchildListJPA.class.getSimpleName(), childEntity.getKey());
    ds.put(grandChildEntity2);

    BidirectionalChildListJPA child =
        em.find(BidirectionalChildListJPA.class, KeyFactory.keyToString(childEntity.getKey()));
    Query q = em.createQuery(
        "select from " + BidirectionalGrandchildListJPA.class.getName() + " c where parent = :p");
    q.setParameter("p", child);
    @SuppressWarnings("unchecked")
    List<BidirectionalGrandchildListJPA> result = (List<BidirectionalGrandchildListJPA>) q.getResultList();
    assertEquals(2, result.size());
    assertEquals(grandChildEntity1.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
    assertEquals(grandChildEntity2.getKey(), KeyFactory.stringToKey(result.get(1).getId()));
  }
  public void testFilterByParentId() {
    Entity parentEntity = new Entity(HasOneToManyListJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity
        bidirEntity =
        new Entity(BidirectionalChildListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(bidirEntity);
    Entity
        bidirEntity2 =
        new Entity(BidirectionalChildListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(bidirEntity2);

    HasOneToManyListJPA parent =
        em.find(HasOneToManyListJPA.class, KeyFactory.keyToString(parentEntity.getKey()));
    Query q = em.createQuery("SELECT FROM " +
                             BidirectionalChildListJPA.class.getName() +
                             " c WHERE parent = :p");

    q.setParameter("p", parent.getId());
    @SuppressWarnings("unchecked")
    List<BidirectionalChildListJPA> result = (List<BidirectionalChildListJPA>) q.getResultList();
    assertEquals(2, result.size());
    assertEquals(bidirEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
    assertEquals(bidirEntity2.getKey(), KeyFactory.stringToKey(result.get(1).getId()));
  }

  public void testFilterByParentKey() {
    Entity parentEntity = new Entity(HasOneToManyListJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity
        bidirEntity =
        new Entity(BidirectionalChildListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(bidirEntity);
    Entity
        bidirEntity2 =
        new Entity(BidirectionalChildListJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(bidirEntity2);

    Query q = em.createQuery("SELECT FROM " +
                             BidirectionalChildListJPA.class.getName() +
                             " c WHERE parent = :p");

    q.setParameter("p", parentEntity.getKey());
    @SuppressWarnings("unchecked")
    List<BidirectionalChildListJPA> result = (List<BidirectionalChildListJPA>) q.getResultList();
    assertEquals(2, result.size());
    assertEquals(bidirEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
    assertEquals(bidirEntity2.getKey(), KeyFactory.stringToKey(result.get(1).getId()));
  }

  public void testFilterByMultiValueProperty() {
    Entity entity = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    entity.setProperty("strList", Utils.newArrayList("1", "2", "3"));
    entity.setProperty("keyList",
                       Utils.newArrayList(KeyFactory.createKey("be", "bo"),
                                          KeyFactory.createKey("bo", "be")));
    ds.put(entity);

    Query q = em.createQuery(
        "select from " + HasMultiValuePropsJPA.class.getName()
        + " c where strList = :p1 AND strList = :p2");
    q.setParameter("p1", "1");
    q.setParameter("p2", "3");
    @SuppressWarnings("unchecked")
    List<HasMultiValuePropsJPA> result = (List<HasMultiValuePropsJPA>) q.getResultList();
    assertEquals(1, result.size());
    q.setParameter("p1", "1");
    q.setParameter("p2", "4");
    @SuppressWarnings("unchecked")
    List<HasMultiValuePropsJPA> result2 = (List<HasMultiValuePropsJPA>) q.getResultList();
    assertEquals(0, result2.size());

    q = em.createQuery(
        "select from " + HasMultiValuePropsJPA.class.getName() + " c where keyList = :p1 AND keyList = :p2");
    q.setParameter("p1", KeyFactory.createKey("be", "bo"));
    q.setParameter("p2", KeyFactory.createKey("bo", "be"));
    assertEquals(1, result.size());
    q.setParameter("p1", KeyFactory.createKey("be", "bo"));
    q.setParameter("p2", KeyFactory.createKey("bo", "be2"));
    @SuppressWarnings("unchecked")
    List<HasMultiValuePropsJPA> result3 = (List<HasMultiValuePropsJPA>) q.getResultList();
    assertEquals(0, result3.size());
  }

  public void testNoPutsAfterLoadingMultiValueProperty() throws NoSuchMethodException {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    testFilterByMultiValueProperty();
    em.close();
  }

  public void testFilterByMultiValueProperty_MemberOf_ArgsWrongOrder() {
    Entity entity = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    entity.setProperty("strList", Utils.newArrayList("1", "2", "3"));
    entity.setProperty("keyList",
                       Utils.newArrayList(KeyFactory.createKey("be", "bo"),
                                          KeyFactory.createKey("bo", "be")));
    ds.put(entity);

    Query q = em.createQuery(
        "select from " + HasMultiValuePropsJPA.class.getName()
        + " c where strList MEMBER OF :p1 AND strList MEMBER OF :p2");
    q.setParameter("p1", "1");
    q.setParameter("p2", "3");
    @SuppressWarnings("unchecked")
    List<HasMultiValuePropsJPA> result = (List<HasMultiValuePropsJPA>) q.getResultList();
    assertEquals(1, result.size());
    q.setParameter("p1", "1");
    q.setParameter("p2", "4");
    @SuppressWarnings("unchecked")
    List<HasMultiValuePropsJPA> result2 = (List<HasMultiValuePropsJPA>) q.getResultList();
    assertEquals(0, result2.size());

    q = em.createQuery(
        "select from " + HasMultiValuePropsJPA.class.getName()
        + " c where keyList MEMBER OF :p1 AND keyList MEMBER OF :p2");
    q.setParameter("p1", KeyFactory.createKey("be", "bo"));
    q.setParameter("p2", KeyFactory.createKey("bo", "be"));
    result = q.getResultList();
    assertEquals(1, result.size());
    q.setParameter("p1", KeyFactory.createKey("be", "bo"));
    q.setParameter("p2", KeyFactory.createKey("bo", "be2"));
    @SuppressWarnings("unchecked")
    List<HasMultiValuePropsJPA> result3 = (List<HasMultiValuePropsJPA>) q.getResultList();
    assertEquals(0, result3.size());
  }

  public void testFilterByMultiValueProperty_MemberOf_ArgsCorrectOrder() {
    Entity entity = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    entity.setProperty("strList", Utils.newArrayList("1", "2", "3"));
    entity.setProperty("keyList",
                       Utils.newArrayList(KeyFactory.createKey("be", "bo"),
                                          KeyFactory.createKey("bo", "be")));
    ds.put(entity);

    Query q = em.createQuery(
        "select from " + HasMultiValuePropsJPA.class.getName()
        + " c where :p1 MEMBER OF strList AND :p2 MEMBER OF strList");
    q.setParameter("p1", "1");
    q.setParameter("p2", "3");
    @SuppressWarnings("unchecked")
    List<HasMultiValuePropsJPA> result = (List<HasMultiValuePropsJPA>) q.getResultList();
    assertEquals(1, result.size());
    q.setParameter("p1", "1");
    q.setParameter("p2", "4");
    @SuppressWarnings("unchecked")
    List<HasMultiValuePropsJPA> result2 = (List<HasMultiValuePropsJPA>) q.getResultList();
    assertEquals(0, result2.size());

    q = em.createQuery(
        "select from " + HasMultiValuePropsJPA.class.getName()
        + " c where :p1 MEMBER OF keyList AND :p2 MEMBER OF keyList");
    q.setParameter("p1", KeyFactory.createKey("be", "bo"));
    q.setParameter("p2", KeyFactory.createKey("bo", "be"));
    result = q.getResultList();
    assertEquals(1, result.size());
    q.setParameter("p1", KeyFactory.createKey("be", "bo"));
    q.setParameter("p2", KeyFactory.createKey("bo", "be2"));
    @SuppressWarnings("unchecked")
    List<HasMultiValuePropsJPA> result3 = (List<HasMultiValuePropsJPA>) q.getResultList();
    assertEquals(0, result3.size());

    // try one with a literal value
    q = em.createQuery(
        "select from " + HasMultiValuePropsJPA.class.getName()
        + " c where '1' MEMBER OF strList");
    result = q.getResultList();
    assertEquals(1, result.size());
  }

  public void testFilterByEmbeddedField() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ds.put(entity);

    Query q = em.createQuery(
        "select from " + Person.class.getName() + " c where name.first = \"max\"");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testFilterByEmbeddedField_OverriddenColumn() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ds.put(entity);

    Query q = em.createQuery(
        "select from " + Person.class.getName()
        + " c where anotherName.last = \"notross\"");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testFilterByEmbeddedField_MultipleFields() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "max");
    entity.setProperty("anotherLast", "notross");
    ds.put(entity);

    Query q = em.createQuery(
        "select from " + Person.class.getName()
        + " c where name.first = \"max\" AND anotherName.last = \"notross\"");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testFilterBySubObject_UnknownField() {
    try {
      em.createQuery(
          "select from " + Flight.class.getName() + " c where origin.first = \"max\"")
          .getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testFilterBySubObject_NotEmbeddable() {
    try {
      em.createQuery(
          "select from " + HasOneToOneJPA.class.getName() + " c where flight.origin = \"max\"")
          .getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testSortByEmbeddedField() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ds.put(entity);

    entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max2");
    entity.setProperty("last", "ross2");
    entity.setProperty("anotherFirst", "notmax2");
    entity.setProperty("anotherLast", "notross2");
    ds.put(entity);

    Query q = em.createQuery("select from " + Person.class.getName() + " c order by name.first desc");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.getResultList();
    assertEquals(2, result.size());
    assertEquals("max2", result.get(0).getName().getFirst());
    assertEquals("max", result.get(1).getName().getFirst());
  }

  public void testSortByEmbeddedField_OverriddenColumn() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ds.put(entity);

    entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max2");
    entity.setProperty("last", "ross2");
    entity.setProperty("anotherFirst", "notmax2");
    entity.setProperty("anotherLast", "notross2");
    ds.put(entity);

    Query q =
        em.createQuery("select from " + Person.class.getName() + " c order by anotherName.last desc");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.getResultList();
    assertEquals(2, result.size());
    assertEquals("notross2", result.get(0).getAnotherName().getLast());
    assertEquals("notross", result.get(1).getAnotherName().getLast());
  }

  public void testSortByEmbeddedField_MultipleFields() {
    Entity entity0 = new Entity(Person.class.getSimpleName());
    entity0.setProperty("first", "max");
    entity0.setProperty("last", "ross");
    entity0.setProperty("anotherFirst", "notmax");
    entity0.setProperty("anotherLast", "z");
    ds.put(entity0);

    Entity entity1 = new Entity(Person.class.getSimpleName());
    entity1.setProperty("first", "max");
    entity1.setProperty("last", "ross2");
    entity1.setProperty("anotherFirst", "notmax2");
    entity1.setProperty("anotherLast", "notross2");
    ds.put(entity1);

    Entity entity2 = new Entity(Person.class.getSimpleName());
    entity2.setProperty("first", "a");
    entity2.setProperty("last", "b");
    entity2.setProperty("anotherFirst", "c");
    entity2.setProperty("anotherLast", "d");
    ds.put(entity2);

    Query q = em.createQuery(
        "select from " + Person.class.getName()
        + " c order by name.first asc, anotherName.last desc");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.getResultList();
    assertEquals(3, result.size());
    assertEquals(Long.valueOf(entity2.getKey().getId()), result.get(0).getId());
    assertEquals(Long.valueOf(entity0.getKey().getId()), result.get(1).getId());
    assertEquals(Long.valueOf(entity1.getKey().getId()), result.get(2).getId());
  }

  public void testSortBySubObject_UnknownField() {
    try {
      em.createQuery(
          "select from " + Book.class.getName() + " c order by author.first").getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testSortBySubObject_NotEmbeddable() {
    try {
      em.createQuery(
          "select from " + HasOneToOneJPA.class.getName() + " c order by book.author")
          .getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testBigDecimalQuery() {
    Entity e = KitchenSink.newKitchenSinkEntity("blarg", null);
    ds.put(e);

    Query q = em.createQuery(
        "select from " + KitchenSink.class.getName() + " c where bigDecimal = :bd");
    q.setParameter("bd", new BigDecimal(2.444d));
    @SuppressWarnings("unchecked")
    List<KitchenSink> results = (List<KitchenSink>) q.getResultList();
    assertEquals(1, results.size());
  }

  public void testQueryWithNegativeLiteralLong() {
    ds.put(Book.newBookEntity("auth", "123432", "title", -40));

    Query q = em.createQuery(
        "select from " + Book.class.getName() + " c where firstPublished = -40");
    @SuppressWarnings("unchecked")
    List<Book> results = (List<Book>) q.getResultList();
    assertEquals(1, results.size());
    q = em.createQuery(
        "select from " + Book.class.getName() + " c where firstPublished > -41");
    @SuppressWarnings("unchecked")
    List<Book> results2 = (List<Book>) q.getResultList();
    assertEquals(1, results2.size());
  }

  public void testQueryWithNegativeLiteralDouble() {
    Entity e = new Entity(HasDoubleJPA.class.getSimpleName());
    e.setProperty("aDouble", -2.23d);
    ds.put(e);

    Query q = em.createQuery(
        "select from " + HasDoubleJPA.class.getName() + " c where aDouble > -2.25");
    @SuppressWarnings("unchecked")
    List<KitchenSink> results = (List<KitchenSink>) q.getResultList();
    assertEquals(1, results.size());
  }

  public void testQueryWithNegativeParam() {
    ds.put(Book.newBookEntity("auth", "123432", "title", -40));

    Query q = em.createQuery(
        "select from " + Book.class.getName() + " c where firstPublished = :p");
    q.setParameter("p", -40);
    @SuppressWarnings("unchecked")
    List<Book> results = (List<Book>) q.getResultList();
    assertEquals(1, results.size());
  }

  public void testKeyQueryWithUnencodedStringPk() {
    Entity e = new Entity(HasUnencodedStringPkJPA.class.getSimpleName(), "yar");
    ds.put(e);
    Query q = em.createQuery(
        "select from " + HasUnencodedStringPkJPA.class.getName() + " c where id = :p");
    q.setParameter("p", e.getKey().getName());
    @SuppressWarnings("unchecked")
    List<HasUnencodedStringPkJPA> results =
        (List<HasUnencodedStringPkJPA>) q.getResultList();
    assertEquals(1, results.size());
    assertEquals(e.getKey().getName(), results.get(0).getId());

    q = em.createQuery(
        "select from " + HasUnencodedStringPkJPA.class.getName() + " c where id = :p");
    q.setParameter("p", e.getKey());
    @SuppressWarnings("unchecked")
    List<HasUnencodedStringPkJPA> results2 =
        (List<HasUnencodedStringPkJPA>) q.getResultList();
    assertEquals(1, results2.size());
    assertEquals(e.getKey().getName(), results2.get(0).getId());
  }

  public void testKeyQueryWithLongPk() {
    Entity e = new Entity(HasLongPkJPA.class.getSimpleName());
    ds.put(e);
    Query q = em.createQuery(
        "select from " + HasLongPkJPA.class.getName() + " c where id = :p");
    q.setParameter("p", e.getKey().getId());
    @SuppressWarnings("unchecked")
    List<HasLongPkJPA> results = (List<HasLongPkJPA>) q.getResultList();
    assertEquals(1, results.size());
    assertEquals(Long.valueOf(e.getKey().getId()), results.get(0).getId());

    q = em.createQuery(
        "select from " + HasLongPkJPA.class.getName() + " c where id = :p");
    q.setParameter("p", e.getKey().getId());
    @SuppressWarnings("unchecked")
    List<HasLongPkJPA> results2 = (List<HasLongPkJPA>) q.getResultList();
    assertEquals(1, results2.size());
    assertEquals(Long.valueOf(e.getKey().getId()), results2.get(0).getId());
  }

  public void testKeyQueryWithEncodedStringPk() {
    Entity e = new Entity(HasEncodedStringPkJPA.class.getSimpleName(), "yar");
    ds.put(e);
    Query q = em.createQuery("select from " + HasEncodedStringPkJPA.class.getName() + " c where id = :p");
    q.setParameter("p", e.getKey().getName());
    HasEncodedStringPkJPA result = (HasEncodedStringPkJPA) q.getSingleResult();
    assertEquals(KeyFactory.keyToString(e.getKey()), result.getId());

    q = em.createQuery("select from " + HasEncodedStringPkJPA.class.getName() + " c where id = :p");
    q.setParameter("p", e.getKey());
    result = (HasEncodedStringPkJPA) q.getSingleResult();
    assertEquals(KeyFactory.keyToString(e.getKey()), result.getId());

    q = em.createQuery("select from " + HasEncodedStringPkJPA.class.getName() + " c where id = :p");
    q.setParameter("p", e.getKey().getName());
    result = (HasEncodedStringPkJPA) q.getSingleResult();
    assertEquals(KeyFactory.keyToString(e.getKey()), result.getId());
  }

  public void testQuerySingleResult_OneResult() {
    Entity e = Book.newBookEntity("max", "12345", "t1");
    ds.put(e);
    Query q = em.createQuery(
        "select from " + Book.class.getName() + " c where title = :p");
    q.setParameter("p", "t1");
    Book pojo = (Book) q.getSingleResult();
    assertEquals(e.getKey(), KeyFactory.stringToKey(pojo.getId()));
  }

  public void testQuerySingleResult_NoResult() {
    Entity e = Book.newBookEntity("max", "12345", "t1");
    ds.put(e);
    Query q = em.createQuery(
        "select from " + Book.class.getName() + " c where title = :p");
    q.setParameter("p", "not t1");
    try {
      q.getSingleResult();
      fail("expected exception");
    } catch (NoResultException ex) {
      // good
    }
  }

  public void testQuerySingleResult_MultipleResults() {
    Entity e1 = Book.newBookEntity("max", "12345", "t1");
    Entity e2 = Book.newBookEntity("max", "12345", "t1");
    ds.put(e1);
    ds.put(e2);
    Query q = em.createQuery(
        "select from " + Book.class.getName() + " c where title = :p");
    q.setParameter("p", "t1");
    try {
      q.getSingleResult();
      fail("expected exception");
    } catch (NonUniqueResultException ex) {
      // good
    }
  }


  public void testSortByUnknownProperty() {
    try {
      em.createQuery("select from " + Book.class.getName() + " c order by dne").getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testDatastoreFailureWhileIterating() {
    // Need to have enough data to ensure a Next call
    for (int i = 0; i < 21; i++) {
      Entity bookEntity = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
      ds.put(bookEntity);
    }
    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          boolean exploded = false;
          protected void doIntercept(String methodName) {
            if (!exploded && methodName.equals("Next")) {
              exploded = true;
              throw new DatastoreFailureException("boom: " + methodName);
            }
          }
        };

    ApiProxy.Delegate original = getDelegateForThread();
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(getDelegateForThread(), policy);
    setDelegateForThread(dd);

    try {
      javax.persistence.Query q = em.createQuery(
          "select from " + Book.class.getName() + " b");
      try {
        @SuppressWarnings("unchecked")
        List<Book> books = (List<Book>) q.getResultList();
        books.size();
        fail("expected exception");
      } catch (PersistenceException e) {
        // good
        assertTrue(e.getCause() instanceof DatastoreFailureException);
      }
    } finally {
      setDelegateForThread(original);
    }
  }

  public void testBadRequest() {
    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;

          protected void doIntercept(String methodName) {
            count++;
            if (count == 1) {
              throw new IllegalArgumentException("boom");
            }
          }
        };
    ApiProxy.Delegate original = getDelegateForThread();

    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(getDelegateForThread(), policy);
    setDelegateForThread(dd);

    try {
      Query q = em.createQuery("select from " + Book.class.getName() + " b");
      try {
        q.getResultList();
        fail("expected exception");
      } catch (PersistenceException e) {
        // good
        assertTrue(e.getCause() instanceof IllegalArgumentException);
      }
    } finally {
      setDelegateForThread(original);
    }
  }

  public void testCountQuery() {
    Entity e1 = Book.newBookEntity("jimmy", "12345", "the title", 2003);
    Entity e2 = Book.newBookEntity("jimmy", "12345", "the title", 2004);
    ds.put(e1);
    ds.put(e2);
    Query q = em.createQuery("select count(id) from " + Book.class.getName() + " c");
    assertEquals(2, q.getSingleResult());

    q = em.createQuery("select COUNT(id) from " + Book.class.getName() + " c");
    assertEquals(2, q.getSingleResult());

    q = em.createQuery("select Count(id) from " + Book.class.getName() + " c");
    assertEquals(2, q.getSingleResult());
  }

  public void testCountQueryWithFilter() {
    Entity e1 = Book.newBookEntity("jimmy", "12345", "the title", 2003);
    Entity e2 = Book.newBookEntity("jimmy", "12345", "the title", 2004);
    ds.put(e1);
    ds.put(e2);
    Query q = em.createQuery(
            "select count(id) from " + Book.class.getName() + " c" + " where firstPublished = 2003");
    assertEquals(1, q.getSingleResult());
  }

  public void testCountQueryWithUnknownCountProp() {
    Entity e1 = Book.newBookEntity("jimmy", "12345", "the title", 2003);
    Entity e2 = Book.newBookEntity("jimmy", "12345", "the title", 2004);
    ds.put(e1);
    ds.put(e2);
    Query q = em.createQuery("select count(doesnotexist) from " + Book.class.getName() + " c");
    try {
      q.getSingleResult();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testCountQueryWithOffset() {
    Entity e1 = Book.newBookEntity("jimmy", "12345", "the title", 2003);
    Entity e2 = Book.newBookEntity("jimmy", "12345", "the title", 2004);
    ds.put(e1);
    ds.put(e2);
    Query q = em.createQuery("select count(id) from " + Book.class.getName() + " c");
    q.setFirstResult(1);
    assertEquals(1, q.getSingleResult());
  }

  public void testCountQueryWithLimit() {
    Entity e1 = Book.newBookEntity("jimmy", "12345", "the title", 2003);
    Entity e2 = Book.newBookEntity("jimmy", "12345", "the title", 2004);
    ds.put(e1);
    ds.put(e2);
    Query q = em.createQuery("select count(id) from " + Book.class.getName() + " c");
    q.setMaxResults(1);
    assertEquals(1, q.getSingleResult());
  }

  public void testCountQueryWithOffsetAndLimit() {
    Entity e1 = Book.newBookEntity("jimmy", "12345", "the title", 2003);
    Entity e2 = Book.newBookEntity("jimmy", "12345", "the title", 2004);
    Entity e3 = Book.newBookEntity("jimmy", "12345", "the title", 2005);
    ds.put(e1);
    ds.put(e2);
    ds.put(e3);
    Query q = em.createQuery("select count(id) from " + Book.class.getName() + " c");
    q.setFirstResult(1);
    q.setMaxResults(1);
    assertEquals(1, q.getSingleResult());
  }

  public void testFilterByEnum_ProvideStringExplicitly() {
    Entity e = new Entity(HasEnumJPA.class.getSimpleName());
    e.setProperty("myEnum", HasEnumJPA.MyEnum.V1.name());
    ds.put(e);
    Query q = em.createQuery("select from " + HasEnumJPA.class.getName() + " c" + " where myEnum = :p1");
    q.setParameter("p1", HasEnumJPA.MyEnum.V1.name());
    List<HasEnumJPA> result = (List<HasEnumJPA>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testFilterByEnum_ProvideEnumExplicitly() {
    Entity e = new Entity(HasEnumJPA.class.getSimpleName());
    e.setProperty("myEnum", HasEnumJPA.MyEnum.V1.name());
    ds.put(e);
    Query q = em.createQuery("select from " + HasEnumJPA.class.getName() + " c" + " where myEnum = :p1");
    q.setParameter("p1", HasEnumJPA.MyEnum.V1);
    List<HasEnumJPA> result = (List<HasEnumJPA>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testFilterByEnum_ProvideLiteral() {
    Entity e = new Entity(HasEnumJPA.class.getSimpleName());
    e.setProperty("myEnum", HasEnumJPA.MyEnum.V1.name());
    ds.put(e);
    Query q = em.createQuery(
        "select from " + HasEnumJPA.class.getName() + " c" + " where myEnum = '"
        + HasEnumJPA.MyEnum.V1.name() + "'");
    List<HasEnumJPA> result = (List<HasEnumJPA>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testFilterByShortBlob() {
    Entity e = new Entity(HasBytesJPA.class.getSimpleName());
    e.setProperty("onePrimByte", 8L);
    e.setProperty("shortBlob", new ShortBlob("short blob".getBytes()));
    ds.put(e);
    Query
        q =
        em.createQuery("select from " + HasBytesJPA.class.getName() + " c" + " where shortBlob = :p1");
    q.setParameter("p1", new ShortBlob("short blob".getBytes()));
    List<HasBytesJPA> result = (List<HasBytesJPA>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testFilterByPrimitiveByteArray() {
    Entity e = new Entity(HasBytesJPA.class.getSimpleName());
    e.setProperty("onePrimByte", 8L);
    e.setProperty("primBytes", new ShortBlob("short blob".getBytes()));
    ds.put(e);
    Query
        q =
        em.createQuery("select from " + HasBytesJPA.class.getName() + " c" + " where primBytes = :p1");
    q.setParameter("p1", "short blob".getBytes());
    List<HasBytesJPA> result = (List<HasBytesJPA>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testFilterByByteArray() {
    Entity e = new Entity(HasBytesJPA.class.getSimpleName());
    e.setProperty("onePrimByte", 8L);
    e.setProperty("bytes", new ShortBlob("short blob".getBytes()));
    ds.put(e);
    Query q = em.createQuery("select from " + HasBytesJPA.class.getName() + " c" + " where bytes = :p1");
    q.setParameter("p1", PrimitiveArrays.asList("short blob".getBytes()).toArray(new Byte[0]));
    List<HasBytesJPA> result = (List<HasBytesJPA>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testAliasedFilter() {
    Entity bookEntity = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName() + " b where b.id = :key");
    q.setParameter("key", KeyFactory.keyToString(bookEntity.getKey()));
    @SuppressWarnings("unchecked")
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(bookEntity.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
  }

  public void testAliasedSort() {
    Entity bookEntity1 = Book.newBookEntity("Joe Blow", "67891", "Bar Book");
    Entity bookEntity2 = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
    ds.put(bookEntity1);
    ds.put(bookEntity2);

    javax.persistence.Query q = em.createQuery(
        "select from " + Book.class.getName() + " b order by b.isbn");
    @SuppressWarnings("unchecked")
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(2, books.size());
    assertEquals(bookEntity2.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
    assertEquals(bookEntity1.getKey(), KeyFactory.stringToKey(books.get(1).getId()));
  }

  public void testAliasedEmbeddedFilter() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ds.put(entity);

    Query q = em.createQuery(
        "select from " + Person.class.getName() + " p where p.name.first = \"max\"");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testAliasedEmbeddedSort() {
    Entity entity1 = new Entity(Person.class.getSimpleName());
    entity1.setProperty("first", "max");
    entity1.setProperty("last", "ross");
    entity1.setProperty("anotherFirst", "notmax2");
    entity1.setProperty("anotherLast", "notross");
    ds.put(entity1);
    Entity entity2 = new Entity(Person.class.getSimpleName());
    entity2.setProperty("first", "max");
    entity2.setProperty("last", "ross");
    entity2.setProperty("anotherFirst", "notmax1");
    entity2.setProperty("anotherLast", "notross");
    ds.put(entity2);

    Query q = em.createQuery(
        "select from " + Person.class.getName() + " p order by p.anotherName.first");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.getResultList();
    assertEquals(2, result.size());
    assertEquals(entity2.getKey(), TestUtils.createKey(Person.class, result.get(0).getId()));
    assertEquals(entity1.getKey(), TestUtils.createKey(Person.class, result.get(1).getId()));
  }

  public void testFilterByNullValue_Literal() {
    Entity e = new Entity(NullDataJPA.class.getSimpleName());
    e.setProperty("string", null);
    ds.put(e);

    Query q = em.createQuery("select from " + NullDataJPA.class.getName() + " c where string = null");
    @SuppressWarnings("unchecked")
    List<NullDataJPA> results = (List<NullDataJPA>) q.getResultList();
    assertEquals(1, results.size());
  }

  public void testFilterByNullValue_Param() {
    Entity e = new Entity(NullDataJPA.class.getSimpleName());
    e.setProperty("string", null);
    ds.put(e);

    Query q = em.createQuery("select from " + NullDataJPA.class.getName() + " c where string = :p");
    q.setParameter("p", null);
    @SuppressWarnings("unchecked")
    List<NullDataJPA> results = (List<NullDataJPA>) q.getResultList();
    assertEquals(1, results.size());
  }

  public void testQueryForOneToManySetWithKeyPk() {
    Entity e = new Entity(HasOneToManyKeyPkSetJPA.class.getSimpleName());
    ds.put(e);

    beginTxn();
    Query q = em.createQuery("select from " + HasOneToManyKeyPkSetJPA.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<HasOneToManyKeyPkSetJPA> results = q.getResultList();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getBooks().size());
    commitTxn();
  }

  public void testQueryForOneToManyListWithKeyPk() {
    Entity e = new Entity(HasOneToManyKeyPkListJPA.class.getSimpleName());
    ds.put(e);

    beginTxn();
    Query q = em.createQuery("select from " + HasOneToManyKeyPkListJPA.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<HasOneToManyKeyPkListJPA> results = q.getResultList();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getBooks().size());
    commitTxn();
  }

  public void testQueryForOneToManySetWithLongPk() {
    Entity e = new Entity(HasOneToManyLongPkSetJPA.class.getSimpleName());
    ds.put(e);

    beginTxn();
    Query q = em.createQuery("select from " + HasOneToManyLongPkSetJPA.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<HasOneToManyLongPkSetJPA> results = q.getResultList();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getBooks().size());
    commitTxn();
  }

  public void testQueryForOneToManyListWithLongPk() {
    Entity e = new Entity(HasOneToManyLongPkListJPA.class.getSimpleName());
    ds.put(e);

    beginTxn();
    Query q = em.createQuery("select from " + HasOneToManyLongPkListJPA.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<HasOneToManyLongPkListJPA> results = q.getResultList();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getBooks().size());
    commitTxn();
  }

  public void testQueryForOneToManySetWithUnencodedStringPk() {
    Entity e = new Entity(HasOneToManyUnencodedStringPkSetJPA.class.getSimpleName(), "yar");
    ds.put(e);

    beginTxn();
    Query q = em.createQuery("select from " + HasOneToManyUnencodedStringPkSetJPA.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<HasOneToManyUnencodedStringPkSetJPA> results = q.getResultList();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getBooks().size());
    commitTxn();
  }

  public void testQueryForOneToManyListWithUnencodedStringPk() {
    Entity e = new Entity(HasOneToManyUnencodedStringPkListJPA.class.getSimpleName(), "yar");
    ds.put(e);

    beginTxn();
    Query q = em.createQuery("select from " + HasOneToManyUnencodedStringPkListJPA.class.getName() + " c ");
    @SuppressWarnings("unchecked")
    List<HasOneToManyUnencodedStringPkListJPA> results = q.getResultList();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getBooks().size());
    commitTxn();
  }

  public void testBatchGet_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e1 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e1);
    Entity e2 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e2);
    Entity e3 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e3);

    Key key = KeyFactory.createKey("yar", "does not exist");
    NoQueryDelegate nqd = new NoQueryDelegate().install();
    try {
      Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where id = :ids");
      q.setParameter("ids", Utils.newArrayList(key, e1.getKey(), e2.getKey()));
      @SuppressWarnings("unchecked")
      List<Book> books = (List<Book>) q.getResultList();
      assertEquals(2, books.size());
      assertEquals(e1.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
      assertEquals(e2.getKey(), KeyFactory.stringToKey(books.get(1).getId()));
    } finally {
      nqd.uninstall();
    }
  }

  public void testBatchGet_NoTxn_EncodedStringParam() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e1 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e1);
    Entity e2 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e2);
    Entity e3 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e3);

    Key key = KeyFactory.createKey("yar", "does not exist");
    NoQueryDelegate nqd = new NoQueryDelegate().install();
    try {
      Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where id = :ids");
      q.setParameter("ids", Utils.newArrayList(
          KeyFactory.keyToString(key),
          KeyFactory.keyToString(e1.getKey()),
          KeyFactory.keyToString(e2.getKey())));
      @SuppressWarnings("unchecked")
      List<Book> books = (List<Book>) q.getResultList();
      assertEquals(2, books.size());
      assertEquals(e1.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
      assertEquals(e2.getKey(), KeyFactory.stringToKey(books.get(1).getId()));
    } finally {
      nqd.uninstall();
    }
  }

  public void testBatchGet_NoTxn_In() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e1 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e1);
    Entity e2 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e2);
    Entity e3 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e3);

    NoQueryDelegate nqd = new NoQueryDelegate().install();
    try {
      Key key = KeyFactory.createKey("yar", "does not exist");
      Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where id IN (:ids)");
      q.setParameter("ids", Utils.newArrayList(key, e1.getKey(), e2.getKey()));
      @SuppressWarnings("unchecked")
      List<Book> books = (List<Book>) q.getResultList();
      assertEquals(2, books.size());
      assertEquals(e1.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
      assertEquals(e2.getKey(), KeyFactory.stringToKey(books.get(1).getId()));

      q = em.createQuery("select from " + Book.class.getName() + " c" + " where id IN (:id1, :id2, :id3)");
      q.setParameter("id1", key);
      q.setParameter("id2", e1.getKey());
      q.setParameter("id3", e2.getKey());
      books = (List<Book>) q.getResultList();
      assertEquals(2, books.size());
      assertEquals(e1.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
      assertEquals(e2.getKey(), KeyFactory.stringToKey(books.get(1).getId()));

      q = em.createQuery("select from " + Book.class.getName() + " c" + " where "
                         + "id IN (:id1, :id3) OR id IN (:id2)");
      q.setParameter("id1", key);
      q.setParameter("id2", e1.getKey());
      q.setParameter("id3", e2.getKey());
      books = (List<Book>) q.getResultList();
      assertEquals(2, books.size());
      assertEquals(e1.getKey(), KeyFactory.stringToKey(books.get(1).getId()));
      assertEquals(e2.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
    } finally {
      nqd.uninstall();
    }
  }

  public void testBatchGet_Count_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e1 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e1);
    Entity e2 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e2);
    Entity e3 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e3);

    NoQueryDelegate nqd = new NoQueryDelegate().install();
    try {
      Key key = KeyFactory.createKey("yar", "does not exist");
      Query q = em.createQuery("select count(id) from " + Book.class.getName() + " c" + " where id = :ids");
      q.setParameter("ids", Utils.newArrayList(key, e1.getKey(), e2.getKey()));
      int count = (Integer) q.getSingleResult();
      assertEquals(2, count);
    } finally {
      nqd.uninstall();
    }
  }

  public void testBatchGet_Count_NoTxn_In() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e1 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e1);
    Entity e2 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e2);
    Entity e3 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e3);

    NoQueryDelegate nqd = new NoQueryDelegate().install();
    try {
      Key key = KeyFactory.createKey("yar", "does not exist");
      Query q = em.createQuery("select count(id) from " + Book.class.getName() + " c" + " where id IN (:ids)");
      q.setParameter("ids", Utils.newArrayList(key, e1.getKey(), e2.getKey()));
      int count = (Integer) q.getSingleResult();
      assertEquals(2, count);

      q = em.createQuery("select count(id) from " + Book.class.getName() + " c" + " where id IN (:id1, :id2, :id3)");
      q.setParameter("id1", key);
      q.setParameter("id2", e1.getKey());
      q.setParameter("id3", e2.getKey());
      count = (Integer) q.getSingleResult();
      assertEquals(2, count);
    } finally {
      nqd.uninstall();
    }
  }

  public void testBatchGet_Txn() {
    Entity e1 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e1);
    Entity e2 = Book.newBookEntity(e1.getKey(), "auth", "123432", "title", -40);
    ds.put(e2);
    Entity e3 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e3);

    NoQueryDelegate nqd = new NoQueryDelegate().install();
    try {
      Key key = KeyFactory.createKey(e1.getKey(), "yar", "does not exist");
      Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where id = :ids");
      q.setParameter("ids", Utils.newArrayList(key, e1.getKey(), e2.getKey()));
      @SuppressWarnings("unchecked")
      List<Book> books = (List<Book>) q.getResultList();
      assertEquals(2, books.size());
      assertEquals(e1.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
      assertEquals(e2.getKey(), KeyFactory.stringToKey(books.get(1).getId()));
    } finally {
      nqd.uninstall();
    }
  }

  public void testBatchGet_Txn_In() {
    Entity e1 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e1);
    Entity e2 = Book.newBookEntity(e1.getKey(), "auth", "123432", "title", -40);
    ds.put(e2);
    Entity e3 = Book.newBookEntity("auth", "123432", "title", -40);
    ds.put(e3);

    NoQueryDelegate nqd = new NoQueryDelegate().install();
    try {
      Key key = KeyFactory.createKey(e1.getKey(), "yar", "does not exist");
      Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where id IN (:ids)");
      q.setParameter("ids", Utils.newArrayList(key, e1.getKey(), e2.getKey()));
      @SuppressWarnings("unchecked")
      List<Book> books = (List<Book>) q.getResultList();
      assertEquals(2, books.size());
      assertEquals(e1.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
      assertEquals(e2.getKey(), KeyFactory.stringToKey(books.get(1).getId()));

      q = em.createQuery("select from " + Book.class.getName() + " c" + " where id IN (:id1, :id2, :id3)");
      q.setParameter("id1", key);
      q.setParameter("id2", e1.getKey());
      q.setParameter("id3", e2.getKey());
      books = (List<Book>) q.getResultList();
      assertEquals(2, books.size());
      assertEquals(e1.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
      assertEquals(e2.getKey(), KeyFactory.stringToKey(books.get(1).getId()));
    } finally {
      nqd.uninstall();
    }
  }

  public void testBatchGet_Illegal() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Query q = em.createQuery("select from " + Flight.class.getName() + " c" + " where origin = :ids");
    q.setParameter("ids", Utils.newArrayList());
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
    q = em.createQuery(
        "select from " + Flight.class.getName() + " c" + " where id = :ids and origin = :origin");
    q.setParameter("ids", Utils.newArrayList());
    q.setParameter("origin", "bos");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
    q = em.createQuery(
        "select from " + Flight.class.getName() + " c" + " where origin = :origin and id = :ids");
    q.setParameter("origin", "bos");
    q.setParameter("ids", Utils.newArrayList());
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
    q = em.createQuery("select from " + Flight.class.getName() + " c" + " where id > :ids");
    q.setParameter("ids", Utils.newArrayList());
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
    q = em.createQuery("select from " + Flight.class.getName() + " c" + " where id = :ids order by id");
    q.setParameter("ids", Utils.newArrayList());
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testNamedQuery() {
    Query q = em.createNamedQuery("namedQuery");
    assertTrue(q.getResultList().isEmpty());
    Entity e = Book.newBookEntity("author", "12345", "yam");
    ds.put(e);
    Book b = (Book) q.getSingleResult();
    assertEquals(e.getKey(), KeyFactory.stringToKey(b.getId()));
  }

  public void testRestrictFetchedFields_UnknownField() {
    Query q = em.createQuery("select dne from " + Book.class.getName());
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testRestrictFetchedFields_OneField() {
    Entity e1 = Book.newBookEntity("author", "12345", "the title");
    ds.put(e1);
    Query q = em.createQuery("select title from " + Book.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<String> titles = (List<String>) q.getResultList();
    assertEquals(1, titles.size());
    assertEquals("the title", titles.get(0));

    Entity e2 = Book.newBookEntity("another author", "123456", "the other title");
    ds.put(e2);

    @SuppressWarnings("unchecked")
    List<String> titles2 = (List<String>) q.getResultList();
    assertEquals(2, titles2.size());
    assertEquals("the title", titles2.get(0));
    assertEquals("the other title", titles2.get(1));
  }

  public void testRestrictFetchedFields_OneIdField() {
    Entity e1 = Book.newBookEntity("author", "12345", "the title");
    ds.put(e1);

    Entity e2 = Book.newBookEntity("another author", "123456", "the other title");
    ds.put(e2);

    // Remove this blocker since the test needs to update!
    DatastoreServiceInterceptor.uninstall();

    beginTxn();
    Query q = em.createQuery("select id from " + Book.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<String> ids = (List<String>) q.getResultList();
    assertEquals(2, ids.size());
    assertEquals(KeyFactory.keyToString(e1.getKey()), ids.get(0));
    assertEquals(KeyFactory.keyToString(e2.getKey()), ids.get(1));

    Book b = em.find(Book.class, e1.getKey());
    assertEquals("author", b.getAuthor());
    b.setAuthor("not author");
    commitTxn();
    beginTxn();
    b = em.find(Book.class, e1.getKey());
    assertEquals("not author", b.getAuthor());
    commitTxn();
  }

  public void testRestrictFetchedFields_TwoFields() {
    Entity e1 = Book.newBookEntity("author", "12345", "the title");
    ds.put(e1);
    Query q = em.createQuery("select author, isbn from " + Book.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.getResultList();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals("author", results.get(0)[0]);
    assertEquals("12345", results.get(0)[1]);

    Entity e2 = Book.newBookEntity("another author", null, "the other title");
    ds.put(e2);

    @SuppressWarnings("unchecked")
    List<Object[]> results2 = (List<Object[]>) q.getResultList();
    assertEquals(2, results2.size());
    assertEquals(2, results2.get(0).length);
    assertEquals("author", results2.get(0)[0]);
    assertEquals("12345", results2.get(0)[1]);
    assertEquals(2, results2.get(0).length);
    assertEquals("another author", results2.get(1)[0]);
    assertNull(results2.get(1)[1]);
  }

  public void testRestrictFetchedFields_TwoIdFields() {
    Entity e1 = Book.newBookEntity("author", "12345", "the title");
    ds.put(e1);
    Query q = em.createQuery("select id, id from " + Book.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.getResultList();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results.get(0)[0]);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results.get(0)[1]);

    Entity e2 = Book.newBookEntity("another author", null, "the other title");
    ds.put(e2);

    @SuppressWarnings("unchecked")
    List<Object[]> results2 = (List<Object[]>) q.getResultList();
    assertEquals(2, results2.size());
    assertEquals(2, results2.get(0).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results2.get(0)[0]);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results2.get(0)[1]);
    assertEquals(2, results2.get(0).length);
    assertEquals(KeyFactory.keyToString(e2.getKey()), results2.get(1)[0]);
    assertEquals(KeyFactory.keyToString(e2.getKey()), results2.get(1)[1]);
  }

  public void testRestrictFetchedFields_TwoIdFields_IdIsFirst() {
    Entity e1 = Book.newBookEntity("author", "12345", "the title");
    ds.put(e1);
    Query q = em.createQuery("select id, author from " + Book.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.getResultList();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results.get(0)[0]);
    assertEquals("author", results.get(0)[1]);

    Entity e2 = Book.newBookEntity("another author", null, "the other title");
    ds.put(e2);

    @SuppressWarnings("unchecked")
    List<Object[]> results2 = (List<Object[]>) q.getResultList();
    assertEquals(2, results2.size());
    assertEquals(2, results2.get(0).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results2.get(0)[0]);
    assertEquals("author", results2.get(0)[1]);
    assertEquals(2, results2.get(0).length);
    assertEquals(KeyFactory.keyToString(e2.getKey()), results2.get(1)[0]);
    assertEquals("another author", results2.get(1)[1]);
  }

  public void testRestrictFetchedFields_TwoIdFields_IdIsSecond() {
    Entity e1 = Book.newBookEntity("author", "12345", "the title");
    ds.put(e1);
    Query q = em.createQuery("select author, id from " + Book.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.getResultList();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals("author", results.get(0)[0]);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results.get(0)[1]);

    Entity e2 = Book.newBookEntity("another author", null, "the other title");
    ds.put(e2);

    @SuppressWarnings("unchecked")
    List<Object[]> results2 = (List<Object[]>) q.getResultList();
    assertEquals(2, results2.size());
    assertEquals(2, results2.get(0).length);
    assertEquals("author", results2.get(0)[0]);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results2.get(0)[1]);
    assertEquals(2, results2.get(0).length);
    assertEquals("another author", results2.get(1)[0]);
    assertEquals(KeyFactory.keyToString(e2.getKey()), results2.get(1)[1]);
  }

  public void testRestrictFetchedFields_OneToOne() {
    Entity e1 = new Entity(HasOneToOneJPA.class.getSimpleName());
    ds.put(e1);
    Entity e2 = Book.newBookEntity(e1.getKey(), "author", "12345", "the title");
    ds.put(e2);
    Query q = em.createQuery("select id, book from " + HasOneToOneJPA.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.getResultList();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results.get(0)[0]);
    Book b = em.find(Book.class, e2.getKey());
    assertEquals(b, results.get(0)[1]);
  }

  public void testRestrictFetchedFields_OneToMany() {
    Entity e1 = new Entity(HasOneToManyListJPA.class.getSimpleName());
    ds.put(e1);
    Entity e2 = Book.newBookEntity(e1.getKey(), "author", "12345", "the title");
    ds.put(e2);
    Query q = em.createQuery("select id, books from " + HasOneToManyListJPA.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.getResultList();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results.get(0)[0]);
    Book b = em.find(Book.class, e2.getKey());
    List<Book> books = (List<Book>) results.get(0)[1];
    assertEquals(1, books.size());
    assertEquals(b, books.get(0));
  }

  public void testRestrictFetchedFields_AliasedField() {
    Entity e1 = Book.newBookEntity("author", "12345", "the title");
    ds.put(e1);
    Query q = em.createQuery("select b.isbn from " + Book.class.getName() + " b");
    @SuppressWarnings("unchecked")
    List<String> isbns = (List<String>) q.getResultList();
    assertEquals(1, isbns.size());
    assertEquals("12345", isbns.get(0));
  }

  public void testRestrictFetchedFieldsAndCount() {
    Entity e1 = Book.newBookEntity("author", "12345", "the title");
    ds.put(e1);
    Query q = em.createQuery("select count(id), isbn from " + Book.class.getName() + " c");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // good
        }
        else {
          throw pe;
        }
    }

    q = em.createQuery("select isbn, count(id) from " + Book.class.getName() + " c");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // good
        }
        else {
          throw pe;
        }
    }
  }

  public void testRestrictFetchedFields_EmbeddedField() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ds.put(entity);

    Query q = em.createQuery("select name.first, anotherName.last from " + Person.class.getName() + " c");
    @SuppressWarnings("unchecked")
    List<Object[]> result = (List<Object[]>) q.getResultList();
    assertEquals(1, result.size());
  }

  public void testIsNull() {
    Entity e = Book.newBookEntity("author", null, "title");
    ds.put(e);
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where isbn is NULL");
    @SuppressWarnings("unchecked")
    List<Book> books = q.getResultList();
    assertEquals(1, books.size());
  }

  public void testIsNotNull() {
    Entity e = Book.newBookEntity("auth", "isbn", null);
    ds.put(e);
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title is not null");
    assertTrue(q.getResultList().isEmpty());
    Query q2 = em.createQuery("select from " + Book.class.getName() + " c" + " where title <> null");
    assertTrue(q2.getResultList().isEmpty());
    e = Book.newBookEntity("auth2", "isbn2", "not null");
    ds.put(e);
    Book b = (Book) q.getSingleResult();
    assertEquals("not null", b.getTitle());
    b = (Book) q2.getSingleResult();
    assertEquals("not null", b.getTitle());
  }

  public void testIsNotNull_Param() {
    Entity e = Book.newBookEntity("auth", "isbn", null);
    ds.put(e);
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title <> :p");
    q.setParameter("p", null);
    assertTrue(q.getResultList().isEmpty());
    e = Book.newBookEntity("auth2", "isbn2", "not null");
    ds.put(e);
    Book b = (Book) q.getSingleResult();
    assertEquals("not null", b.getTitle());
  }

  public void testNotEqual() {
    Entity e = Book.newBookEntity("auth", "isbn", "yar");
    ds.put(e);
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title <> 'yar'");
    assertTrue(q.getResultList().isEmpty());
    e = Book.newBookEntity("auth2", "isbn2", "not yar");
    ds.put(e);
    Book b = (Book) q.getSingleResult();
    assertEquals("not yar", b.getTitle());
  }

  public void testNotEqual_Param() {
    Entity e = Book.newBookEntity("auth", "isbn", "yar");
    ds.put(e);
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title <> :p");
    q.setParameter("p", "yar");
    assertTrue(q.getResultList().isEmpty());
    e = Book.newBookEntity("auth2", "isbn2", "not yar");
    ds.put(e);
    Book b = (Book) q.getSingleResult();
    assertEquals("not yar", b.getTitle());
  }

  public void testIn_Literals() {
    Entity e = Book.newBookEntity("auth1", "isbn1", "yar1");
    Entity e2 = Book.newBookEntity("auth2", "isbn2", null);
    Entity e3 = Book.newBookEntity("auth3", "isbn3", "yar3");
    ds.put(Arrays.asList(e, e2, e3));
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where author IN ('auth1', 'auth3')");
    List<Book> books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(1).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where title IN (null, 'yar1')");
    books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e2.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(1).getId());
  }

  public void testIn_Params() {
    Entity e = Book.newBookEntity("auth1", "isbn1", "yar1");
    Entity e2 = Book.newBookEntity("auth2", "isbn2", "yar2");
    Entity e3 = Book.newBookEntity("auth3", "isbn3", "yar3");
    ds.put(Arrays.asList(e, e2, e3));
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where author IN (:p1, :p2)");
    q.setParameter("p1", "auth1");
    q.setParameter("p2", "auth3");
    List<Book> books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(1).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" +
                       " where author IN (:p1) OR author IN (:p2)");
    q.setParameter("p1", "auth1");
    q.setParameter("p2", "auth3");
    books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(1).getId());
  }

  public void testIn_CollectionParam() {
    Entity e = Book.newBookEntity("auth1", "isbn1", "yar1");
    Entity e2 = Book.newBookEntity("auth2", "isbn2", "yar2");
    Entity e3 = Book.newBookEntity("auth3", "isbn3", "yar3");
    ds.put(Arrays.asList(e, e2, e3));
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where author IN (:p1)");
    q.setParameter("p1", Arrays.asList("auth1", "auth3"));
    List<Book> books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(1).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" +
                       " where author IN (:p1) OR author IN (:p2)");
    q.setParameter("p1", Arrays.asList("auth1"));
    q.setParameter("p2", Arrays.asList("auth3"));
    books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(1).getId());
  }

  public void testMultipleIn_Literals() {
    Entity e = Book.newBookEntity("auth1", "isbn1", "yar1");
    Entity e2 = Book.newBookEntity("auth2", "isbn2", "yar2");
    Entity e3 = Book.newBookEntity("auth3", "isbn3", "yar3");
    ds.put(Arrays.asList(e, e2, e3));
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where "
                             + "author IN ('auth1', 'auth3') AND isbn IN ('isbn3', 'isbn2')");
    List<Book> books = q.getResultList();
    assertEquals(1, books.size());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(0).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where "
                             + "author IN ('auth1') OR author IN ('auth4', 'auth2')");
    books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e2.getKey()), books.get(1).getId());
  }

  public void testMultipleIn_Params() {
    Entity e = Book.newBookEntity("auth1", "isbn1", "yar1");
    Entity e2 = Book.newBookEntity("auth2", "isbn2", "yar2");
    Entity e3 = Book.newBookEntity("auth3", "isbn3", "yar3");
    ds.put(Arrays.asList(e, e2, e3));
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where "
                             + "author IN (:p1, :p2) AND isbn IN (:p3, :p4)");
    q.setParameter("p1", "auth1");
    q.setParameter("p2", "auth3");
    q.setParameter("p3", "isbn3");
    q.setParameter("p4", "isbn2");
    List<Book> books = q.getResultList();
    assertEquals(1, books.size());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(0).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where "
                             + "author IN (:p1, :p2) OR author IN (:p3, :p4)");
    q.setParameter("p1", "auth1");
    q.setParameter("p2", "auth3");
    q.setParameter("p3", "auth4");
    q.setParameter("p4", "auth5");
    books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(1).getId());
  }

  public void testMultipleIn_Params_KeyFilter() {
    Entity e = Book.newBookEntity("auth1", "isbn1", "yar1");
    Entity e2 = Book.newBookEntity("auth2", "isbn2", "yar2");
    Entity e3 = Book.newBookEntity("auth3", "isbn3", "yar3");
    ds.put(Arrays.asList(e, e2, e3));
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where "
                             + "id IN (:p1) AND isbn IN (:p2, :p3)");
    q.setParameter("p1", e2.getKey());
    q.setParameter("p2", "isbn2");
    q.setParameter("p3", "isbn3");
    @SuppressWarnings("unchecked")
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(KeyFactory.keyToString(e2.getKey()), books.get(0).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where "
                             + "(id = :p1 or id = :p2) AND isbn IN (:p3, :p4)");
    q.setParameter("p1", e2.getKey());
    q.setParameter("p2", e3.getKey());
    q.setParameter("p3", "isbn2");
    q.setParameter("p4", "isbn3");
    @SuppressWarnings("unchecked")
    List<Book> books2 = (List<Book>) q.getResultList();
    assertEquals(2, books2.size());
  }

  public void testOr_Literals() {
    Entity e = Book.newBookEntity("auth1", "isbn1", "yar1");
    Entity e2 = Book.newBookEntity("auth2", "isbn2", null);
    Entity e3 = Book.newBookEntity("auth3", "isbn3", "yar3");
    ds.put(Arrays.asList(e, e2, e3));
    Query q = em.createQuery("select from " + Book.class.getName() + " c" +
                             " where author = 'auth1' or author = 'auth3'");
    List<Book> books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(1).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" +
                       " where title is null or title = 'yar1'");
    books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e2.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(1).getId());
  }

  public void testOr_Params() {
    Entity e = Book.newBookEntity("auth1", "isbn1", "yar1");
    Entity e2 = Book.newBookEntity("auth2", "isbn2", "yar2");
    Entity e3 = Book.newBookEntity("auth3", "isbn3", "yar3");
    ds.put(Arrays.asList(e, e2, e3));
    Query q = em.createQuery("select from " + Book.class.getName() + " c" +
                             " where author = :p1 or author = :p2");
    q.setParameter("p1", "auth1");
    q.setParameter("p2", "auth3");
    List<Book> books = q.getResultList();
    assertEquals(2, books.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), books.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(1).getId());
  }

  public void testMultipleOr_Literals() {
    Entity e = Book.newBookEntity("auth1", "isbn1", "yar1");
    Entity e2 = Book.newBookEntity("auth2", "isbn2", "yar2");
    Entity e3 = Book.newBookEntity("auth3", "isbn3", "yar3");
    ds.put(Arrays.asList(e, e2, e3));
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where "
                             + "(author  = 'auth1' or author = 'auth3') AND "
                             + "(isbn = 'isbn3' or isbn = 'isbn2')");
    List<Book> books = q.getResultList();
    assertEquals(1, books.size());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(0).getId());
  }

  public void testMultipleOr_Params() {
    Entity e = Book.newBookEntity("auth1", "isbn1", "yar1");
    Entity e2 = Book.newBookEntity("auth2", "isbn2", "yar2");
    Entity e3 = Book.newBookEntity("auth3", "isbn3", "yar3");
    ds.put(Arrays.asList(e, e2, e3));
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where "
                             + "(author = :p1 or author = :p2) AND "
                             + "(isbn = :p3 or isbn = :p4)");
    q.setParameter("p1", "auth1");
    q.setParameter("p2", "auth3");
    q.setParameter("p3", "isbn3");
    q.setParameter("p4", "isbn2");
    List<Book> books = q.getResultList();
    assertEquals(1, books.size());
    assertEquals(KeyFactory.keyToString(e3.getKey()), books.get(0).getId());
  }

  public void testIsNullChild() {
    Entity e = new Entity(HasOneToOneJPA.class.getSimpleName());
    ds.put(e);
    Query q = em.createQuery(
        "select from " + HasOneToOneJPA.class.getName() + " c" + " where book is null");
    try {
      q.getResultList();
      fail("expected");
    } catch (PersistenceException pe) {
      // good
    }
  }

  public void testIsNullParent() {
    Entity e = new Entity(HasOneToOneJPA.class.getSimpleName());
    Key key = ds.put(e);
    e = new Entity(HasOneToOneParentJPA.class.getSimpleName(), key);
    ds.put(e);
    Query q = em.createQuery(
        "select from " + HasOneToOneParentJPA.class.getName() + " c" + " where parent is null");
    try {
      q.getResultList();
      fail("expected");
    } catch (PersistenceException pe) {
      // good
    }
  }

  public void testQueryWithEntityShortName() {
    // Uninstall this since the test needs to update!
    DatastoreServiceInterceptor.uninstall();

    Query q = em.createQuery("select b from bookalias b where title = 'yam'");
    assertTrue(q.getResultList().isEmpty());

    Book b = new Book();
    b.setTitle("yam");
    beginTxn();
    em.persist(b);
    commitTxn();

    assertEquals(b, q.getResultList().get(0));

    q = em.createQuery("select from bookalias b where title = 'yam'");
    assertEquals(b, q.getResultList().get(0));

    q = em.createQuery("select from bookalias b where b.title = 'yam'");
    assertEquals(b, q.getResultList().get(0));
  }

  public void testQueryWithSingleCharacterLiteral() {
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title = 'y'");
    assertTrue(q.getResultList().isEmpty());

    Entity e = Book.newBookEntity("author", "12345", "y");
    ds.put(e);
    Book b = (Book) q.getSingleResult();
    assertEquals(e.getKey(), KeyFactory.stringToKey(b.getId()));
  }

  public void testSetFirstResultAfterSetParameter() {
    Entity e = Book.newBookEntity("author", "12345", "y");
    ds.put(e);
    e = Book.newBookEntity("author", "12345", "y");
    ds.put(e);

    Query query = em.createQuery("SELECT b FROM " + Book.class.getName() + " b WHERE b.title = :title");
    query.setParameter("title", "y");
    query.setFirstResult(1);
    Book b = (Book) query.getSingleResult();
    assertEquals(e.getKey(), KeyFactory.stringToKey(b.getId()));
  }

  public void testSetMaxResultsAfterSetParameter() {
    Entity e = Book.newBookEntity("author", "12345", "y");
    ds.put(e);
    Entity e2 = Book.newBookEntity("author", "12345", "y");
    ds.put(e2);

    Query query = em.createQuery("SELECT b FROM " + Book.class.getName() + " b WHERE b.title = :title");
    query.setParameter("title", "y");
    query.setMaxResults(1);
    Book b = (Book) query.getSingleResult();
    assertEquals(e.getKey(), KeyFactory.stringToKey(b.getId()));
  }

  public void testSetFirstResultAfterSetPCParameter() {
    Entity e = new Entity(HasOneToOneJPA.class.getSimpleName());
    Key key = ds.put(e);
    e = new Entity(HasOneToOneParentJPA.class.getSimpleName(), key);
    ds.put(e);
    e = new Entity(HasOneToOneParentJPA.class.getSimpleName(), key);
    ds.put(e);
    HasOneToOneJPA parent = em.find(HasOneToOneJPA.class, key);
    Query q = em.createQuery(
        "select from " + HasOneToOneParentJPA.class.getName() + " c" + " where parent = :p");
    q.setParameter("p", parent);
    q.setFirstResult(1);
    HasOneToOneParentJPA child = (HasOneToOneParentJPA) q.getSingleResult();
    assertEquals(e.getKey(), KeyFactory.stringToKey(child.getId()));
  }

  public void testSetMaxResultsAfterSetPCParameter() {
    Entity e = new Entity(HasOneToOneJPA.class.getSimpleName());
    Key key = ds.put(e);
    Entity child1 = new Entity(HasOneToOneParentJPA.class.getSimpleName(), key);
    ds.put(child1);
    Entity child2 = new Entity(HasOneToOneParentJPA.class.getSimpleName(), key);
    ds.put(child2);
    HasOneToOneJPA parent = em.find(HasOneToOneJPA.class, key);
    Query q = em.createQuery(
        "select from " + HasOneToOneParentJPA.class.getName() + " c" + " where parent = :p");
    q.setParameter("p", parent);
    q.setMaxResults(1);
    HasOneToOneParentJPA child = (HasOneToOneParentJPA) q.getSingleResult();
    assertEquals(child1.getKey(), KeyFactory.stringToKey(child.getId()));
  }

  public void testAccessResultsAfterClose() {
    for (int i = 0; i < 3; i++) {
      Entity e = Book.newBookEntity("this", "that", "the other");
      ds.put(e);
    }
    beginTxn();
    Query q = em.createQuery("select from " + Book.class.getName() + " c");
    List<Book> results = q.getResultList();
    Iterator<Book> iter = results.iterator();
    iter.next();
    commitTxn();
    em.close();
    Book b = iter.next();
    b.getIsbn();
    b.getAuthor();
    iter.next();
  }

  public void testAncestorQueryForDifferentEntityGroupWithCurrentTxn() {
    switchDatasource(EntityManagerFactoryName.transactional_ds_non_transactional_ops_allowed);
    Entity e1 = Book.newBookEntity("this", "that", "the other");
    ds.put(e1);

    beginTxn();
    // Not used, but associates the txn with the book's entity group
    /*Book b = */em.find(Book.class, e1.getKey());
    Query q = em.createQuery(
        "select from " + HasKeyAncestorStringPkJPA.class.getName() + " c" + " where ancestorKey = :p");
    q.setParameter("p", KeyFactory.keyToString(KeyFactory.createKey("yar", 33L)));
    try {
      q.getResultList().iterator();
      fail("expected iae");
    } catch (PersistenceException e) {
      // good
    }

    q.setHint("gae.exclude-query-from-txn", false);
    try {
      q.getResultList().iterator();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
    q.setHint("gae.exclude-query-from-txn", true);
    q.getResultList();
  }

  public void testLikeQuery_Literal() {
    Entity e1 = Book.newBookEntity("this", "that", "xxxx");
    ds.put(e1);
    Entity e2 = Book.newBookEntity("this", "that", "y");
    ds.put(e2);
    Entity e3 = Book.newBookEntity("this", "that", "yb");
    ds.put(e3);
    Entity e4 = Book.newBookEntity("this", "that", "z");
    ds.put(e4);

    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE 'y%'");
    @SuppressWarnings("unchecked")
    List<Book> result = q.getResultList();

    assertEquals(2, result.size());
    assertEquals(KeyFactory.keyToString(e2.getKey()), result.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), result.get(1).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE 'z%'");
    @SuppressWarnings("unchecked")
    List<Book> result2 = q.getResultList();

    assertEquals(1, result2.size());
    assertEquals(KeyFactory.keyToString(e4.getKey()), result2.get(0).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE 'za%'");
    @SuppressWarnings("unchecked")
    List<Book> result3 = q.getResultList();
    assertTrue(result3.isEmpty());
  }

  public void testLikeQuery_Literal2() {
    for (int i = 0; i < 10; i++) {
      Entity e1 = Book.newBookEntity("this", "that", "xxxx");
      ds.put(e1);
    }

    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE 'x%'");
    @SuppressWarnings("unchecked")
    List<Book> result = q.getResultList();

    assertEquals(10, result.size());
  }

  public void testLikeQuery_Param() {
    Entity e1 = Book.newBookEntity("this", "that", "xxxx");
    ds.put(e1);
    Entity e2 = Book.newBookEntity("this", "that", "y");
    ds.put(e2);
    Entity e3 = Book.newBookEntity("this", "that", "yb");
    ds.put(e3);
    Entity e4 = Book.newBookEntity("this", "that", "z");
    ds.put(e4);

    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE :p");
    q.setParameter("p", "y%");

    @SuppressWarnings("unchecked")
    List<Book> result = q.getResultList();

    assertEquals(2, result.size());
    assertEquals(KeyFactory.keyToString(e2.getKey()), result.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), result.get(1).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE 'z%'");
    q.setParameter("p", "y%");
    @SuppressWarnings("unchecked")
    List<Book> result2 = q.getResultList();

    assertEquals(1, result2.size());
    assertEquals(KeyFactory.keyToString(e4.getKey()), result2.get(0).getId());

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE 'za%'");
    q.setParameter("p", "y%");
    @SuppressWarnings("unchecked")
    List<Book> result3 = q.getResultList();
    assertTrue(result3.isEmpty());
  }

  public void testLikeQuery_InvalidLiteral() {
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE '%y'");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // good
        }
        else {
          throw pe;
        }
    }

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE 'y%y'");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // good
        }
        else {
          throw pe;
        }
    }

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE 'y'");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // good
        }
        else {
          throw pe;
        }
    }

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE 'y%' and author LIKE 'z%'");
    try {
      q.getResultList().iterator();
      fail("expected exception");
    } catch (PersistenceException pe) {
      // good
    }
  }

  public void testLikeQuery_InvalidParameter() {
    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE :p");
    q.setParameter("p", "%y");
    try {
      q.getResultList().iterator();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // good
        }
        else {
          throw pe;
        }
    }

    q.setParameter("p", "y%y");
    try {
      q.getResultList().iterator();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // good
        }
        else {
          throw pe;
        }
    }

    q.setParameter("p", "y");
    try {
      q.getResultList().iterator();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // good
        }
        else {
          throw pe;
        }
    }

    q.setParameter("p", 23);
    try {
      q.getResultList().iterator();
      fail("expected exception");
    } catch (PersistenceException pe) {
      // good
    }

    q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE :p and author LIKE :q");
    q.setParameter("p", "y%");
    q.setParameter("q", "y%");
    try {
      q.getResultList().iterator();
      fail("expected exception");
    } catch (PersistenceException pe) {
      // good
    }
  }

  public void testLikeQuery_Literal_CustomEscapeChar() {
    Entity e1 = Book.newBookEntity("this", "that", "xxxx");
    ds.put(e1);
    Entity e2 = Book.newBookEntity("this", "that", "%");
    ds.put(e2);
    Entity e3 = Book.newBookEntity("this", "that", "%a");
    ds.put(e3);
    Entity e4 = Book.newBookEntity("this", "that", "z");
    ds.put(e4);

    Query q = em.createQuery("select from " + Book.class.getName() + " c" + " where title LIKE '%^' ESCAPE '^'");
    try {
      q.getResultList();
      fail("DataNucleus must now be parsing 'ESCAPE'.  Hooray!");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
            // Not good, but correct because DataNuc doesn't handle ESCAPE yet.
        }
        else {
          throw pe;
        }
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException uefe) {
    }

//    assertEquals(2, result.size());
//    assertEquals(KeyFactory.keyToString(e2.getKey()), result.get(0).getId());
//    assertEquals(KeyFactory.keyToString(e3.getKey()), result.get(1).getId());
//
//    q = em.createQuery("select from " + Book.class.getName() + " where title LIKE 'a^' ESCAPE '^'");
//    @SuppressWarnings("unchecked")
//    List<Book> result3 = q.getResultList();
//    assertTrue(result3.isEmpty());
  }

  public void testUpdateQueryFails() {
    Query q = em.createQuery("update " + Book.class.getName() + " set author = 'yar'");
    try {
      q.executeUpdate();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testNullAncestorParam() {
    Query q = em.createQuery(
        "select from " + HasKeyAncestorStringPkJPA.class.getName() + " c" + " where ancestorKey = :p");
    q.setParameter("p", null);
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
          // good
        }
        else {
          throw pe;
        }
    }
  }

  public void testCurrentDateFuncs() {
    Entity e = KitchenSink.newKitchenSinkEntity("blarg", null);
    ds.put(e);

    for (String dateFunc : Arrays.asList("CURRENT_TIMESTAMP", "CURRENT_DATE")) {
      Query q = em.createQuery("select from " + KitchenSink.class.getName() + " c where dateVal < " + dateFunc);
      @SuppressWarnings("unchecked")
      List<KitchenSink> results = (List<KitchenSink>) q.getResultList();
      assertEquals(1, results.size());

      DatastoreQuery.NowProvider orig = DatastoreQuery.NOW_PROVIDER;
      DatastoreQuery.NOW_PROVIDER = new DatastoreQuery.NowProvider() {
        public Date now() {
          return new Date(KitchenSink.DATE1.getTime() - 1);
        }
      };
      try {
        e.setProperty("dateVal", new Date(KitchenSink.DATE1.getTime() - 1));
        assertTrue(q.getResultList().isEmpty());
        assertEquals(1, results.size());
      } finally {
        DatastoreQuery.NOW_PROVIDER = orig;
      }
    }
  }

  public void testPositionalParams() {
    Entity e = Book.newBookEntity("me", "isbn", "return of yam");
    ds.put(e);
    List<Book> result =
        em.createQuery("select b from " + Book.class.getName() + " b where isbn=?1 AND title=?2")
            .setParameter(1, "isbn").setParameter(2, "return of yam").getResultList();
    assertEquals(1, result.size());
  }

  public void testOutOfOrderPositionalParams() {
    Entity e = Book.newBookEntity("me", "isbn", "return of yam");
    ds.put(e);
    List<Book> result =
        em.createQuery("select b from " + Book.class.getName() + " b where isbn=?2 AND title=?1")
            .setParameter(2, "isbn").setParameter(1, "return of yam").getResultList();
    assertEquals(1, result.size());
  }

  public void testDefaultClassName() {
    ds.put(new Entity(DetachableJPA.class.getSimpleName()));
    assertEquals(1, em.createQuery("select o from " + DetachableJPA.class.getName() + " o").getResultList().size());
    assertEquals(1, em.createQuery("select o from DetachableJPA o").getResultList().size());
  }

  public void testQueryWithEntityShortName2() {
    // Uninstall this since the test needs to update!
    DatastoreServiceInterceptor.uninstall();

    Query q = em.createQuery("select b from bookalias b where title = 'yam'");
    assertTrue(q.getResultList().isEmpty());

    Book b = new Book();
    b.setTitle("yam");
    beginTxn();
    em.persist(b);
    commitTxn();

    assertEquals(b, q.getResultList().get(0));

    q = em.createQuery("select from bookalias c where title = 'yam'");
    assertEquals(b, q.getResultList().get(0));

    q = em.createQuery("select from bookalias b where b.title = 'yam'");
    assertEquals(b, q.getResultList().get(0));
  }


  public void testQueryForClassWithNameStartingWithIn() {
    // Uninstall this since the test needs to update!
    DatastoreServiceInterceptor.uninstall();

    // force the class metadata to load - we've seen occasional problems
    // where the class can't be resolved for a query if we don't reference it first.
    InTheHouseJPA pojo = new InTheHouseJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertNotNull(em.createQuery("select b from InTheHouseJPA b").getResultList().get(0));
  }

  public void testNonexistentClassThrowsReasonableException() {
    try {
      em.createQuery("select o from xyam o").getResultList();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }

  }

  public void testSubclassesNotSupported() {
    JPQLQuery q = new JPQLQuery(getExecutionContext());
    q.setCandidateClass(Base1.class);
    q.setSubclasses(false);
    try {
      q.setSubclasses(true);
      fail("expected nue");
    } catch (NucleusUserException nue) {
      // good
    }
    q.setCandidateClass(UnidirTop.class);
    q.setSubclasses(false);
    q.setSubclasses(true);
  }

  public void testQueryTimeout() {
    DatastoreServiceFactoryInternal.setDatastoreService(null);
    ApiProxy.ApiConfig config = new ApiProxy.ApiConfig();
    config.setDeadlineInSeconds(3.0);
    ApiProxy.Delegate delegate = EasyMock.createMock(ApiProxy.Delegate.class);
    EasyMock.expect(delegate.makeAsyncCall(EasyMock.isA(ApiProxy.Environment.class),
                              EasyMock.eq(LocalDatastoreService.PACKAGE),
                              EasyMock.eq("RunQuery"),
                              EasyMock.isA(byte[].class),
                              ApiConfigMatcher.eqApiConfig(config)))
            .andThrow(new DatastoreTimeoutException("too long")).anyTimes();
    EasyMock.replay(delegate);
    ApiProxy.Delegate original = getDelegateForThread();
    setDelegateForThread(delegate);

    try {
      Query q = em.createQuery("select from " + Book.class.getName() + " c");
      q.setHint("javax.persistence.query.timeout", 3000);
      try {
        q.getResultList().iterator();
        fail("expected exception");
      } catch (QueryTimeoutException e) {
        // good
      }
    } finally {
      setDelegateForThread(original);
    }
    EasyMock.verify(delegate);
  }

  public void testQueryTimeoutWhileIterating() {
    DatastoreServiceFactoryInternal.setDatastoreService(null);

    // Need to have enough data to ensure a Next call
    for (int i = 0; i < 21; i++) {
      Entity bookEntity = Book.newBookEntity("Joe Blow", "67890", "Bar Book");
      ds.put(bookEntity);
    }
    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          boolean exploded = false;
          protected void doIntercept(String methodName) {
            if (!exploded && methodName.equals("Next")) {
              exploded = true;
              throw new DatastoreTimeoutException("boom: " + methodName);
            }
          }
        };

    ApiProxy.Delegate original = getDelegateForThread();
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(getDelegateForThread(), policy);
    setDelegateForThread(dd);

    try {
      javax.persistence.Query q = em.createQuery(
          "select from " + Book.class.getName() + " c");
      try {
        @SuppressWarnings("unchecked")
        List<Book> books = (List<Book>) q.getResultList();
        books.size();
        fail("expected exception");
      } catch (QueryTimeoutException qte) {
        assertTrue(qte.getCause() instanceof org.datanucleus.store.query.QueryTimeoutException);
        assertTrue(qte.getCause().getCause() instanceof DatastoreTimeoutException);        
      }
    } finally {
      setDelegateForThread(original);
    }
  }

  public void testOverrideReadConsistency() {
    DatastoreServiceFactoryInternal.setDatastoreService(null);
    ApiProxy.Delegate original = getDelegateForThread();
    try {
      ApiProxy.Delegate delegate = EasyMock.createMock(ApiProxy.Delegate.class);
      setDelegateForThread(delegate);
      ApiProxy.ApiConfig config = new ApiProxy.ApiConfig();
      EasyMock.expect(delegate.makeAsyncCall(EasyMock.isA(ApiProxy.Environment.class),
                                EasyMock.eq(LocalDatastoreService.PACKAGE),
                                EasyMock.eq("RunQuery"),
                                FailoverMsMatcher.eqFailoverMs(null),
                                ApiConfigMatcher.eqApiConfig(config))).andReturn(null);
      EasyMock.replay(delegate);
      Query q = em.createQuery("select from " + Book.class.getName() + " c");
      q.getResultList();
      EasyMock.verify(delegate);

      delegate = EasyMock.createMock(ApiProxy.Delegate.class);
      setDelegateForThread(delegate);
      EasyMock.expect(delegate.makeAsyncCall(EasyMock.isA(ApiProxy.Environment.class),
                                EasyMock.eq(LocalDatastoreService.PACKAGE),
                                EasyMock.eq("RunQuery"),
                                FailoverMsMatcher.eqFailoverMs(null),
                                ApiConfigMatcher.eqApiConfig(config))).andReturn(null);
      EasyMock.replay(delegate);
      q = em.createQuery("select from " + Book.class.getName() + " c");
      q.setHint("datanucleus.appengine.datastoreReadConsistency", null);
      q.getResultList();
      EasyMock.verify(delegate);

      delegate = EasyMock.createMock(ApiProxy.Delegate.class);
      setDelegateForThread(delegate);
      EasyMock.expect(delegate.makeAsyncCall(EasyMock.isA(ApiProxy.Environment.class),
                                EasyMock.eq(LocalDatastoreService.PACKAGE),
                                EasyMock.eq("RunQuery"),
                                FailoverMsMatcher.eqFailoverMs(null),
                                ApiConfigMatcher.eqApiConfig(config))).andReturn(null);
      EasyMock.replay(delegate);
      q = em.createQuery("select from " + Book.class.getName() + " c");
      q.setHint("datanucleus.appengine.datastoreReadConsistency", "STRONG");
      q.getResultList();
      EasyMock.verify(delegate);

      delegate = EasyMock.createMock(ApiProxy.Delegate.class);
      setDelegateForThread(delegate);
      EasyMock.expect(delegate.makeAsyncCall(EasyMock.isA(ApiProxy.Environment.class),
                                EasyMock.eq(LocalDatastoreService.PACKAGE),
                                EasyMock.eq("RunQuery"),
                                FailoverMsMatcher.eqFailoverMs(-1L),
                                ApiConfigMatcher.eqApiConfig(config))).andReturn(null);
      EasyMock.replay(delegate);
      q = em.createQuery("select from " + Book.class.getName() + " c");
      q.setHint("datanucleus.appengine.datastoreReadConsistency", "EVENTUAL");
      q.getResultList();
      EasyMock.verify(delegate);
    } finally {
      setDelegateForThread(original);
    }
  }

  public void testSetChunkSize() {
    DatastoreServiceFactoryInternal.setDatastoreService(null);
    ApiProxy.Delegate original = getDelegateForThread();
    Future<DatastorePb.QueryResult> result = EasyMock.createNiceMock(Future.class);
    try {
      ApiProxy.Delegate delegate = EasyMock.createMock(ApiProxy.Delegate.class);
      setDelegateForThread(delegate);
      ApiProxy.ApiConfig config = new ApiProxy.ApiConfig();
      EasyMock.expect(delegate.makeAsyncCall(EasyMock.isA(ApiProxy.Environment.class),
                                EasyMock.eq(LocalDatastoreService.PACKAGE),
                                EasyMock.eq("RunQuery"),
                                ChunkMatcher.eqChunkSize(33),
                                ApiConfigMatcher.eqApiConfig(config))).andReturn(result);
      EasyMock.replay(delegate, result);
      Query q = em.createQuery("select from " + Flight.class.getName() + " c");
      q.setHint("datanucleus.query.fetchSize", 33);
      q.getResultList();
      EasyMock.verify(delegate);
    } finally {
      setDelegateForThread(original);
    }
  }


  private void assertQueryUnsupportedByDatastore(String query, Class<?> expectedCauseClass) {
    Query q = em.createQuery(query);
    try {
      q.getResultList().iterator();
      fail("expected PersistenceException for query <" + query + ">");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause().getClass().getName() + ": " + e.getCause().getMessage(),
                 expectedCauseClass.isAssignableFrom(e.getCause().getClass()));
    }
  }

  private void assertQueryUnsupportedByOrm(String query,
                                           Expression.Operator unsupportedOp) {
    Query q = em.createQuery(query);
    try {
      q.getResultList();
      fail("expected PersistenceException->UnsupportedOperationException for query <" + query + ">");
    } catch (PersistenceException e) {
      Throwable cause = e.getCause();
      if (cause instanceof DatastoreQuery.UnsupportedDatastoreOperatorException) {
        // Good. Expression.Operator doesn't override equals
        // so we just compare the string representation. Operator case-insensitive
        assertEquals(unsupportedOp.toString().toLowerCase(), ((DatastoreQuery.UnsupportedDatastoreOperatorException)cause).getOperation().toString().toLowerCase());
      }
      else {
        throw e;
      }
    }
  }

  private void assertQueryUnsupportedByOrm(String query,
                                           Expression.Operator unsupportedOp,
                                           Set<Expression.Operator> unsupportedOps) {
    assertQueryUnsupportedByOrm(query, unsupportedOp);
    unsupportedOps.remove(unsupportedOp);
  }

  private void assertQueryRequiresUnsupportedDatastoreFeature(String query) {
    Query q = em.createQuery(query);
    try {
      q.getResultList();
      fail("expected UnsupportedDatastoreFeatureException for query <" + query + ">");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
          // good
        }
        else {
          throw pe;
        }
    }
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

    assertFilterPredicatesEqual(addedFilters, getFilterPredicates(q));
    assertEquals(addedSorts, getSortPredicates(q));
  }

  // TODO(maxr): Get rid of this when we've fixed the npe in FilterPredicate.equals().
  private static void assertFilterPredicatesEqual(
      List<FilterPredicate> expected, List<FilterPredicate> actual) {
    List<FilterPredicate> expected2 = Utils.newArrayList();
    for (FilterPredicate fp : expected) {
      if (fp.getValue() == null) {
        expected2.add(new FilterPredicate(fp.getPropertyName(), fp.getOperator(), "____null"));
      } else {
        expected2.add(fp);
      }
    }
    List<FilterPredicate> actual2 = Utils.newArrayList();
    for (FilterPredicate fp : actual) {
      if (fp.getValue() == null) {
        actual2.add(new FilterPredicate(fp.getPropertyName(), fp.getOperator(), "____null"));
      } else {
        actual2.add(fp);
      }
    }
    assertEquals(expected2, actual2);
  }
  private DatastoreQuery getDatastoreQuery(javax.persistence.Query q) {
    return ((JPQLQuery) ((JPAQuery) q).getInternalQuery()).getDatastoreQuery();
  }

  private List<FilterPredicate> getFilterPredicates(javax.persistence.Query q) {
    return getDatastoreQuery(q).getLatestDatastoreQuery().getFilterPredicates();
  }

  private List<SortPredicate> getSortPredicates(javax.persistence.Query q) {
    return getDatastoreQuery(q).getLatestDatastoreQuery().getSortPredicates();
  }
}
