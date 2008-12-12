// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.query;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Query.FilterOperator;
import com.google.apphosting.api.datastore.Query.FilterPredicate;
import com.google.apphosting.api.datastore.Query.SortDirection;
import com.google.apphosting.api.datastore.Query.SortPredicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.datanucleus.jdo.JDOQuery;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.appengine.JDOTestCase;
import org.datanucleus.test.Flight;
import static org.datanucleus.test.Flight.newFlightEntity;
import org.datanucleus.test.HasAncestorJDO;
import org.datanucleus.test.HasKeyAncestorKeyStringPkJDO;
import org.datanucleus.test.HasKeyPkJDO;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jdo.Query;


/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOQLQueryTest extends JDOTestCase {

  private static final List<SortPredicate> NO_SORTS = Collections.emptyList();
  private static final List<FilterPredicate> NO_FILTERS = Collections.emptyList();

  private static final FilterPredicate ORIGIN_EQ_2 =
      new FilterPredicate("origin", FilterOperator.EQUAL, 2L);
  private static final FilterPredicate ORIGIN_EQ_2STR =
      new FilterPredicate("origin", FilterOperator.EQUAL, "2");
  private static final FilterPredicate DEST_EQ_4 =
      new FilterPredicate("dest", FilterOperator.EQUAL, 4L);
  private static final FilterPredicate ORIG_GT_2 =
      new FilterPredicate("origin", FilterOperator.GREATER_THAN, 2L);
  private static final FilterPredicate ORIG_GTE_2 =
      new FilterPredicate("origin", FilterOperator.GREATER_THAN_OR_EQUAL, 2L);
  private static final FilterPredicate DEST_LT_4 =
      new FilterPredicate("dest", FilterOperator.LESS_THAN, 4L);
  private static final FilterPredicate DEST_LTE_4 =
      new FilterPredicate("dest", FilterOperator.LESS_THAN_OR_EQUAL, 4L);
  private static final SortPredicate ORIG_ASC =
      new SortPredicate("origin", SortDirection.ASCENDING);
  private static final SortPredicate DESC_DESC =
      new SortPredicate("dest", SortDirection.DESCENDING);

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    beginTxn();
  }

  @Override
  protected void tearDown() throws Exception {
    commitTxn();
    super.tearDown();
  }

  public void testUnsupportedFilters() {
    assertQueryUnsupportedByOrm("select from " + Flight.class.getName()
        + " where origin == 2 group by dest", DatastoreQuery.GROUP_BY_OP);
    // can't actually test having because the parser doesn't recognize it unless there is a
    // group by, and the group by gets seen first
    assertQueryUnsupportedByOrm("select from " + Flight.class.getName()
        + " where origin == 2 group by dest having dest == 2", DatastoreQuery.GROUP_BY_OP);
    Set<Expression.Operator> unsupportedOps = Sets.newHashSet(DatastoreQuery.UNSUPPORTED_OPERATORS);
    assertQueryUnsupportedByOrm(Flight.class,
        "origin == 2 || dest == 3", Expression.OP_OR, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "!origin", Expression.OP_NOT, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "(origin + dest) == 4", Expression.OP_ADD, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "origin + dest == 4", Expression.OP_ADD, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "(origin - dest) == 4", Expression.OP_SUB, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "origin - dest == 4", Expression.OP_SUB, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "(origin / dest) == 4", Expression.OP_DIV, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "origin / dest == 4", Expression.OP_DIV, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "(origin * dest) == 4", Expression.OP_MUL, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "origin * dest == 4", Expression.OP_MUL, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "(origin % dest) == 4", Expression.OP_MOD, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "origin % dest == 4", Expression.OP_MOD, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "origin | dest == 4", Expression.OP_OR, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "~origin == 4", Expression.OP_COM, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "!origin == 4", Expression.OP_NOT, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "-origin == 4", Expression.OP_NEG, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "origin instanceof " + Flight.class.getName(),
        Expression.OP_IS, unsupportedOps);
    assertEquals(Sets.<Expression.Operator>newHashSet(Expression.OP_CONCAT, Expression.OP_LIKE,
        Expression.OP_BETWEEN, Expression.OP_ISNOT), unsupportedOps);
    // multiple inequality filters
    assertQueryUnsupportedByDatastore("select from " + Flight.class.getName()
        + " where (origin > 2 && dest < 4)");
    // inequality filter prop is not the same as the first order by prop
    assertQueryUnsupportedByDatastore("select from " + Flight.class.getName()
        + " where origin > 2 order by dest");
  }

  public void testSupportedFilters() {
    assertQuerySupported(Flight.class, "", NO_FILTERS, NO_SORTS);
    assertQuerySupported(Flight.class, "origin == 2", Lists.newArrayList(ORIGIN_EQ_2), NO_SORTS);
    assertQuerySupported(
        Flight.class, "origin == \"2\"", Lists.newArrayList(ORIGIN_EQ_2STR), NO_SORTS);
    assertQuerySupported(Flight.class, "(origin == 2)", Lists.newArrayList(ORIGIN_EQ_2), NO_SORTS);
    assertQuerySupported(Flight.class, "origin == 2 && dest == 4", Lists.newArrayList(ORIGIN_EQ_2,
        DEST_EQ_4), NO_SORTS);
    assertQuerySupported(Flight.class, "(origin == 2 && dest == 4)", Lists.newArrayList(ORIGIN_EQ_2,
        DEST_EQ_4), NO_SORTS);
    assertQuerySupported(Flight.class, "(origin == 2) && (dest == 4)", Lists.newArrayList(
        ORIGIN_EQ_2, DEST_EQ_4), NO_SORTS);

    assertQuerySupported(Flight.class, "origin > 2", Lists.newArrayList(ORIG_GT_2), NO_SORTS);
    assertQuerySupported(Flight.class, "origin >= 2", Lists.newArrayList(ORIG_GTE_2), NO_SORTS);
    assertQuerySupported(Flight.class, "dest < 4", Lists.newArrayList(DEST_LT_4), NO_SORTS);
    assertQuerySupported(Flight.class, "dest <= 4", Lists.newArrayList(DEST_LTE_4), NO_SORTS);

    assertQuerySupported("select from " + Flight.class.getName() + " order by origin asc",
        NO_FILTERS, Lists.newArrayList(ORIG_ASC));
    assertQuerySupported("select from " + Flight.class.getName() + " order by dest desc",
        NO_FILTERS, Lists.newArrayList(DESC_DESC));
    assertQuerySupported("select from " + Flight.class.getName()
        + " order by origin asc, dest desc", NO_FILTERS, Lists.newArrayList(ORIG_ASC, DESC_DESC));

    assertQuerySupported("select from " + Flight.class.getName()
        + " where origin == 2 && dest == 4 order by origin asc, dest desc",
        Lists.newArrayList(ORIGIN_EQ_2, DEST_EQ_4), Lists.newArrayList(ORIG_ASC, DESC_DESC));
  }

  public void testBindVariables() {
    String queryStr = "select from " + Flight.class.getName() + " where origin == two ";
    assertQuerySupported(queryStr + " parameters String two",
        Lists.newArrayList(ORIGIN_EQ_2STR), NO_SORTS, "2");
    assertQuerySupportedWithExplicitParams(queryStr,
        Lists.newArrayList(ORIGIN_EQ_2STR), NO_SORTS, "String two", "2");

    queryStr = "select from " + Flight.class.getName() + " where origin == two && dest == four ";
    assertQuerySupported(queryStr + "parameters int two, int four",
        Lists.newArrayList(ORIGIN_EQ_2, DEST_EQ_4), NO_SORTS, 2L, 4L);
    assertQuerySupportedWithExplicitParams(queryStr,
        Lists.newArrayList(ORIGIN_EQ_2, DEST_EQ_4), NO_SORTS, "int two, int four", 2L, 4L);

    queryStr = "select from " + Flight.class.getName() + " where origin == two && dest == four ";
    String orderStr = "order by origin asc, dest desc";
    assertQuerySupported(queryStr + "parameters int two, int four " + orderStr,
        Lists.newArrayList(ORIGIN_EQ_2, DEST_EQ_4),
        Lists.newArrayList(ORIG_ASC, DESC_DESC), 2L, 4L);
    assertQuerySupportedWithExplicitParams(queryStr + orderStr,
        Lists.newArrayList(ORIGIN_EQ_2, DEST_EQ_4),
        Lists.newArrayList(ORIG_ASC, DESC_DESC), "int two, int four", 2L, 4L);
  }

  public void test2Equals2OrderBy() {
    ldth.ds.put(newFlightEntity("1", "yam", "bam", 1, 2));
    ldth.ds.put(newFlightEntity("2", "yam", "bam", 1, 1));
    ldth.ds.put(newFlightEntity("3", "yam", "bam", 2, 1));
    ldth.ds.put(newFlightEntity("4", "yam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "notyam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "yam", "notbam", 2, 2));
    Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where origin == \"yam\" && dest == \"bam\""
            + " order by you asc, me desc");
    @SuppressWarnings("unchecked")
    List<Flight> result = (List<Flight>) q.execute();
    assertEquals(4, result.size());

    assertEquals("1", result.get(0).getName());
    assertEquals("2", result.get(1).getName());
    assertEquals("4", result.get(2).getName());
    assertEquals("3", result.get(3).getName());
  }

  public void testDefaultOrderingIsAsc() {
    ldth.ds.put(newFlightEntity("1", "yam", "bam", 1, 2));
    ldth.ds.put(newFlightEntity("2", "yam", "bam", 1, 1));
    ldth.ds.put(newFlightEntity("3", "yam", "bam", 2, 1));
    ldth.ds.put(newFlightEntity("4", "yam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "notyam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "yam", "notbam", 2, 2));
    Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where origin == \"yam\" && dest == \"bam\""
            + " order by you");
    @SuppressWarnings("unchecked")
    List<Flight> result = (List<Flight>) q.execute();
    assertEquals(4, result.size());

    assertEquals("1", result.get(0).getName());
    assertEquals("2", result.get(1).getName());
    assertEquals("3", result.get(2).getName());
    assertEquals("4", result.get(3).getName());
  }

  public void testLimitQuery() {
    ldth.ds.put(newFlightEntity("1", "yam", "bam", 1, 2));
    ldth.ds.put(newFlightEntity("2", "yam", "bam", 1, 1));
    ldth.ds.put(newFlightEntity("3", "yam", "bam", 2, 1));
    ldth.ds.put(newFlightEntity("4", "yam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "notyam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "yam", "notbam", 2, 2));
    Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where origin == \"yam\" && dest == \"bam\""
            + " order by you asc, me desc");

    q.setRange(Long.MAX_VALUE, 1);
    @SuppressWarnings("unchecked")
    List<Flight> result1 = (List<Flight>) q.execute();
    assertEquals(1, result1.size());
    assertEquals("1", result1.get(0).getName());

    q.setRange(Long.MAX_VALUE, Long.MAX_VALUE);
    @SuppressWarnings("unchecked")
    List<Flight> result2 = (List<Flight>) q.execute();
    assertEquals(4, result2.size());
    assertEquals("1", result2.get(0).getName());

    q.setRange(Long.MAX_VALUE, 0);
    @SuppressWarnings("unchecked")
    List<Flight> result3 = (List<Flight>) q.execute();
    assertEquals(0, result3.size());

    q.setRange(Long.MAX_VALUE, -1);
    try {
      q.execute();
      fail("expected iae");
    } catch (IllegalArgumentException iae) {
      // good
    }
  }

  public void testOffsetQuery() {
    ldth.ds.put(newFlightEntity("1", "yam", "bam", 1, 2));
    ldth.ds.put(newFlightEntity("2", "yam", "bam", 1, 1));
    ldth.ds.put(newFlightEntity("3", "yam", "bam", 2, 1));
    ldth.ds.put(newFlightEntity("4", "yam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "notyam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "yam", "notbam", 2, 2));
    Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where origin == \"yam\" && dest == \"bam\""
            + " order by you asc, me desc");

    q.setRange(0, Long.MAX_VALUE);
    @SuppressWarnings("unchecked")
    List<Flight> result1 = (List<Flight>) q.execute();
    assertEquals(4, result1.size());
    assertEquals("1", result1.get(0).getName());

    q.setRange(1, Long.MAX_VALUE);
    @SuppressWarnings("unchecked")
    List<Flight> result2 = (List<Flight>) q.execute();
    assertEquals(3, result2.size());
    assertEquals("2", result2.get(0).getName());

    q.setRange(Long.MAX_VALUE, Long.MAX_VALUE);
    @SuppressWarnings("unchecked")
    List<Flight> result3 = (List<Flight>) q.execute();
    assertEquals(4, result3.size());
    assertEquals("1", result3.get(0).getName());

    q.setRange(-1, Long.MAX_VALUE);
    try {
      q.execute();
      fail("expected iae");
    } catch (IllegalArgumentException iae) {
      // good
    }
  }

  public void testOffsetLimitQuery() {
    ldth.ds.put(newFlightEntity("1", "yam", "bam", 1, 2));
    ldth.ds.put(newFlightEntity("2", "yam", "bam", 1, 1));
    ldth.ds.put(newFlightEntity("3", "yam", "bam", 2, 1));
    ldth.ds.put(newFlightEntity("4", "yam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "notyam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "yam", "notbam", 2, 2));
    Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where origin == \"yam\" && dest == \"bam\""
            + " order by you asc, me desc");

    q.setRange(0, 0);
    @SuppressWarnings("unchecked")
    List<Flight> result1 = (List<Flight>) q.execute();
    assertEquals(0, result1.size());

    q.setRange(1, 0);
    @SuppressWarnings("unchecked")
    List<Flight> result2 = (List<Flight>) q.execute();
    assertEquals(0, result2.size());

    q.setRange(0, 1);
    @SuppressWarnings("unchecked")
    List<Flight> result3 = (List<Flight>) q.execute();
    assertEquals(1, result3.size());

    q.setRange(0, 2);
    @SuppressWarnings("unchecked")
    List<Flight> result4 = (List<Flight>) q.execute();
    assertEquals(2, result4.size());
    assertEquals("1", result4.get(0).getName());

    q.setRange(1, 2);
    @SuppressWarnings("unchecked")
    List<Flight> result5 = (List<Flight>) q.execute();
    assertEquals(1, result5.size());
    assertEquals("2", result5.get(0).getName());

    q.setRange(2, 5);
    @SuppressWarnings("unchecked")
    List<Flight> result6 = (List<Flight>) q.execute();
    assertEquals(2, result6.size());
    assertEquals("4", result6.get(0).getName());

    q.setRange(2, 2);
    @SuppressWarnings("unchecked")
    List<Flight> result7 = (List<Flight>) q.execute();
    assertEquals(0, result7.size());

    q.setRange(2, 1);
    @SuppressWarnings("unchecked")
    List<Flight> result8 = (List<Flight>) q.execute();
    assertEquals(0, result8.size());
  }

  public void testSerialization() throws IOException {
    Query q = pm.newQuery("select from " + Flight.class.getName());
    q.execute();

    JDOQLQuery innerQuery = (JDOQLQuery)((JDOQuery)q).getInternalQuery();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    // the fact that this doesn't blow up is the test
    oos.writeObject(innerQuery);
  }

  public void testKeyQuery_StringPk() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);

    javax.jdo.Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where id == key parameters String key");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(KeyFactory.encodeKey(flightEntity.getKey()));
    assertEquals(1, flights.size());
    assertEquals(flightEntity.getKey(), KeyFactory.decodeKey(flights.get(0).getId()));
  }

  public void testKeyQuery_KeyPk() {
    Entity e = new Entity(HasKeyPkJDO.class.getSimpleName());
    ldth.ds.put(e);

    javax.jdo.Query q = pm.newQuery(
        "select from " + HasKeyPkJDO.class.getName() + " where key == mykey parameters String mykey");
    @SuppressWarnings("unchecked")
    List<HasKeyPkJDO> result = (List<HasKeyPkJDO>) q.execute(e.getKey());
    assertEquals(1, result.size());
    assertEquals(e.getKey(), result.get(0).getKey());
  }

  public void testKeyQueryWithSorts() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);

    javax.jdo.Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where id == key parameters String key order by id asc");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(KeyFactory.encodeKey(flightEntity.getKey()));
    assertEquals(1, flights.size());
    assertEquals(flightEntity.getKey(), KeyFactory.decodeKey(flights.get(0).getId()));
  }

  public void testKeyQuery_MultipleFilters() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);

    javax.jdo.Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where id == key && origin == \"yam\" parameters String key");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(KeyFactory.encodeKey(flightEntity.getKey()));
    assertEquals(1, flights.size());
    assertEquals(flightEntity.getKey(), KeyFactory.decodeKey(flights.get(0).getId()));
  }

  public void testKeyQuery_NonEqualityFilter() {
    Entity flightEntity1 = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity1);
    Entity flightEntity2 = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity2);

    javax.jdo.Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where id > key parameters String key");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(KeyFactory.encodeKey(flightEntity1.getKey()));
    assertEquals(1, flights.size());
    assertEquals(flightEntity2.getKey(), KeyFactory.decodeKey(flights.get(0).getId()));
  }

  public void testKeyQuery_SortByKey() {
    Entity flightEntity1 = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity1);

    Entity flightEntity2 = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity2);

    javax.jdo.Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where origin == 'yam' order by id DESC");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute();
    assertEquals(2, flights.size());
    assertEquals(flightEntity2.getKey(), KeyFactory.decodeKey(flights.get(0).getId()));
    assertEquals(flightEntity1.getKey(), KeyFactory.decodeKey(flights.get(1).getId()));
  }

  public void testAncestorQueryWithStringAncestor() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity = new Entity(HasAncestorJDO.class.getSimpleName(), flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    javax.jdo.Query q = pm.newQuery(
        "select from " + HasAncestorJDO.class.getName()
            + " where ancestorId == ancId parameters String ancId");
    @SuppressWarnings("unchecked")
    List<HasAncestorJDO> haList =
        (List<HasAncestorJDO>) q.execute(KeyFactory.encodeKey(flightEntity.getKey()));
    assertEquals(1, haList.size());
    assertEquals(flightEntity.getKey(), KeyFactory.decodeKey(haList.get(0).getAncestorId()));

    assertEquals(
        flightEntity.getKey(), getDatastoreQuery(q).getMostRecentDatastoreQuery().getAncestor());
    assertEquals(NO_FILTERS, getFilterPredicates(q));
    assertEquals(NO_SORTS, getSortPredicates(q));
  }

  public void testAncestorQueryWithKeyAncestor() {
    Entity e = new Entity("parent");
    ldth.ds.put(e);
    Entity childEntity = new Entity(HasKeyAncestorKeyStringPkJDO.class.getSimpleName(), e.getKey());
    ldth.ds.put(childEntity);

    javax.jdo.Query q = pm.newQuery(
        "select from " + HasKeyAncestorKeyStringPkJDO.class.getName()
            + " where ancestorKey == ancId parameters " + Key.class.getName() + " ancId");
    @SuppressWarnings("unchecked")
    List<HasKeyAncestorKeyStringPkJDO> result =
        (List<HasKeyAncestorKeyStringPkJDO>) q.execute(e.getKey());
    assertEquals(1, result.size());
    assertEquals(e.getKey(), result.get(0).getAncestorKey());
  }

  public void testIllegalAncestorQuery_BadOperator() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity = new Entity(HasAncestorJDO.class.getName(), flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    javax.jdo.Query q = pm.newQuery(
        "select from " + HasAncestorJDO.class.getName()
            + " where ancestorId > ancId parameters String ancId");
    try {
      q.execute(KeyFactory.encodeKey(flightEntity.getKey()));
      fail ("expected udfe");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }
  }

  public void testSortByFieldWithCustomColumn() {
    ldth.ds.put(newFlightEntity("1", "yam", "bam", 1, 2, 400));
    ldth.ds.put(newFlightEntity("2", "yam", "bam", 1, 1, 300));
    ldth.ds.put(newFlightEntity("3", "yam", "bam", 2, 1, 200));
    Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where origin == \"yam\" && dest == \"bam\""
            + " order by flightNumber asc");
    @SuppressWarnings("unchecked")
    List<Flight> result = (List<Flight>) q.execute();
    assertEquals(3, result.size());

    assertEquals("3", result.get(0).getName());
    assertEquals("2", result.get(1).getName());
    assertEquals("1", result.get(2).getName());
  }

  public void testIllegalAncestorQuery_SortByAncestor() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity = new Entity(HasAncestorJDO.class.getName(), flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    javax.jdo.Query q = pm.newQuery(
        "select from " + HasAncestorJDO.class.getName()
            + " where ancestorId == ancId parameters String ancId order by ancestorId ASC");
    try {
      q.execute(KeyFactory.encodeKey(flightEntity.getKey()));
      fail ("expected udfe");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }
  }

  private void assertQueryUnsupportedByOrm(
      Class<?> clazz, String query, Expression.Operator unsupportedOp,
      Set<Expression.Operator> unsupportedOps) {
    Query q = pm.newQuery(clazz, query);
    try {
      q.execute();
      fail("expected UnsupportedOperationException for query <" + query + ">");
    } catch (DatastoreQuery.UnsupportedDatastoreOperatorException uoe) {
      // good
      assertEquals(unsupportedOp, uoe.getOperation());
    }
    unsupportedOps.remove(unsupportedOp);
  }

  private void assertQueryUnsupportedByDatastore(String query) {
    Query q = pm.newQuery(query);
    try {
      q.execute();
      fail("expected IllegalArgumentException for query <" + query + ">");
    } catch (IllegalArgumentException iae) {
      // good
    }
  }

  private void assertQueryUnsupportedByOrm(String query, Expression.Operator unsupportedOp) {
    Query q = pm.newQuery(query);
    try {
      q.execute();
      fail("expected UnsupportedOperationException for query <" + query + ">");
    } catch (DatastoreQuery.UnsupportedDatastoreOperatorException uoe) {
      // good
      assertEquals(unsupportedOp, uoe.getOperation());
    }
  }

  private void assertQuerySupported(Class<?> clazz, String query,
      List<FilterPredicate> addedFilters, List<SortPredicate> addedSorts, Object... bindVariables) {
    Query q;
    if (query.isEmpty()) {
      q = pm.newQuery(clazz);
    } else {
      q = pm.newQuery(clazz, query);
    }
    assertQuerySupported(q, addedFilters, addedSorts, bindVariables);
  }

  private void assertQuerySupported(String query,
      List<FilterPredicate> addedFilters, List<SortPredicate> addedSorts, Object... bindVariables) {
    assertQuerySupported(pm.newQuery(query), addedFilters, addedSorts, bindVariables);
  }

  private void assertQuerySupported(Query q, List<FilterPredicate> addedFilters,
      List<SortPredicate> addedSorts, Object... bindVariables) {
    if (bindVariables.length == 0) {
      q.execute();
    } else if (bindVariables.length == 1) {
      q.execute(bindVariables[0]);
    } else if (bindVariables.length == 2) {
      q.execute(bindVariables[0], bindVariables[1]);
    }
    assertEquals(addedFilters, getFilterPredicates(q));
    assertEquals(addedSorts, getSortPredicates(q));
  }

  private DatastoreQuery getDatastoreQuery(javax.jdo.Query q) {
    return ((JDOQLQuery)((JDOQuery)q).getInternalQuery()).getDatastoreQuery();
}

  private List<FilterPredicate> getFilterPredicates(javax.jdo.Query q) {
    return getDatastoreQuery(q).getMostRecentDatastoreQuery().getFilterPredicates();
  }

  private List<SortPredicate> getSortPredicates(javax.jdo.Query q) {
    return getDatastoreQuery(q).getMostRecentDatastoreQuery().getSortPredicates();
  }

  private void assertQuerySupportedWithExplicitParams(String query,
      List<FilterPredicate> addedFilters, List<SortPredicate> addedSorts, String explicitParams,
      Object... bindVariables) {
    javax.jdo.Query q = pm.newQuery(query);
    q.declareParameters(explicitParams);
    if (bindVariables.length == 0) {
      q.execute();
    } else if (bindVariables.length == 1) {
      q.execute(bindVariables[0]);
    } else if (bindVariables.length == 2) {
      q.execute(bindVariables[0], bindVariables[1]);
    }
    assertEquals(addedFilters, getFilterPredicates(q));
    assertEquals(addedSorts, getSortPredicates(q));
  }
}
