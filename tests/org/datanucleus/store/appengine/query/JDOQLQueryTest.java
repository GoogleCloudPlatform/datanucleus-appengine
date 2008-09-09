// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.query;

import com.google.apphosting.api.datastore.Entity;
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
  private static final SortPredicate ORIG_ASC = new SortPredicate("origin", SortDirection.ASCENDING);
  private static final SortPredicate DESC_DESC = new SortPredicate("dest", SortDirection.DESCENDING);

  public void testUnsupportedFilters() {
    assertQueryUnsupported("select from " + Flight.class.getName()
        + " where origin == 2 group by dest", DatastoreQuery.GROUP_BY_OP);
    // can't actually test having because the parser doesn't recognize it unless there is a
    // group by, and the group by gets seen first
    assertQueryUnsupported("select from " + Flight.class.getName()
        + " where origin == 2 group by dest having dest == 2", DatastoreQuery.GROUP_BY_OP);
    Set<Expression.Operator> unsupportedOps = Sets.newHashSet(DatastoreQuery.UNSUPPORTED_OPERATORS);
    assertQueryUnsupported(Flight.class,
        "origin == 2 || dest == 3", Expression.OP_OR, unsupportedOps);
    assertQueryUnsupported(Flight.class, "!origin", Expression.OP_NOT, unsupportedOps);
    assertQueryUnsupported(Flight.class, "(origin + dest) == 4", Expression.OP_ADD, unsupportedOps);
    assertQueryUnsupported(Flight.class, "origin + dest == 4", Expression.OP_ADD, unsupportedOps);
    assertQueryUnsupported(Flight.class, "(origin - dest) == 4", Expression.OP_SUB, unsupportedOps);
    assertQueryUnsupported(Flight.class, "origin - dest == 4", Expression.OP_SUB, unsupportedOps);
    assertQueryUnsupported(Flight.class, "(origin / dest) == 4", Expression.OP_DIV, unsupportedOps);
    assertQueryUnsupported(Flight.class, "origin / dest == 4", Expression.OP_DIV, unsupportedOps);
    assertQueryUnsupported(Flight.class, "(origin * dest) == 4", Expression.OP_MUL, unsupportedOps);
    assertQueryUnsupported(Flight.class, "origin * dest == 4", Expression.OP_MUL, unsupportedOps);
    assertQueryUnsupported(Flight.class, "(origin % dest) == 4", Expression.OP_MOD, unsupportedOps);
    assertQueryUnsupported(Flight.class, "origin % dest == 4", Expression.OP_MOD, unsupportedOps);
    assertQueryUnsupported(Flight.class, "origin | dest == 4", Expression.OP_OR, unsupportedOps);
    assertQueryUnsupported(Flight.class, "~origin == 4", Expression.OP_COM, unsupportedOps);
    assertQueryUnsupported(Flight.class, "!origin == 4", Expression.OP_NOT, unsupportedOps);
    assertQueryUnsupported(Flight.class, "-origin == 4", Expression.OP_NEG, unsupportedOps);
    assertQueryUnsupported(Flight.class, "origin instanceof " + Flight.class.getName(),
        Expression.OP_IS, unsupportedOps);
    assertEquals(Sets.newHashSet(Expression.OP_CONCAT, Expression.OP_LIKE,
        Expression.OP_BETWEEN, Expression.OP_ISNOT), unsupportedOps);
  }

  public void testSupportedFilters() {
    assertQuerySupported(Flight.class, "", NO_FILTERS, NO_SORTS);
    assertQuerySupported(Flight.class, "origin == 2", Lists.newArrayList(ORIGIN_EQ_2), NO_SORTS);
    assertQuerySupported(Flight.class, "origin == \"2\"", Lists.newArrayList(ORIGIN_EQ_2STR), NO_SORTS);
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
    assertQuerySupported(Flight.class, "(origin > 2 && dest < 4)", Lists.newArrayList(ORIG_GT_2,
        DEST_LT_4), NO_SORTS);

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
    assertQuerySupported("select from " + Flight.class.getName()
        + " where origin == two parameters String two",
        Lists.newArrayList(ORIGIN_EQ_2STR), NO_SORTS, "2");
    assertQuerySupported("select from " + Flight.class.getName()
        + " where origin == two && dest == four parameters int two, int four",
        Lists.newArrayList(ORIGIN_EQ_2, DEST_EQ_4), NO_SORTS, 2L, 4L);
    assertQuerySupported("select from " + Flight.class.getName()
        + " where origin == two && dest == four parameters int two, int four "
        + "order by origin asc, dest desc",
        Lists.newArrayList(ORIGIN_EQ_2, DEST_EQ_4),
        Lists.newArrayList(ORIG_ASC, DESC_DESC), 2L, 4L);
  }

  public void test2Equals2OrderBy() {
    ldth.ds.put(newFlight("1", "yam", "bam", 1, 2));
    ldth.ds.put(newFlight("2", "yam", "bam", 1, 1));
    ldth.ds.put(newFlight("3", "yam", "bam", 2, 1));
    ldth.ds.put(newFlight("4", "yam", "bam", 2, 2));
    ldth.ds.put(newFlight("5", "notyam", "bam", 2, 2));
    ldth.ds.put(newFlight("5", "yam", "notbam", 2, 2));
    Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where origin == \"yam\" && dest == \"bam\""
            + " order by you asc, me desc");
    List<Flight> result = (List<Flight>) q.execute();
    assertEquals(4, result.size());

    assertEquals("1", result.get(0).getName());
    assertEquals("2", result.get(1).getName());
    assertEquals("4", result.get(2).getName());
    assertEquals("3", result.get(3).getName());
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

  private static Entity newFlight(String name, String origin, String dest,
      int you, int me) {
    Entity e = new Entity(Flight.class.getName());
    e.setProperty("name", name);
    e.setProperty("origin", origin);
    e.setProperty("dest", dest);
    e.setProperty("you", you);
    e.setProperty("me", me);
    return e;
  }

  private void assertQueryUnsupported(
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

  private void assertQueryUnsupported(String query, Expression.Operator unsupportedOp) {
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
    assertEquals(addedFilters, ((JDOQLQuery)((JDOQuery)q).getInternalQuery()).getDatastoreQuery().getMostRecentDatastoreQuery().getFilterPredicates());
    assertEquals(addedSorts, ((JDOQLQuery)((JDOQuery)q).getInternalQuery()).getDatastoreQuery().getMostRecentDatastoreQuery().getSortPredicates());
  }

}
