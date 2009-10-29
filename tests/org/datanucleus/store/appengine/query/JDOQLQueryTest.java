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
package org.datanucleus.store.appengine.query;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.Query.SortPredicate;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.users.User;
import com.google.apphosting.api.ApiProxy;

import org.datanucleus.ObjectManager;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.jdo.JDOPersistenceManager;
import org.datanucleus.jdo.JDOQuery;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.appengine.ExceptionThrowingDatastoreDelegate;
import org.datanucleus.store.appengine.JDOTestCase;
import org.datanucleus.store.appengine.PrimitiveArrays;
import org.datanucleus.store.appengine.TestUtils;
import org.datanucleus.store.appengine.Utils;
import org.datanucleus.store.appengine.WriteBlocker;
import org.datanucleus.test.BidirectionalChildListJDO;
import org.datanucleus.test.BidirectionalChildLongPkListJDO;
import org.datanucleus.test.BidirectionalGrandchildListJDO;
import org.datanucleus.test.Flight;
import static org.datanucleus.test.Flight.newFlightEntity;
import org.datanucleus.test.HasBytesJDO;
import org.datanucleus.test.HasEmbeddedJDO;
import org.datanucleus.test.HasEncodedStringPkJDO;
import org.datanucleus.test.HasEnumJDO;
import org.datanucleus.test.HasKeyAncestorKeyPkJDO;
import org.datanucleus.test.HasKeyAncestorStringPkJDO;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasLongPkJDO;
import org.datanucleus.test.HasMultiValuePropsJDO;
import org.datanucleus.test.HasOneToManyKeyPkListJDO;
import org.datanucleus.test.HasOneToManyKeyPkSetJDO;
import org.datanucleus.test.HasOneToManyListJDO;
import org.datanucleus.test.HasOneToManyLongPkListJDO;
import org.datanucleus.test.HasOneToManyLongPkSetJDO;
import org.datanucleus.test.HasOneToManyUnencodedStringPkListJDO;
import org.datanucleus.test.HasOneToManyUnencodedStringPkSetJDO;
import org.datanucleus.test.HasOneToOneJDO;
import org.datanucleus.test.HasOneToOneParentJDO;
import org.datanucleus.test.HasStringAncestorStringPkJDO;
import org.datanucleus.test.HasUnencodedStringPkJDO;
import org.datanucleus.test.KitchenSink;
import org.datanucleus.test.NullDataJDO;
import org.datanucleus.test.Person;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.Extent;
import javax.jdo.JDOException;
import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOUserException;
import javax.jdo.Query;


/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOQLQueryTest extends JDOTestCase {

  private static final List<SortPredicate> NO_SORTS = Collections.emptyList();
  private static final List<FilterPredicate> NO_FILTERS = Collections.emptyList();

  private static final FilterPredicate ORIGIN_EQ_2 =
      new FilterPredicate("origin", FilterOperator.EQUAL, 2);
  private static final FilterPredicate ORIGIN_EQ_2_LITERAL =
      new FilterPredicate("origin", FilterOperator.EQUAL, 2L);
  private static final FilterPredicate ORIGIN_NEQ_NULL_LITERAL =
      new FilterPredicate("origin", FilterOperator.NOT_EQUAL, null);
  private static final FilterPredicate ORIGIN_EQ_2STR =
      new FilterPredicate("origin", FilterOperator.EQUAL, "2");
  private static final FilterPredicate ORIGIN_NEQ_2_LITERAL =
      new FilterPredicate("origin", FilterOperator.NOT_EQUAL, 2L);
  private static final FilterPredicate DEST_EQ_4_LITERAL =
      new FilterPredicate("dest", FilterOperator.EQUAL, 4L);
  private static final FilterPredicate ORIG_GT_2_LITERAL =
      new FilterPredicate("origin", FilterOperator.GREATER_THAN, 2L);
  private static final FilterPredicate ORIG_GTE_2_LITERAL =
      new FilterPredicate("origin", FilterOperator.GREATER_THAN_OR_EQUAL, 2L);
  private static final FilterPredicate DEST_LT_4_LITERAL =
      new FilterPredicate("dest", FilterOperator.LESS_THAN, 4L);
  private static final FilterPredicate DEST_LTE_4_LITERAL =
      new FilterPredicate("dest", FilterOperator.LESS_THAN_OR_EQUAL, 4L);
  private static final SortPredicate ORIG_ASC =
      new SortPredicate("origin", SortDirection.ASCENDING);
  private static final SortPredicate DESC_DESC =
      new SortPredicate("dest", SortDirection.DESCENDING);
  private static final FilterPredicate ORIGIN_IN_2_ARGS =
      new FilterPredicate("origin", FilterOperator.IN, Arrays.asList("2", 2L));
  private static final FilterPredicate ORIGIN_IN_3_ARGS =
      new FilterPredicate("origin", FilterOperator.IN, Arrays.asList("2", 2L, false));

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    WriteBlocker.installNoWritesDatastoreService();
    beginTxn();
  }

  @Override
  protected void tearDown() throws Exception {
    if (!pm.isClosed() && pm.currentTransaction().isActive()) {
      commitTxn();
    }
    try {
      super.tearDown();
    } finally {
      WriteBlocker.uninstallNoWritesDatastoreService();
    }
  }

  public void testUnsupportedFilters() {
    assertQueryUnsupportedByOrm("select from " + Flight.class.getName()
        + " where origin == 2 group by dest", DatastoreQuery.GROUP_BY_OP);
    // can't actually test having because the parser doesn't recognize it unless there is a
    // group by, and the group by gets seen first
    assertQueryUnsupportedByOrm("select from " + Flight.class.getName()
        + " where origin == 2 group by dest having dest == 2", DatastoreQuery.GROUP_BY_OP);
    assertQueryUnsupportedByOrm(
        "select avg(you) from " + Flight.class.getName(), new Expression.Operator("avg", 0));
    Set<Expression.Operator> unsupportedOps = Utils.newHashSet(DatastoreQuery.UNSUPPORTED_OPERATORS);
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
    assertQueryUnsupportedByOrm(Flight.class, "~origin == 4", Expression.OP_COM, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "!origin == 4", Expression.OP_NOT, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "-origin == 4", Expression.OP_NEG, unsupportedOps);
    assertQueryUnsupportedByOrm(Flight.class, "origin instanceof " + Flight.class.getName(),
        Expression.OP_IS, unsupportedOps);
    assertEquals(Utils.<Expression.Operator>newHashSet(Expression.OP_CONCAT, Expression.OP_LIKE,
        Expression.OP_ISNOT), unsupportedOps);
    String baseQuery = "select from " + Flight.class.getName() + " where ";
    // multiple inequality filters
    // TODO(maxr) Make this pass against the real datastore.
    // We need to have it return BadRequest instead of NeedIndex for that to
    // happen.
    assertQueryUnsupportedByDatastore(baseQuery + "(origin > 2 && dest < 4)");
    // inequality filter prop is not the same as the first order by prop
    assertQueryUnsupportedByDatastore(baseQuery + "origin > 2 order by dest");
    // gets split into multiple inequality filters
    assertQueryUnsupportedByDatastore(baseQuery + "origin != 2 && dest != 4");

    // can't have 'or' on multiple properties
    assertQueryRequiresUnsupportedDatastoreFeature(baseQuery + "title == 'yar' || author == null");
    assertQueryRequiresUnsupportedDatastoreFeature(baseQuery + "isbn == 4 && (title == 'yar' || author == 'yam')");
    assertQueryRequiresUnsupportedDatastoreFeature(baseQuery + ":p1.contains(title) || author == 'yam'");
    // can only check equality
    assertQueryRequiresUnsupportedDatastoreFeature(baseQuery + "title > 5 || title < 2");
  }

  private void assertQueryRequiresUnsupportedDatastoreFeature(String query) {
    Query q = pm.newQuery(query);
    try {
      q.execute();
      fail("expected UnsupportedDatastoreFeatureException for query <" + query + ">");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // Good.
    }
  }

  public void testSupportedFilters() {
    assertQuerySupported(Flight.class, "", NO_FILTERS, NO_SORTS);
    assertQuerySupported(Flight.class, "origin == 2", Utils.newArrayList(ORIGIN_EQ_2_LITERAL), NO_SORTS);
    assertQuerySupported(
        Flight.class, "origin == \"2\"", Utils.newArrayList(ORIGIN_EQ_2STR), NO_SORTS);
    assertQuerySupported(Flight.class, "(origin == 2)", Utils.newArrayList(ORIGIN_EQ_2_LITERAL), NO_SORTS);
    assertQuerySupported(Flight.class, "origin == 2 && dest == 4", Utils.newArrayList(ORIGIN_EQ_2_LITERAL,
        DEST_EQ_4_LITERAL), NO_SORTS);
    assertQuerySupported(Flight.class, "(origin == 2 && dest == 4)", Utils.newArrayList(ORIGIN_EQ_2_LITERAL,
        DEST_EQ_4_LITERAL), NO_SORTS);
    assertQuerySupported(Flight.class, "(origin == 2) && (dest == 4)", Utils.newArrayList(
        ORIGIN_EQ_2_LITERAL, DEST_EQ_4_LITERAL), NO_SORTS);

    assertQuerySupported(Flight.class, "origin > 2", Utils.newArrayList(ORIG_GT_2_LITERAL), NO_SORTS);
    assertQuerySupported(Flight.class, "origin >= 2", Utils.newArrayList(ORIG_GTE_2_LITERAL), NO_SORTS);
    assertQuerySupported(Flight.class, "dest < 4", Utils.newArrayList(DEST_LT_4_LITERAL), NO_SORTS);
    assertQuerySupported(Flight.class, "dest <= 4", Utils.newArrayList(DEST_LTE_4_LITERAL), NO_SORTS);

    assertQuerySupported("select from " + Flight.class.getName() + " order by origin asc",
        NO_FILTERS, Utils.newArrayList(ORIG_ASC));
    assertQuerySupported("select from " + Flight.class.getName() + " order by dest desc",
        NO_FILTERS, Utils.newArrayList(DESC_DESC));
    assertQuerySupported("select from " + Flight.class.getName()
        + " order by origin asc, dest desc", NO_FILTERS, Utils.newArrayList(ORIG_ASC, DESC_DESC));

    assertQuerySupported("select from " + Flight.class.getName()
        + " where origin == 2 && dest == 4 order by origin asc, dest desc",
        Utils.newArrayList(ORIGIN_EQ_2_LITERAL, DEST_EQ_4_LITERAL), Utils.newArrayList(ORIG_ASC, DESC_DESC));
    assertQuerySupported(Flight.class, "origin != 2", Utils.newArrayList(ORIGIN_NEQ_2_LITERAL), NO_SORTS);
    assertQuerySupported("select from " + Flight.class.getName() + " where origin != null",
        Utils.newArrayList(ORIGIN_NEQ_NULL_LITERAL), NO_SORTS);
    assertQuerySupported(Flight.class, "origin == '2' || origin == 2",
                         Utils.newArrayList(ORIGIN_IN_2_ARGS), NO_SORTS);
    assertQuerySupported(Flight.class, "origin == '2' || origin == 2 || origin == false",
                         Utils.newArrayList(ORIGIN_IN_3_ARGS), NO_SORTS);
    assertQuerySupported(Flight.class, ":p1.contains(origin)",
                         Utils.newArrayList(ORIGIN_IN_2_ARGS), NO_SORTS, Arrays.asList("2", 2L));
    assertQuerySupported(Flight.class, ":p1.contains(origin)",
                         Utils.newArrayList(ORIGIN_IN_3_ARGS), NO_SORTS, Arrays.asList("2", 2L, false));
    assertQuerySupported(Flight.class, "(origin == '2' || origin == 2) && dest == 4",
                         Utils.newArrayList(DEST_EQ_4_LITERAL, ORIGIN_IN_2_ARGS), NO_SORTS);
    assertQuerySupported(Flight.class, ":p1.contains(origin) && dest == 4",
                         Utils.newArrayList(ORIGIN_IN_2_ARGS, DEST_EQ_4_LITERAL), NO_SORTS, Arrays.asList("2", 2L));
    assertQuerySupported(Flight.class, "(origin == '2' || origin == 2 || origin == false) && dest == 4",
                         Utils.newArrayList(DEST_EQ_4_LITERAL, ORIGIN_IN_3_ARGS), NO_SORTS);
    assertQuerySupported(Flight.class, ":p1.contains(origin) && dest == 4",
                         Utils.newArrayList(ORIGIN_IN_3_ARGS, DEST_EQ_4_LITERAL), NO_SORTS, Arrays.asList("2", 2L, false));
  }

  public void testBindVariables() {
    String queryStr = "select from " + Flight.class.getName() + " where origin == two ";
    assertQuerySupported(queryStr + " parameters String two",
        Utils.newArrayList(ORIGIN_EQ_2STR), NO_SORTS, "2");
    assertQuerySupportedWithExplicitParams(queryStr,
        Utils.newArrayList(ORIGIN_EQ_2STR), NO_SORTS, "String two", "2");

    queryStr = "select from " + Flight.class.getName() + " where origin == two && dest == four ";
    assertQuerySupported(queryStr + "parameters int two, int four",
        Utils.newArrayList(ORIGIN_EQ_2, DEST_EQ_4_LITERAL), NO_SORTS, 2, 4L);
    assertQuerySupportedWithExplicitParams(queryStr,
        Utils.newArrayList(ORIGIN_EQ_2, DEST_EQ_4_LITERAL), NO_SORTS, "int two, int four", 2, 4L);

    queryStr = "select from " + Flight.class.getName() + " where origin == two && dest == four ";
    String orderStr = "order by origin asc, dest desc";
    assertQuerySupported(queryStr + "parameters int two, int four " + orderStr,
        Utils.newArrayList(ORIGIN_EQ_2, DEST_EQ_4_LITERAL),
        Utils.newArrayList(ORIG_ASC, DESC_DESC), 2, 4L);
    assertQuerySupportedWithExplicitParams(queryStr + orderStr,
        Utils.newArrayList(ORIGIN_EQ_2, DEST_EQ_4_LITERAL),
        Utils.newArrayList(ORIG_ASC, DESC_DESC), "int two, int four", 2, 4L);
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

  public void testSetFilter() {
    ldth.ds.put(newFlightEntity("1", "yam", "bam", 1, 1));
    ldth.ds.put(newFlightEntity("2", "yam", "bam", 2, 2));
    Query q = pm.newQuery(
        "select from " + Flight.class.getName());
    q.setFilter("origin == \"yam\" && you == 2");
    @SuppressWarnings("unchecked")
    List<Flight> result = (List<Flight>) q.execute();
    assertEquals(1, result.size());
  }

  public void testSetInvalidFilter() {
    Query q = pm.newQuery(
        "select from " + Flight.class.getName());
    q.setFilter("origin == \"yam\" AND you == 2");
    try {
      q.execute();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
    }
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
    assertEquals("1", result3.get(0).getName());

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

  public void testOffsetLimitSingleStringQuery() {
    ldth.ds.put(newFlightEntity("1", "yam", "bam", 1, 2));
    ldth.ds.put(newFlightEntity("2", "yam", "bam", 1, 1));
    ldth.ds.put(newFlightEntity("3", "yam", "bam", 2, 1));
    ldth.ds.put(newFlightEntity("4", "yam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "notyam", "bam", 2, 2));
    ldth.ds.put(newFlightEntity("5", "yam", "notbam", 2, 2));
    String queryFormat =
        "select from " + Flight.class.getName()
            + " where origin == \"yam\" && dest == \"bam\""
            + " order by you asc, me desc range %d,%d";
    Query q = pm.newQuery(String.format(queryFormat, 0, 0));
    @SuppressWarnings("unchecked")
    List<Flight> result1 = (List<Flight>) q.execute();
    assertEquals(0, result1.size());

    q = pm.newQuery(String.format(queryFormat, 1, 0));
    @SuppressWarnings("unchecked")
    List<Flight> result2 = (List<Flight>) q.execute();
    assertEquals(0, result2.size());

    q = pm.newQuery(String.format(queryFormat, 0, 1));
    @SuppressWarnings("unchecked")
    List<Flight> result3 = (List<Flight>) q.execute();
    assertEquals(1, result3.size());

    q = pm.newQuery(String.format(queryFormat, 0, 2));
    @SuppressWarnings("unchecked")
    List<Flight> result4 = (List<Flight>) q.execute();
    assertEquals(2, result4.size());
    assertEquals("1", result4.get(0).getName());

    q = pm.newQuery(String.format(queryFormat, 1, 2));
    @SuppressWarnings("unchecked")
    List<Flight> result5 = (List<Flight>) q.execute();
    assertEquals(1, result5.size());
    assertEquals("2", result5.get(0).getName());

    q = pm.newQuery(String.format(queryFormat, 2, 5));
    @SuppressWarnings("unchecked")
    List<Flight> result6 = (List<Flight>) q.execute();
    assertEquals(2, result6.size());
    assertEquals("4", result6.get(0).getName());

    q = pm.newQuery(String.format(queryFormat, 2, 2));
    @SuppressWarnings("unchecked")
    List<Flight> result7 = (List<Flight>) q.execute();
    assertEquals(0, result7.size());

    q = pm.newQuery(String.format(queryFormat, 2, 1));
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

    Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where id == key parameters String key");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(KeyFactory.keyToString(flightEntity.getKey()));
    assertEquals(1, flights.size());
    assertEquals(flightEntity.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
  }

  public void testKeyQuery_KeyPk() {
    Entity entityWithName = new Entity(HasKeyPkJDO.class.getSimpleName(), "blarg");
    Entity entityWithId = new Entity(HasKeyPkJDO.class.getSimpleName());
    ldth.ds.put(entityWithName);
    ldth.ds.put(entityWithId);

    Query q = pm.newQuery(
        "select from " + HasKeyPkJDO.class.getName() +
        " where key == mykey parameters " + Key.class.getName() + " mykey");
    @SuppressWarnings("unchecked")
    List<HasKeyPkJDO> result = (List<HasKeyPkJDO>) q.execute(entityWithName.getKey());
    assertEquals(1, result.size());
    assertEquals(entityWithName.getKey(), result.get(0).getKey());

    q = pm.newQuery(
        "select from " + HasKeyPkJDO.class.getName() +
        " where key == mykey parameters " + Key.class.getName() + " mykey");
    @SuppressWarnings("unchecked")
    List<HasKeyPkJDO> result2 = (List<HasKeyPkJDO>) q.execute(entityWithId.getKey());
    assertEquals(1, result2.size());
    assertEquals(entityWithId.getKey(), result2.get(0).getKey());

    q = pm.newQuery(
        "select from " + HasKeyPkJDO.class.getName() +
        " where key == mykeyname parameters " + String.class.getName() + " mykeyname");
    @SuppressWarnings("unchecked")
    List<HasKeyPkJDO> result3 = (List<HasKeyPkJDO>) q.execute(entityWithName.getKey().getName());
    assertEquals(1, result3.size());
    assertEquals(entityWithName.getKey(), result3.get(0).getKey());

    q = pm.newQuery(
        "select from " + HasKeyPkJDO.class.getName() +
        " where key == mykeyid parameters " + String.class.getName() + " mykeyid");
    @SuppressWarnings("unchecked")
    List<HasKeyPkJDO> result4 = (List<HasKeyPkJDO>) q.execute(entityWithId.getKey().getId());
    assertEquals(1, result4.size());
    assertEquals(entityWithId.getKey(), result4.get(0).getKey());
  }

  public void testKeyQueryWithSorts() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);

    Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where id == key parameters String key order by id asc");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(KeyFactory.keyToString(flightEntity.getKey()));
    assertEquals(1, flights.size());
    assertEquals(flightEntity.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
  }

  public void testKeyQuery_MultipleFilters() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);

    Query q = pm.newQuery(
        "select from " + Flight.class.getName()
            + " where id == key && origin == \"yam\" parameters String key");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(KeyFactory.keyToString(flightEntity.getKey()));
    assertEquals(1, flights.size());
    assertEquals(flightEntity.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
  }

  public void testKeyQuery_NonEqualityFilter() {
    Entity flightEntity1 = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity1);
    Entity flightEntity2 = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity2);

    Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where id > key parameters String key");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(KeyFactory.keyToString(flightEntity1.getKey()));
    assertEquals(1, flights.size());
    assertEquals(flightEntity2.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
  }

  public void testKeyQuery_SortByKey() {
    Entity flightEntity1 = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity1);

    Entity flightEntity2 = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity2);

    Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where origin == 'yam' order by id DESC");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute();
    assertEquals(2, flights.size());
    assertEquals(flightEntity2.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    assertEquals(flightEntity1.getKey(), KeyFactory.stringToKey(flights.get(1).getId()));
  }

  public void testAncestorQueryWithStringAncestor() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity = new Entity(HasStringAncestorStringPkJDO.class.getSimpleName(), flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    Query q = pm.newQuery(
        "select from " + HasStringAncestorStringPkJDO.class.getName()
            + " where ancestorId == ancId parameters String ancId");
    @SuppressWarnings("unchecked")
    List<HasStringAncestorStringPkJDO> haList =
        (List<HasStringAncestorStringPkJDO>) q.execute(KeyFactory.keyToString(flightEntity.getKey()));
    assertEquals(1, haList.size());
    assertEquals(flightEntity.getKey(), KeyFactory.stringToKey(haList.get(0).getAncestorId()));

    assertEquals(
        flightEntity.getKey(), getDatastoreQuery(q).getLatestDatastoreQuery().getAncestor());
    assertEquals(NO_FILTERS, getFilterPredicates(q));
    assertEquals(NO_SORTS, getSortPredicates(q));
  }

  public void testAncestorQueryWithKeyAncestor() {
    Entity e = new Entity("parent");
    ldth.ds.put(e);
    Entity childEntity = new Entity(HasKeyAncestorStringPkJDO.class.getSimpleName(), e.getKey());
    ldth.ds.put(childEntity);

    Query q = pm.newQuery(
        "select from " + HasKeyAncestorStringPkJDO.class.getName()
            + " where ancestorKey == ancId parameters " + Key.class.getName() + " ancId");
    @SuppressWarnings("unchecked")
    List<HasKeyAncestorStringPkJDO> result =
        (List<HasKeyAncestorStringPkJDO>) q.execute(e.getKey());
    assertEquals(1, result.size());
    assertEquals(e.getKey(), result.get(0).getAncestorKey());
  }

  public void testIllegalAncestorQuery_BadOperator() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity = new Entity(HasStringAncestorStringPkJDO.class.getName(), flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    Query q = pm.newQuery(
        "select from " + HasStringAncestorStringPkJDO.class.getName()
            + " where ancestorId > ancId parameters String ancId");
    try {
      q.execute(KeyFactory.keyToString(flightEntity.getKey()));
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
    Entity hasAncestorEntity = new Entity(HasStringAncestorStringPkJDO.class.getName(), flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    Query q = pm.newQuery(
        "select from " + HasStringAncestorStringPkJDO.class.getName()
            + " where ancestorId == ancId parameters String ancId order by ancestorId ASC");
    try {
      q.execute(KeyFactory.keyToString(flightEntity.getKey()));
      fail ("expected udfe");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }
  }

  private interface FlightProvider {
    Flight getFlight(Key key);
  }

  private class AttachedFlightProvider implements FlightProvider {
    public Flight getFlight(Key key) {
      return pm.getObjectById(Flight.class, key);
    }
  }

  private class TransientFlightProvider implements FlightProvider {
    public Flight getFlight(Key key) {
      Flight f = new Flight();
      f.setId(KeyFactory.keyToString(key));
      return f;
    }
  }

  private void testFilterByChildObject(FlightProvider fp) {
    Entity parentEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity flightEntity = newFlightEntity(parentEntity.getKey(), null, "f", "bos", "mia", 2, 4, 33);
    ldth.ds.put(flightEntity);

    Flight flight = fp.getFlight(flightEntity.getKey());
    Query q = pm.newQuery(
        "select from " + HasOneToOneJDO.class.getName()
        + " where flight == f parameters " + Flight.class.getName() + " f");
    List<HasOneToOneJDO> result = (List<HasOneToOneJDO>) q.execute(flight);
    assertEquals(1, result.size());
    assertEquals(parentEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
  }

  public void testFilterByChildObject() {
    testFilterByChildObject(new AttachedFlightProvider());
    commitTxn();
    beginTxn();
    testFilterByChildObject(new TransientFlightProvider());
  }

  public void testFilterByNullChildObject() {
    Entity parentEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity flightEntity = newFlightEntity(parentEntity.getKey(), null, "f", "bos", "mia", 2, 4, 33);
    ldth.ds.put(flightEntity);

    Query q = pm.newQuery(
        "select from " + HasOneToOneJDO.class.getName()
        + " where flight == f parameters " + Flight.class.getName() + " f");
    try {
      q.execute(null);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
  }

  public void testContains() {
    Entity e = Flight.newFlightEntity("name1", "bos1", "mia1", 23, 24);
    Entity e2 = Flight.newFlightEntity("name2", "bos2", null, 25, 26);
    Entity e3 = Flight.newFlightEntity("name3", "bos3", "mia2", 27, 28);
    ldth.ds.put(Arrays.asList(e, e2, e3));
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where :p1.contains(name)");
    List<Flight> flights = (List<Flight>) q.execute(Arrays.asList("name1", "name3"));
    assertEquals(2, flights.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), flights.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), flights.get(1).getId());

    q = pm.newQuery("select from " + Flight.class.getName() + " where :p1.contains(dest)");
    flights = (List<Flight>) q.execute(Arrays.asList(null, "mia1"));
    assertEquals(2, flights.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), flights.get(0).getId());
    assertEquals(KeyFactory.keyToString(e2.getKey()), flights.get(1).getId());

    q = pm.newQuery("select from " + Flight.class.getName() + " where :p1.contains(dest) || :p2.contains(dest)");
    flights = (List<Flight>) q.execute(Arrays.asList(null, "mia1"), Arrays.asList("mia2"));
    assertEquals(3, flights.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), flights.get(0).getId());
    assertEquals(KeyFactory.keyToString(e2.getKey()), flights.get(1).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), flights.get(2).getId());
  }

  public void testMultipleIn_Params() {
    Entity e = Flight.newFlightEntity("name1", "mia1", "bos1", 23, 24);
    Entity e2 = Flight.newFlightEntity("name2", "mia2", "bos2", 25, 26);
    Entity e3 = Flight.newFlightEntity("name3", "mia3", "bos3", 27, 28);
    ldth.ds.put(Arrays.asList(e, e2, e3));
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where :p1.contains(name) && :p2.contains(origin)");
    List<Flight> flights =
        (List<Flight>) q.execute(Utils.newArrayList("name1", "name3"), Utils.newArrayList("mia3", "mia2"));
    assertEquals(1, flights.size());
    assertEquals(KeyFactory.keyToString(e3.getKey()), flights.get(0).getId());

    q = pm.newQuery("select from " + Flight.class.getName() + " where :p1.contains(name) || :p2.contains(name)");
    flights =
        (List<Flight>) q.execute(Utils.newArrayList("name1", "name3"), Utils.newArrayList("name4", "name5"));

    assertEquals(2, flights.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), flights.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), flights.get(1).getId());
  }


  public void testOr_Literals() {
    Entity e = Flight.newFlightEntity("name1", "bos1", "mia1", 23, 24);
    Entity e2 = Flight.newFlightEntity("name2", "bos2", null, 25, 26);
    Entity e3 = Flight.newFlightEntity("name3", "bos3", "mia2", 27, 28);
    ldth.ds.put(Arrays.asList(e, e2, e3));
    Query q = pm.newQuery("select from " + Flight.class.getName() +
                             " where name == 'name1' || name == 'name3'");
    List<Flight> flights = (List<Flight>) q.execute();
    assertEquals(2, flights.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), flights.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), flights.get(1).getId());

    q = pm.newQuery("select from " + Flight.class.getName() +
                       " where dest == null || dest == 'mia1'");
    flights = (List<Flight>) q.execute();
    assertEquals(2, flights.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), flights.get(0).getId());
    assertEquals(KeyFactory.keyToString(e2.getKey()), flights.get(1).getId());
  }

  public void testOr_Params() {
    Entity e = Flight.newFlightEntity("name1", "bos1", "mia1", 23, 24);
    Entity e2 = Flight.newFlightEntity("name2", "bos2", "mia2", 25, 26);
    Entity e3 = Flight.newFlightEntity("name3", "bos3", "mia3", 27, 28);
    ldth.ds.put(Arrays.asList(e, e2, e3));
    Query q = pm.newQuery("select from " + Flight.class.getName() +
                             " where name == :p1 || name == :p2");
    List<Flight> flights = (List<Flight>) q.execute("name1", "name3");
    assertEquals(2, flights.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), flights.get(0).getId());
    assertEquals(KeyFactory.keyToString(e3.getKey()), flights.get(1).getId());
  }

  public void testMultipleOr_Literals() {
    Entity e = Flight.newFlightEntity("name1", "bos1", "mia1", 23, 24);
    Entity e2 = Flight.newFlightEntity("name2", "bos2", "mia2", 25, 26);
    Entity e3 = Flight.newFlightEntity("name3", "bos3", "mia3", 27, 28);
    ldth.ds.put(Arrays.asList(e, e2, e3));
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where "
                             + "(name  == 'name1' || name == 'name3') && "
                             + "(origin == 'bos3' || origin == 'bos2')");
    List<Flight> flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(KeyFactory.keyToString(e3.getKey()), flights.get(0).getId());
  }

  public void testMultipleOr_Params() {
    Entity e = Flight.newFlightEntity("name1", "bos1", "mia1", 23, 24);
    Entity e2 = Flight.newFlightEntity("name2", "bos2", "mia2", 25, 26);
    Entity e3 = Flight.newFlightEntity("name3", "bos3", "mia3", 27, 28);
    ldth.ds.put(Arrays.asList(e, e2, e3));
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where "
                             + "(name == :p1 || name == :p2) && "
                             + "(origin == :p3 || origin == :p4)");
    Map<String, String> paramMap = Utils.newHashMap();
    paramMap.put("p1", "name1");
    paramMap.put("p2", "name3");
    paramMap.put("p3", "bos3");
    paramMap.put("p4", "bos2");
    List<Flight> flights = (List<Flight>) q.executeWithMap(paramMap);
    assertEquals(1, flights.size());
    assertEquals(KeyFactory.keyToString(e3.getKey()), flights.get(0).getId());
  }

  public void testIsNullChild() {
    Entity e = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(e);
    Query q = pm.newQuery("select from " + HasOneToOneJDO.class.getName() + " where flight == null");
    try {
      q.execute();
      fail("expected");
    } catch (JDOFatalUserException ex) {
      // good
    }
  }

  public void testIsNullParent() {
    Entity e = new Entity(HasOneToOneJDO.class.getSimpleName());
    Key key = ldth.ds.put(e);
    e = new Entity(HasOneToOneParentJDO.class.getSimpleName(), key);
    ldth.ds.put(e);
    Query q = pm.newQuery(
        "select from " + HasOneToOneParentJDO.class.getName() + " where parent == null");
    try {
      q.execute();
      fail("expected");
    } catch (JDOFatalUserException ex) {
      // good
    }
  }

  private void testFilterByChildObject_AdditionalFilterOnParent(FlightProvider fp) {
    Entity parentEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity flightEntity = newFlightEntity(parentEntity.getKey(), null, "f", "bos", "mia", 2, 4, 33);
    ldth.ds.put(flightEntity);

    Flight flight = fp.getFlight(flightEntity.getKey());
    Query q = pm.newQuery(
        "select from " + HasOneToOneJDO.class.getName()
        + " where id == parentId && flight == f "
        + "parameters String parentId, " + Flight.class.getName() + " f");
    List<HasOneToOneJDO> result = (List<HasOneToOneJDO>) q.execute(KeyFactory.keyToString(flightEntity.getKey()), flight);
    assertTrue(result.isEmpty());

    result = (List<HasOneToOneJDO>) q.execute(KeyFactory.keyToString(parentEntity.getKey()), flight);
    assertEquals(1, result.size());
    assertEquals(parentEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
  }

  public void testFilterByChildObject_AdditionalFilterOnParent() {
    testFilterByChildObject_AdditionalFilterOnParent(new AttachedFlightProvider());
    commitTxn();
    beginTxn();
    testFilterByChildObject_AdditionalFilterOnParent(new TransientFlightProvider());
  }

  private void testFilterByChildObject_UnsupportedOperator(FlightProvider fp) {
    Entity parentEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity flightEntity = newFlightEntity(parentEntity.getKey(), null, "f", "bos", "mia", 2, 4, 33);
    ldth.ds.put(flightEntity);

    Flight flight = fp.getFlight(flightEntity.getKey());
    Query q = pm.newQuery(
        "select from " + HasOneToOneJDO.class.getName()
        + " where flight > f parameters " + Flight.class.getName() + " f");
    try {
      q.execute(flight);
      fail("expected udfe");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }
  }

  public void testFilterByChildObject_UnsupportedOperator() {
    testFilterByChildObject_UnsupportedOperator(new AttachedFlightProvider());
    commitTxn();
    beginTxn();
    testFilterByChildObject_UnsupportedOperator(new TransientFlightProvider());
  }

  private void testFilterByChildObject_ValueWithoutAncestor(FlightProvider fp) {
    Entity parentEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity flightEntity = newFlightEntity("f", "bos", "mia", 2, 4, 33);
    ldth.ds.put(flightEntity);

    Flight flight = fp.getFlight(flightEntity.getKey());
    Query q = pm.newQuery(
        "select from " + HasOneToOneJDO.class.getName()
        + " where flight == f parameters " + Flight.class.getName() + " f");
    try {
      q.execute(flight);
      fail("expected JDOException");
    } catch (JDOException e) {
      // good
    }
  }

  public void testFilterByChildObject_ValueWithoutAncestor() {
    testFilterByChildObject_ValueWithoutAncestor(new AttachedFlightProvider());
    commitTxn();
    beginTxn();
    testFilterByChildObject_ValueWithoutAncestor(new TransientFlightProvider());
  }

  public void testFilterByChildObject_KeyIsWrongType() {
    Entity parentEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);

    Query q = pm.newQuery(
        "select from " + HasOneToOneJDO.class.getName()
        + " where flight == f parameters " + Flight.class.getName() + " f");
    try {
      q.execute(parentEntity.getKey());
      fail("expected JDOException");
    } catch (JDOException e) {
      // good
    }
  }

  public void testFilterByChildObject_KeyParentIsWrongType() {
    Key parent = KeyFactory.createKey("yar", 44);
    Entity flightEntity = new Entity(Flight.class.getSimpleName(), parent);

    Query q = pm.newQuery(
        "select from " + HasOneToOneJDO.class.getName()
        + " where flight == f parameters " + Flight.class.getName() + " f");
    assertTrue(((Collection)q.execute(flightEntity.getKey())).isEmpty());
  }

  public void testFilterByChildObject_ValueWithoutId() {
    Entity parentEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity flightEntity = newFlightEntity("f", "bos", "mia", 2, 4, 33);
    ldth.ds.put(flightEntity);

    Flight flight = new Flight();
    Query q = pm.newQuery(
        "select from " + HasOneToOneJDO.class.getName()
        + " where flight == f parameters " + Flight.class.getName() + " f");
    try {
      q.execute(flight);
      fail("expected JDOException");
    } catch (JDOException e) {
      // good
    }
  }

  public void testFilterByParentObject() {
    Entity parentEntity = new Entity(HasOneToManyListJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity bidirEntity = new Entity(BidirectionalChildListJDO.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(bidirEntity);
    Entity bidirEntity2 = new Entity(BidirectionalChildListJDO.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(bidirEntity2);

    HasOneToManyListJDO parent =
        pm.getObjectById(HasOneToManyListJDO.class, KeyFactory.keyToString(parentEntity.getKey()));
    Query q = pm.newQuery(
        "select from " + BidirectionalChildListJDO.class.getName()
        + " where parent == p parameters " + HasOneToManyListJDO.class.getName() + " p");
    List<BidirectionalChildListJDO> result = (List<BidirectionalChildListJDO>) q.execute(parent);
    assertEquals(2, result.size());
    assertEquals(bidirEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
    assertEquals(bidirEntity2.getKey(), KeyFactory.stringToKey(result.get(1).getId()));
  }

  public void testFilterByParentLongObjectId() {
    Entity parentEntity = new Entity(HasOneToManyLongPkListJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity bidirEntity = new Entity(BidirectionalChildLongPkListJDO.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(bidirEntity);
    Entity bidirEntity2 = new Entity(BidirectionalChildLongPkListJDO.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(bidirEntity2);

    HasOneToManyLongPkListJDO parent =
        pm.getObjectById(HasOneToManyLongPkListJDO.class, KeyFactory.keyToString(parentEntity.getKey()));
    Query q = pm.newQuery(
        "select from " + BidirectionalChildLongPkListJDO.class.getName()
        + " where parent == p parameters long p");
    List<BidirectionalChildLongPkListJDO> result =
        (List<BidirectionalChildLongPkListJDO>) q.execute(parent.getId());
    assertEquals(2, result.size());
    assertEquals(bidirEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
    assertEquals(bidirEntity2.getKey(), KeyFactory.stringToKey(result.get(1).getId()));
  }

  public void testFilterByParentIntObjectId() {
    Entity parentEntity = new Entity(HasOneToManyLongPkListJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity bidirEntity = new Entity(BidirectionalChildLongPkListJDO.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(bidirEntity);
    Entity bidirEntity2 = new Entity(BidirectionalChildLongPkListJDO.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(bidirEntity2);

    HasOneToManyLongPkListJDO parent =
        pm.getObjectById(HasOneToManyLongPkListJDO.class, KeyFactory.keyToString(parentEntity.getKey()));
    Query q = pm.newQuery(
        "select from " + BidirectionalChildLongPkListJDO.class.getName()
        + " where parent == p parameters int p");
    List<BidirectionalChildLongPkListJDO> result =
        (List<BidirectionalChildLongPkListJDO>) q.execute(parent.getId().intValue());
    assertEquals(2, result.size());
    assertEquals(bidirEntity.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
    assertEquals(bidirEntity2.getKey(), KeyFactory.stringToKey(result.get(1).getId()));
  }

  public void testFilterByParentObjectWhereParentIsAChild() {
    Entity parentEntity = new Entity(HasOneToManyListJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(BidirectionalChildListJDO.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(childEntity);
    Entity grandChildEntity1 =
        new Entity(BidirectionalGrandchildListJDO.class.getSimpleName(), childEntity.getKey());
    ldth.ds.put(grandChildEntity1);
    Entity grandChildEntity2 =
        new Entity(BidirectionalGrandchildListJDO.class.getSimpleName(), childEntity.getKey());
    ldth.ds.put(grandChildEntity2);

    BidirectionalChildListJDO child =
        pm.getObjectById(BidirectionalChildListJDO.class, KeyFactory.keyToString(childEntity.getKey()));
    Query q = pm.newQuery(
        "select from " + BidirectionalGrandchildListJDO.class.getName()
        + " where parent == p parameters " + BidirectionalChildListJDO.class.getName() + " p");
    List<BidirectionalGrandchildListJDO> result = (List<BidirectionalGrandchildListJDO>) q.execute(child);
    assertEquals(2, result.size());
    assertEquals(grandChildEntity1.getKey(), KeyFactory.stringToKey(result.get(0).getId()));
    assertEquals(grandChildEntity2.getKey(), KeyFactory.stringToKey(result.get(1).getId()));
  }

  public void testFilterByMultiValueProperty() {
    Entity entity = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    entity.setProperty("strList", Utils.newArrayList("1", "2", "3"));
    entity.setProperty("keyList",
        Utils.newArrayList(KeyFactory.createKey("be", "bo"), KeyFactory.createKey("bo", "be")));
    ldth.ds.put(entity);

    Query q = pm.newQuery(
        "select from " + HasMultiValuePropsJDO.class.getName()
        + " where strList == p1 && strList == p2 parameters String p1, String p2");
    List<HasMultiValuePropsJDO> result = (List<HasMultiValuePropsJDO>) q.execute("1", "3");
    assertEquals(1, result.size());
    result = (List<HasMultiValuePropsJDO>) q.execute("1", "4");
    assertEquals(0, result.size());

    q = pm.newQuery(
        "select from " + HasMultiValuePropsJDO.class.getName()
        + " where keyList == p1 && keyList == p2 parameters " + Key.class.getName() + " p1, "
        + Key.class.getName() + " p2");
    result = (List<HasMultiValuePropsJDO>) q.execute(KeyFactory.createKey("be", "bo"), KeyFactory.createKey("bo", "be"));
    assertEquals(1, result.size());
    result = (List<HasMultiValuePropsJDO>) q.execute(KeyFactory.createKey("be", "bo"), KeyFactory.createKey("bo", "be2"));
    assertEquals(0, result.size());
  }

  public void testNoPutsAfterLoadingMultiValueProperty() throws NoSuchMethodException {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testFilterByMultiValueProperty();
    pm.close();
  }

  public void testFilterByMultiValueProperty_ContainsWithParam() {
    Entity entity = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    entity.setProperty("strList", Utils.newArrayList("1", "2", "3"));
    entity.setProperty("keyList",
        Utils.newArrayList(KeyFactory.createKey("be", "bo"), KeyFactory.createKey("bo", "be")));
    ldth.ds.put(entity);

    Query q = pm.newQuery(
        "select from " + HasMultiValuePropsJDO.class.getName()
        + " where strList.contains(p1) parameters String p1");
    List<HasMultiValuePropsJDO> result = (List<HasMultiValuePropsJDO>) q.execute("1");
    assertEquals(1, result.size());
    result = (List<HasMultiValuePropsJDO>) q.execute("4");
    assertEquals(0, result.size());

    q = pm.newQuery(
        "select from " + HasMultiValuePropsJDO.class.getName()
        + " where keyList.contains(p1) && keyList.contains(p2) parameters "
        + Key.class.getName() + " p1, " + Key.class.getName() + " p2");
    result = (List<HasMultiValuePropsJDO>) q.execute(KeyFactory.createKey("be", "bo"), KeyFactory.createKey("bo", "be"));
    assertEquals(1, result.size());
    result = (List<HasMultiValuePropsJDO>) q.execute(KeyFactory.createKey("be", "bo"), KeyFactory.createKey("bo", "be2"));
    assertEquals(0, result.size());
  }

  public void testFilterByMultiValueProperty_ContainsWithImplicitParam() {
    Entity entity = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    entity.setProperty("strList", Utils.newArrayList("1", "2", "3"));
    entity.setProperty("keyList",
        Utils.newArrayList(KeyFactory.createKey("be", "bo"), KeyFactory.createKey("bo", "be")));
    ldth.ds.put(entity);

    Query q = pm.newQuery(
        "select from " + HasMultiValuePropsJDO.class.getName()
        + " where strList.contains(:p)");
    List<HasMultiValuePropsJDO> result = (List<HasMultiValuePropsJDO>) q.execute("1");
    assertEquals(1, result.size());
    result = (List<HasMultiValuePropsJDO>) q.execute("4");
    assertEquals(0, result.size());

    q = pm.newQuery(
        "select from " + HasMultiValuePropsJDO.class.getName()
        + " where keyList.contains(:p1) && keyList.contains(:p2)");
    result = (List<HasMultiValuePropsJDO>) q.execute(KeyFactory.createKey("be", "bo"), KeyFactory.createKey("bo", "be"));
    assertEquals(1, result.size());
    result = (List<HasMultiValuePropsJDO>) q.execute(KeyFactory.createKey("be", "bo"), KeyFactory.createKey("bo", "be2"));
    assertEquals(0, result.size());
  }

  public void testFilterByMultiValueProperty_ContainsWithLiteralString() {
    Entity entity = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    entity.setProperty("strList", Utils.newArrayList("1", "2", "3"));
    ldth.ds.put(entity);

    Query q = pm.newQuery(
        "select from " + HasMultiValuePropsJDO.class.getName()
        + " where strList.contains(\"1\")");
    List<HasMultiValuePropsJDO> result = (List<HasMultiValuePropsJDO>) q.execute();
    assertEquals(1, result.size());
  }

  public void testFilterByEmbeddedField() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ldth.ds.put(entity);

    Query q = pm.newQuery(
        "select from " + Person.class.getName()
        + " where name.first == \"max\"");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.execute();
    assertEquals(1, result.size());
  }

  public void testFilterByEmbeddedField_2LevelsDeep() {
    Entity entity = new Entity(HasEmbeddedJDO.class.getSimpleName());
    entity.setProperty("val1", "v1");
    entity.setProperty("val2", "v2");
    ldth.ds.put(entity);

    Query q = pm.newQuery(
        "select from " + HasEmbeddedJDO.class.getName()
        + " where embedded1.embedded2.val2 == \"v2\"");
    @SuppressWarnings("unchecked")
    List<HasEmbeddedJDO> result = (List<HasEmbeddedJDO>) q.execute();
    assertEquals(1, result.size());
  }

  public void testFilterByEmbeddedField_OverriddenColumn() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ldth.ds.put(entity);

    Query q = pm.newQuery(
        "select from " + Person.class.getName()
        + " where anotherName.last == \"notross\"");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.execute();
    assertEquals(1, result.size());
  }

  public void testFilterByEmbeddedField_MultipleFields() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "max");
    entity.setProperty("anotherLast", "notross");
    ldth.ds.put(entity);

    Query q = pm.newQuery(
        "select from " + Person.class.getName()
        + " where name.first == \"max\" && anotherName.last == \"notross\"");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.execute();
    assertEquals(1, result.size());
  }

  public void testFilterBySubObject_UnknownField() {
    try {
      pm.newQuery(
          "select from " + Flight.class.getName() + " where origin.doesnotexist == \"max\"").execute();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
  }

  public void testFilterBySubObject_NotEmbeddable() {
    try {
      pm.newQuery(
          "select from " + HasOneToOneJDO.class.getName() + " where flight.origin == \"max\"").execute();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
  }

  public void testSortByEmbeddedField() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ldth.ds.put(entity);

    entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max2");
    entity.setProperty("last", "ross2");
    entity.setProperty("anotherFirst", "notmax2");
    entity.setProperty("anotherLast", "notross2");
    ldth.ds.put(entity);

    Query q = pm.newQuery(
        "select from " + Person.class.getName() + " order by name.first desc");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.execute();
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
    ldth.ds.put(entity);

    entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max2");
    entity.setProperty("last", "ross2");
    entity.setProperty("anotherFirst", "notmax2");
    entity.setProperty("anotherLast", "notross2");
    ldth.ds.put(entity);

    Query q =
        pm.newQuery("select from " + Person.class.getName() + " order by anotherName.last desc");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.execute();
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
    ldth.ds.put(entity0);

    Entity entity1 = new Entity(Person.class.getSimpleName());
    entity1.setProperty("first", "max");
    entity1.setProperty("last", "ross2");
    entity1.setProperty("anotherFirst", "notmax2");
    entity1.setProperty("anotherLast", "notross2");
    ldth.ds.put(entity1);

    Entity entity2 = new Entity(Person.class.getSimpleName());
    entity2.setProperty("first", "a");
    entity2.setProperty("last", "b");
    entity2.setProperty("anotherFirst", "c");
    entity2.setProperty("anotherLast", "d");
    ldth.ds.put(entity2);

    Query q = pm.newQuery(
        "select from " + Person.class.getName() + " order by name.first asc, anotherName.last desc");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.execute();
    assertEquals(3, result.size());
    assertEquals(Long.valueOf(entity2.getKey().getId()), result.get(0).getId());
    assertEquals(Long.valueOf(entity0.getKey().getId()), result.get(1).getId());
    assertEquals(Long.valueOf(entity1.getKey().getId()), result.get(2).getId());
  }

  public void testSortBySubObject_UnknownField() {
    try {
      pm.newQuery("select from " + Flight.class.getName() + " order by origin.first").execute();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
  }

  public void testSortBySubObject_NotEmbeddable() {
    try {
      pm.newQuery("select from " + HasOneToOneJDO.class.getName() + " order by flight.origin").execute();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
  }

  public void testUserQuery() {
    Entity e = KitchenSink.newKitchenSinkEntity("blarg", null);
    ldth.ds.put(e);

    Query q = pm.newQuery(
        "select from " + KitchenSink.class.getName() + " where userVal == u parameters " + User.class.getName() + " u");
    @SuppressWarnings("unchecked")
    List<KitchenSink> results = (List<KitchenSink>) q.execute(KitchenSink.USER1);
    assertEquals(1, results.size());

    Query q2 = pm.newQuery(KitchenSink.class, "userVal == u");
    q2.declareParameters(User.class.getName() + " u");
    @SuppressWarnings("unchecked")
    List<KitchenSink> results2 = (List<KitchenSink>) q2.execute(KitchenSink.USER1);
    assertEquals(1, results2.size());
  }

  public void testBigDecimalQuery() {
    Entity e = KitchenSink.newKitchenSinkEntity("blarg", null);
    ldth.ds.put(e);

    Query q = pm.newQuery(
        "select from " + KitchenSink.class.getName() + " where bigDecimal == bd parameters " + BigDecimal.class.getName() + " bd");
    @SuppressWarnings("unchecked")
    List<KitchenSink> results = (List<KitchenSink>) q.execute(new BigDecimal(2.444d));
    assertEquals(1, results.size());
  }

  public void testQueryWithNegativeLiteralLong() {
    ldth.ds.put(newFlightEntity("1", "yam", "bam", -1, 2));

    Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where you == -1");
    @SuppressWarnings("unchecked")
    List<Flight> results = (List<Flight>) q.execute();
    assertEquals(1, results.size());
    q = pm.newQuery(
        "select from " + Flight.class.getName() + " where you > -2");
    @SuppressWarnings("unchecked")
    List<Flight> results2 = (List<Flight>) q.execute();
    assertEquals(1, results2.size());
  }

  public void testQueryWithNegativeLiteralDouble() {
    ldth.ds.put(KitchenSink.newKitchenSinkEntity("blarg", null));

    Query q = pm.newQuery(
        "select from " + KitchenSink.class.getName() + " where doubleVal > -2.25");
    @SuppressWarnings("unchecked")
    List<KitchenSink> results = (List<KitchenSink>) q.execute();
    assertEquals(1, results.size());
  }

  public void testQueryWithNegativeParam() {
    ldth.ds.put(newFlightEntity("1", "yam", "bam", -1, 2));

    Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where you == p parameters int p");
    @SuppressWarnings("unchecked")
    List<Flight> results = (List<Flight>) q.execute(-1);
    assertEquals(1, results.size());
  }

  public void testKeyQueryWithUnencodedStringPk() {
    Entity e = new Entity(HasUnencodedStringPkJDO.class.getSimpleName(), "yar");
    ldth.ds.put(e);
    Query q = pm.newQuery(
        "select from " + HasUnencodedStringPkJDO.class.getName() + " where id == p parameters String p");
    @SuppressWarnings("unchecked")
    List<HasUnencodedStringPkJDO> results =
        (List<HasUnencodedStringPkJDO>) q.execute(e.getKey().getName());
    assertEquals(1, results.size());
    assertEquals(e.getKey().getName(), results.get(0).getId());

    q = pm.newQuery(
        "select from " + HasUnencodedStringPkJDO.class.getName() + " where id == p parameters "
        + Key.class.getName() + " p");
    @SuppressWarnings("unchecked")
    List<HasUnencodedStringPkJDO> results2 =
        (List<HasUnencodedStringPkJDO>) q.execute(e.getKey());
    assertEquals(1, results2.size());
    assertEquals(e.getKey().getName(), results2.get(0).getId());
  }

  public void testKeyQueryWithLongPk() {
    Entity e = new Entity(HasLongPkJDO.class.getSimpleName());
    ldth.ds.put(e);
    Query q = pm.newQuery(
        "select from " + HasLongPkJDO.class.getName() + " where id == p parameters Long p");
    @SuppressWarnings("unchecked")
    List<HasLongPkJDO> results = (List<HasLongPkJDO>) q.execute(e.getKey().getId());
    assertEquals(1, results.size());
    assertEquals(Long.valueOf(e.getKey().getId()), results.get(0).getId());

    q = pm.newQuery(
        "select from " + HasLongPkJDO.class.getName() + " where id == p parameters "
        + Key.class.getName() + " p");
    @SuppressWarnings("unchecked")
    List<HasLongPkJDO> results2 = (List<HasLongPkJDO>) q.execute(e.getKey().getId());
    assertEquals(1, results2.size());
    assertEquals(Long.valueOf(e.getKey().getId()), results2.get(0).getId());
  }

  public void testKeyQueryWithEncodedStringPk() {
    Entity e = new Entity(HasEncodedStringPkJDO.class.getSimpleName(), "yar");
    ldth.ds.put(e);
    Query q = pm.newQuery(
        "select from " + HasEncodedStringPkJDO.class.getName() + " where id == p parameters String p");
    @SuppressWarnings("unchecked")
    List<HasEncodedStringPkJDO> results =
        (List<HasEncodedStringPkJDO>) q.execute(e.getKey().getName());
    assertEquals(1, results.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), results.get(0).getId());

    q = pm.newQuery(
        "select from " + HasEncodedStringPkJDO.class.getName() + " where id == p parameters "
        + Key.class.getName() + " p");
    @SuppressWarnings("unchecked")
    List<HasEncodedStringPkJDO> results2 =
        (List<HasEncodedStringPkJDO>) q.execute(e.getKey());
    assertEquals(1, results2.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), results2.get(0).getId());

    q = pm.newQuery(
        "select from " + HasEncodedStringPkJDO.class.getName() + " where id == p parameters String p");
    @SuppressWarnings("unchecked")
    List<HasEncodedStringPkJDO> results3 =
        (List<HasEncodedStringPkJDO>) q.execute(e.getKey().getName());
    assertEquals(1, results3.size());
    assertEquals(KeyFactory.keyToString(e.getKey()), results3.get(0).getId());

  }

  public void testUniqueQuery_OneResult() {
    Entity e = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e);
    Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where you == p parameters Long p");
    q.setUnique(true);
    @SuppressWarnings("unchecked")
    Flight result = (Flight) q.execute(23);
    assertEquals(e.getKey(), KeyFactory.stringToKey(result.getId()));
  }

  public void testUniqueQuery_NoResult() {
    Entity e = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e);
    Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where you == p parameters Long p");
    q.setUnique(true);
    assertNull(q.execute(43));
  }

  public void testUniqueQuery_MultipleResults() {
    Entity e1 = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
    Entity e2 = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e1);
    ldth.ds.put(e2);
    Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where you == p parameters Long p");
    q.setUnique(true);
    try {
      q.execute(23);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
    }
  }

  public void testSortByUnknownProperty() {
    try {
      pm.newQuery("select from " + Flight.class.getName() + " order by dne").execute();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
  }

  public void testSetOrdering() {
    Entity e1 = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
    Entity e2 = newFlightEntity("harold", "bos", "mia", 33, 34, 35);
    ldth.ds.put(e1);
    ldth.ds.put(e2);
    Query q = pm.newQuery(Flight.class);
    q.setOrdering("you");
    @SuppressWarnings("unchecked")
    List<Flight> results = (List<Flight>) q.execute();
    assertEquals(2, results.size());
    Flight f1 = results.get(0);
    Flight f2 = results.get(1);
    assertEquals(KeyFactory.stringToKey(f2.getId()), e2.getKey());
    assertEquals(KeyFactory.stringToKey(f1.getId()), e1.getKey());
  }

  public void testSize() {
    for (int i = 0; i < 10; i++) {
      Entity e = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
      ldth.ds.put(e);
    }
    Query q = pm.newQuery(Flight.class);
    @SuppressWarnings("unchecked")
    List<Flight> results = (List<Flight>) q.execute();
    assertEquals(10, results.size());
  }

  public void testDatastoreFailureWhileIterating() {
    // Need to have enough data to ensure a Next call
    for (int i = 0; i < 21; i++) {
      Entity e = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
      ldth.ds.put(e);
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
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);

    Query q = pm.newQuery(Flight.class);
    @SuppressWarnings("unchecked")
    List<Flight> results = (List<Flight>) q.execute();
    try {
      results.size();
      fail("expected exception");
    } catch (NucleusDataStoreException e) { // DataNuc bug - they should be wrapping with JDO exceptions
      // good
      assertTrue(e.getCause() instanceof DatastoreFailureException);
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
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);

    Query q = pm.newQuery(Flight.class);
    try {
      q.execute();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  public void testCountQuery_SetResult() {
    Entity e1 = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
    Entity e2 = newFlightEntity("harold", "bos", "mia", 33, 34, 35);
    ldth.ds.put(e1);
    ldth.ds.put(e2);
    Query q = pm.newQuery(Flight.class);
    q.setResult("count(id)");
    assertEquals(2, q.execute());
  }

  public void testCountQuery_SingleString() {
    Entity e1 = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
    Entity e2 = newFlightEntity("harold", "bos", "mia", 33, 34, 35);
    ldth.ds.put(e1);
    ldth.ds.put(e2);
    Query q = pm.newQuery("select count(id) from " + Flight.class.getName());
    assertEquals(2, q.execute());

    q = pm.newQuery("select COUNT(id) from " + Flight.class.getName());
    assertEquals(2, q.execute());
  }

  public void testCountQueryWithFilter_SingleString() {
    Entity e1 = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
    Entity e2 = newFlightEntity("harold", "bos", "mia", 33, 34, 35);
    ldth.ds.put(e1);
    ldth.ds.put(e2);
    Query q = pm.newQuery("select count(id) from " + Flight.class.getName() + " where you == 23");
    assertEquals(1, q.execute());
  }

  public void testCountQueryWithUnknownCountProp_SingleString() {
    Entity e1 = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
    Entity e2 = newFlightEntity("harold", "bos", "mia", 33, 34, 35);
    ldth.ds.put(e1);
    ldth.ds.put(e2);
    // letting this go through intentionally
    // we may want to circle back and lock this down but for now it's really
    // not a big deal
    Query q = pm.newQuery("select count(doesnotexist) from " + Flight.class.getName());
    assertEquals(2, q.execute());
  }

  public void testCountQueryWithOffsetFails() {
    Entity e1 = newFlightEntity("harold", "bos", "mia", 23, 24, 25);
    Entity e2 = newFlightEntity("harold", "bos", "mia", 33, 34, 35);
    ldth.ds.put(e1);
    ldth.ds.put(e2);
    Query q = pm.newQuery("select count(id) from " + Flight.class.getName());
    q.setRange(1, Long.MAX_VALUE);
    try {
      q.execute();
      fail("expected exception");
    } catch (UnsupportedOperationException uoe) {
      // good
    }
  }

  public void testQueryCacheDisabled() {
    ObjectManager om = ((JDOPersistenceManager)pm).getObjectManager();
    JDOQLQuery q = new JDOQLQuery(om, "select from " + Flight.class.getName());
    assertFalse(q.getBooleanExtensionProperty("datanucleus.query.cached", true));
  }

  public void testFilterByEnum_ProvideStringExplicitly() {
    Entity e = new Entity(HasEnumJDO.class.getSimpleName());
    e.setProperty("myEnum", HasEnumJDO.MyEnum.V1.name());
    ldth.ds.put(e);
    Query q = pm.newQuery("select from " + HasEnumJDO.class.getName() + " where myEnum == p1");
    q.declareParameters(String.class.getName() + " p1");
    List<HasEnumJDO> result = (List<HasEnumJDO>) q.execute(HasEnumJDO.MyEnum.V1.name());
    assertEquals(1, result.size());
  }

  public void testFilterByEnum_ProvideEnumExplicitly() {
    Entity e = new Entity(HasEnumJDO.class.getSimpleName());
    e.setProperty("myEnum", HasEnumJDO.MyEnum.V1.name());
    ldth.ds.put(e);
    Query q = pm.newQuery("select from " + HasEnumJDO.class.getName() + " where myEnum == p1");
    q.declareParameters(HasEnumJDO.MyEnum.class.getName() + " p1");
    List<HasEnumJDO> result = (List<HasEnumJDO>) q.execute(HasEnumJDO.MyEnum.V1);
    assertEquals(1, result.size());
  }

  public void testFilterByEnum_ProvideStringParameterInline() {
    Entity e = new Entity(HasEnumJDO.class.getSimpleName());
    e.setProperty("myEnum", HasEnumJDO.MyEnum.V1.name());
    ldth.ds.put(e);
    Query q = pm.newQuery("select from " + HasEnumJDO.class.getName() + " where myEnum == p1 parameters String p1");
    List<HasEnumJDO> result = (List<HasEnumJDO>) q.execute(HasEnumJDO.MyEnum.V1.name());
    assertEquals(1, result.size());
  }

  public void testFilterByEnum_ProvideEnumParameterInline() {
    Entity e = new Entity(HasEnumJDO.class.getSimpleName());
    e.setProperty("myEnum", HasEnumJDO.MyEnum.V1.name());
    ldth.ds.put(e);
    Query q = pm.newQuery("select from " + HasEnumJDO.class.getName() + " where myEnum == p1 parameters " + HasEnumJDO.MyEnum.class.getName() + " p1");
    List<HasEnumJDO> result = (List<HasEnumJDO>) q.execute(HasEnumJDO.MyEnum.V1);
    assertEquals(1, result.size());
  }

  public void testFilterByEnum_ProvideLiteral() {
    Entity e = new Entity(HasEnumJDO.class.getSimpleName());
    e.setProperty("myEnum", HasEnumJDO.MyEnum.V1.name());
    ldth.ds.put(e);
    Query q = pm.newQuery(
        "select from " + HasEnumJDO.class.getName() + " where myEnum == '"
        + HasEnumJDO.MyEnum.V1.name() + "'");
    List<HasEnumJDO> result = (List<HasEnumJDO>) q.execute();
    assertEquals(1, result.size());
  }

  public void testFilterByShortBlob() {
    Entity e = new Entity(HasBytesJDO.class.getSimpleName());
    e.setProperty("onePrimByte", 8L);
    e.setProperty("shortBlob", new ShortBlob("short blob".getBytes()));
    ldth.ds.put(e);
    Query q = pm.newQuery("select from " + HasBytesJDO.class.getName() + " where shortBlob == p1");
    q.declareParameters(ShortBlob.class.getName() + " p1");
    List<HasBytesJDO> result =
        (List<HasBytesJDO>) q.execute(new ShortBlob("short blob".getBytes()));
    assertEquals(1, result.size());
  }

  public void testFilterByPrimitiveByteArray() {
    Entity e = new Entity(HasBytesJDO.class.getSimpleName());
    e.setProperty("onePrimByte", 8L);
    e.setProperty("primBytes", new ShortBlob("short blob".getBytes()));
    ldth.ds.put(e);
    Query q = pm.newQuery("select from " + HasBytesJDO.class.getName() + " where primBytes == p1");
    q.declareParameters("byte[] p1");
    List<HasBytesJDO> result = (List<HasBytesJDO>) q.execute("short blob".getBytes());
    assertEquals(1, result.size());
  }

  public void testFilterByByteArray() {
    Entity e = new Entity(HasBytesJDO.class.getSimpleName());
    e.setProperty("onePrimByte", 8L);
    e.setProperty("bytes", new ShortBlob("short blob".getBytes()));
    ldth.ds.put(e);
    Query q = pm.newQuery("select from " + HasBytesJDO.class.getName() + " where bytes == p1");
    q.declareParameters("Byte[] p1");
    List<HasBytesJDO> result = (List<HasBytesJDO>) q.execute(
        PrimitiveArrays.asList("short blob".getBytes()).toArray(new Byte[0]));
    assertEquals(1, result.size());
  }

  public void testFilterByDate() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity(null));
    Query q = pm.newQuery("select from " + KitchenSink.class.getName()
                          + " where dateVal >= p1 parameters java.util.Date p1");
    List<KitchenSink> result = (List<KitchenSink>) q.execute(KitchenSink.DATE1);
    assertEquals(1, result.size());
    assertEquals(key, KeyFactory.stringToKey(result.get(0).key));
  }

  public void testExtents() {
    LinkedList<Key> keyStack = new LinkedList<Key>();
    keyStack.addFirst(ldth.ds.put(new Entity(HasLongPkJDO.class.getSimpleName())));
    keyStack.addFirst(ldth.ds.put(new Entity(HasLongPkJDO.class.getSimpleName())));
    keyStack.addFirst(ldth.ds.put(new Entity(HasLongPkJDO.class.getSimpleName())));

    Extent<HasLongPkJDO> ext = pm.getExtent(HasLongPkJDO.class);
    for (HasLongPkJDO pojo : ext) {
      assertEquals(keyStack.removeLast(), TestUtils.createKey(pojo, pojo.getId()));
    }
    assertTrue(keyStack.isEmpty());
  }

  public void testAliasedFilter() {
    Entity flightEntity = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity);

    Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " where this.id == key parameters String key");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(KeyFactory.keyToString(flightEntity.getKey()));
    assertEquals(1, flights.size());
    assertEquals(flightEntity.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
  }

  public void testAliasedSort() {
    Entity flightEntity1 = newFlightEntity("1", "yam", "bam", 2, 2);
    Entity flightEntity2 = newFlightEntity("1", "yam", "bam", 1, 2);
    ldth.ds.put(flightEntity1);
    ldth.ds.put(flightEntity2);

    Query q = pm.newQuery(
        "select from " + Flight.class.getName() + " order by this.you");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute();
    assertEquals(2, flights.size());
    assertEquals(flightEntity2.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    assertEquals(flightEntity1.getKey(), KeyFactory.stringToKey(flights.get(1).getId()));
  }

  public void testAliasedEmbeddedFilter() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ldth.ds.put(entity);

    Query q = pm.newQuery(
        "select from " + Person.class.getName() + " where this.name.first == \"max\"");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.execute();
    assertEquals(1, result.size());
  }

  public void testAliasedEmbeddedSort() {
    Entity entity1 = new Entity(Person.class.getSimpleName());
    entity1.setProperty("first", "max");
    entity1.setProperty("last", "ross");
    entity1.setProperty("anotherFirst", "notmax2");
    entity1.setProperty("anotherLast", "notross");
    ldth.ds.put(entity1);
    Entity entity2 = new Entity(Person.class.getSimpleName());
    entity2.setProperty("first", "max");
    entity2.setProperty("last", "ross");
    entity2.setProperty("anotherFirst", "notmax1");
    entity2.setProperty("anotherLast", "notross");
    ldth.ds.put(entity2);

    Query q = pm.newQuery(
        "select from " + Person.class.getName() + " order by this.anotherName.first");
    @SuppressWarnings("unchecked")
    List<Person> result = (List<Person>) q.execute();
    assertEquals(2, result.size());
    assertEquals(entity2.getKey(), TestUtils.createKey(Person.class, result.get(0).getId()));
    assertEquals(entity1.getKey(), TestUtils.createKey(Person.class, result.get(1).getId()));
  }

  public void testFilterByLiteralDoubleValue() {
    Entity e = KitchenSink.newKitchenSinkEntity("blarg", null);
    ldth.ds.put(e);

    Query q = pm.newQuery(
        "select from " + KitchenSink.class.getName() + " where doublePrimVal > 2.1");
    @SuppressWarnings("unchecked")
    List<KitchenSink> results = (List<KitchenSink>) q.execute();
    assertEquals(1, results.size());
  }

  public void testFilterByParameterDoubleValue() {
    Entity e = KitchenSink.newKitchenSinkEntity("blarg", null);
    ldth.ds.put(e);

    Query q = pm.newQuery(
        "select from " + KitchenSink.class.getName() + " where doublePrimVal > p parameters double p");
    @SuppressWarnings("unchecked")
    List<KitchenSink> results = (List<KitchenSink>) q.execute(2.1d);
    assertEquals(1, results.size());
  }

  public void testFilterByNullValue_Literal() {
    Entity e = new Entity(NullDataJDO.class.getSimpleName());
    e.setProperty("string", null);
    ldth.ds.put(e);

    Query q = pm.newQuery("select from " + NullDataJDO.class.getName() + " where string == null");
    @SuppressWarnings("unchecked")
    List<NullDataJDO> results = (List<NullDataJDO>) q.execute();
    assertEquals(1, results.size());
  }

  public void testFilterByNullValue_Param() {
    Entity e = new Entity(NullDataJDO.class.getSimpleName());
    e.setProperty("string", null);
    ldth.ds.put(e);

    Query q = pm.newQuery("select from " + NullDataJDO.class.getName() + " where string == p");
    q.declareParameters("String p");
    @SuppressWarnings("unchecked")
    List<NullDataJDO> results = (List<NullDataJDO>) q.execute(null);
    assertEquals(1, results.size());
  }

  public void testIsNotNull() {
    Entity e = Flight.newFlightEntity("name", "origin", null, 23, 24);
    ldth.ds.put(e);
    assertEquals(1, countForClass(Flight.class));
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where dest != null");
    assertTrue(((List)q.execute()).isEmpty());
    commitTxn();
    beginTxn();
    e = Flight.newFlightEntity("name", "origin", "not null", 23, 24);
    ldth.ds.put(e);
    q.setUnique(true);
    Flight flight = (Flight) q.execute();
    assertEquals("not null", flight.getDest());
  }

  public void testIsNotNull_Param() {
    Entity e = Flight.newFlightEntity("name", "origin", null, 23, 24);
    ldth.ds.put(e);
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where dest != :p");
    assertTrue(((List)q.execute((String) null)).isEmpty());
    commitTxn();
    beginTxn();
    e = Flight.newFlightEntity("name", "origin", "not null", 23, 24);
    ldth.ds.put(e);
    q.setUnique(true);
    Flight flight = (Flight) q.execute((String) null);
    assertEquals("not null", flight.getDest());
  }

  public void testNotEqual() {
    Entity e = Flight.newFlightEntity("name", "origin", "mia", 23, 24);
    ldth.ds.put(e);
    assertEquals(1, countForClass(Flight.class));
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where dest != 'mia'");
    assertTrue(((List)q.execute()).isEmpty());
    commitTxn();
    beginTxn();
    e = Flight.newFlightEntity("name", "origin", "not mia", 23, 24);
    ldth.ds.put(e);
    q.setUnique(true);
    Flight flight = (Flight) q.execute();
    assertEquals("not mia", flight.getDest());
  }

  public void testNotEqual_Param() {
    Entity e = Flight.newFlightEntity("name", "origin", "mia", 23, 24);
    ldth.ds.put(e);
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where dest != :p");
    assertTrue(((List)q.execute("mia")).isEmpty());
    commitTxn();
    beginTxn();
    e = Flight.newFlightEntity("name", "origin", "not mia", 23, 24);
    ldth.ds.put(e);
    q.setUnique(true);
    Flight flight = (Flight) q.execute("mia");
    assertEquals("not mia", flight.getDest());
  }

  public void testQueryForOneToManySetWithKeyPk() {
    Entity e = new Entity(HasOneToManyKeyPkSetJDO.class.getSimpleName());
    ldth.ds.put(e);

    Query q = pm.newQuery("select from " + HasOneToManyKeyPkSetJDO.class.getName());
    @SuppressWarnings("unchecked")
    List<HasOneToManyKeyPkSetJDO> results = (List<HasOneToManyKeyPkSetJDO>) q.execute();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getFlights().size());
  }

  public void testQueryForOneToManyListWithKeyPk() {
    Entity e = new Entity(HasOneToManyKeyPkListJDO.class.getSimpleName());
    ldth.ds.put(e);

    Query q = pm.newQuery("select from " + HasOneToManyKeyPkListJDO.class.getName());
    @SuppressWarnings("unchecked")
    List<HasOneToManyKeyPkListJDO> results = (List<HasOneToManyKeyPkListJDO>) q.execute();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getFlights().size());
  }

  public void testQueryForOneToManySetWithLongPk() {
    Entity e = new Entity(HasOneToManyLongPkSetJDO.class.getSimpleName());
    ldth.ds.put(e);

    Query q = pm.newQuery("select from " + HasOneToManyLongPkSetJDO.class.getName());
    @SuppressWarnings("unchecked")
    List<HasOneToManyLongPkSetJDO> results = (List<HasOneToManyLongPkSetJDO>) q.execute();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getFlights().size());
  }

  public void testQueryForOneToManyListWithLongPk() {
    Entity e = new Entity(HasOneToManyLongPkListJDO.class.getSimpleName());
    ldth.ds.put(e);

    Query q = pm.newQuery("select from " + HasOneToManyLongPkListJDO.class.getName());
    @SuppressWarnings("unchecked")
    List<HasOneToManyLongPkListJDO> results = (List<HasOneToManyLongPkListJDO>) q.execute();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getFlights().size());
  }

  public void testQueryForOneToManySetWithUnencodedStringPk() {
    Entity e = new Entity(HasOneToManyUnencodedStringPkSetJDO.class.getSimpleName(), "yar");
    ldth.ds.put(e);

    Query q = pm.newQuery("select from " + HasOneToManyUnencodedStringPkSetJDO.class.getName());
    @SuppressWarnings("unchecked")
    List<HasOneToManyUnencodedStringPkSetJDO> results =
        (List<HasOneToManyUnencodedStringPkSetJDO>) q.execute();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getFlights().size());
  }

  public void testQueryForOneToManyListWithUnencodedStringPk() {
    Entity e = new Entity(HasOneToManyUnencodedStringPkListJDO.class.getSimpleName(), "yar");
    ldth.ds.put(e);

    Query q = pm.newQuery("select from " + HasOneToManyUnencodedStringPkListJDO.class.getName());
    @SuppressWarnings("unchecked")
    List<HasOneToManyUnencodedStringPkListJDO> results =
        (List<HasOneToManyUnencodedStringPkListJDO>) q.execute();
    assertEquals(1, results.size());
    assertEquals(0, results.get(0).getFlights().size());
  }

  public void testImplicitParams() {
    Entity e1 = newFlightEntity("the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e1);
    Entity e2 = newFlightEntity("the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e2);
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where origin == :orig");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute("bos");
    assertEquals(2, flights.size());
  }

  public void testBatchGet_NoTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity e1 = newFlightEntity("the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e1);
    Entity e2 = newFlightEntity("the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e2);
    Entity e3 = newFlightEntity("the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e3);

    Key key = KeyFactory.createKey("yar", "does not exist");
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where id == :ids");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(Utils.newArrayList(key, e1.getKey(), e2.getKey()));
    assertEquals(2, flights.size());
    Set<Key> keys = Utils.newHashSet(KeyFactory.stringToKey(
        flights.get(0).getId()), KeyFactory.stringToKey(flights.get(1).getId()));
    assertTrue(keys.contains(e1.getKey()));
    assertTrue(keys.contains(e2.getKey()));
  }

  public void testBatchGet_Count_NoTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity e1 = newFlightEntity("the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e1);
    Entity e2 = newFlightEntity("the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e2);
    Entity e3 = newFlightEntity("the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e3);

    Key key = KeyFactory.createKey("yar", "does not exist");

    Query q = pm.newQuery("select count(id) from " + Flight.class.getName() + " where id == :ids");
    int count = (Integer) q.execute(Utils.newArrayList(key, e1.getKey(), e2.getKey()));
    assertEquals(2, count);
  }

  public void testBatchGet_Txn() {
    Entity e1 = newFlightEntity("the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e1);
    Entity e2 = newFlightEntity(e1.getKey(), "blar", "the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e2);
    Entity e3 = newFlightEntity("the name", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e3);

    Key key = KeyFactory.createKey(e1.getKey(), "yar", "does not exist");
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where id == :ids");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute(Utils.newArrayList(key, e1.getKey(), e2.getKey()));
    assertEquals(2, flights.size());
    Set<Key> keys = Utils.newHashSet(KeyFactory.stringToKey(
        flights.get(0).getId()), KeyFactory.stringToKey(flights.get(1).getId()));
    assertTrue(keys.contains(e1.getKey()));
    assertTrue(keys.contains(e2.getKey()));
  }

  public void testBatchGet_Illegal() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where origin == :ids");
    try {
      q.execute(Utils.newArrayList());
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
    q = pm.newQuery("select from " + Flight.class.getName() + " where id == :ids && origin == :origin");
    try {
      q.execute(Utils.newArrayList(), "bos");
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
    q = pm.newQuery("select from " + Flight.class.getName() + " where origin == :origin && id == :ids");
    try {
      q.execute("bos", Utils.newArrayList());
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
    q = pm.newQuery("select from " + Flight.class.getName() + " where id > :ids");
    try {
      q.execute(Utils.newArrayList());
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
    q = pm.newQuery("select from " + Flight.class.getName() + " where id == :ids order by id");
    try {
      q.execute(Utils.newArrayList());
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
  }

  public void testRestrictFetchedFields_UnknownField() {
    Query q = pm.newQuery("select dne from " + Flight.class.getName());
    try {
      q.execute();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
    }
  }

  public void testRestrictFetchedFields_OneField() {
    Entity e1 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ldth.ds.put(e1);
    commitTxn();
    beginTxn();
    Query q = pm.newQuery("select origin from " + Flight.class.getName());
    @SuppressWarnings("unchecked")
    List<String> origins = (List<String>) q.execute();
    assertEquals(1, origins.size());
    assertEquals("bos", origins.get(0));

    Entity e2 = Flight.newFlightEntity("jimmy", "lax", "mia", 23, 24);
    ldth.ds.put(e2);
    commitTxn();
    beginTxn();
    @SuppressWarnings("unchecked")
    List<String> origins2 = (List<String>) q.execute();
    assertEquals(2, origins2.size());
    assertEquals("bos", origins2.get(0));
    assertEquals("lax", origins2.get(1));
  }

  public void testRestrictFetchedFields_OneIdField() {
    Entity e1 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ldth.ds.put(e1);
    Entity e2 = Flight.newFlightEntity("jimmy", "lax", "mia", 23, 24);
    ldth.ds.put(e2);

    Query q = pm.newQuery("select id from " + Flight.class.getName());
    @SuppressWarnings("unchecked")
    List<String> ids = (List<String>) q.execute();
    assertEquals(2, ids.size());
    assertEquals(KeyFactory.keyToString(e1.getKey()), ids.get(0));
    assertEquals(KeyFactory.keyToString(e2.getKey()), ids.get(1));
    Flight f = pm.getObjectById(Flight.class, e1.getKey());
    assertEquals("jimmy", f.getName());
    f.setName("not jimmy");
    commitTxn();
    beginTxn();
    f = pm.getObjectById(Flight.class, e1.getKey());
    assertEquals("not jimmy", f.getName());
    commitTxn();
  }

  public void testRestrictFetchedFields_TwoIdFields() {
    Entity e1 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ldth.ds.put(e1);
    Entity e2 = Flight.newFlightEntity("jimmy", "lax", "mia", 23, 24);
    ldth.ds.put(e2);

    Query q = pm.newQuery("select id, id from " + Flight.class.getName());
    @SuppressWarnings("unchecked")
    List<Object[]> ids = (List<Object[]>) q.execute();
    assertEquals(2, ids.size());
    assertEquals(2, ids.get(0).length);
    assertEquals(2, ids.get(1).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), ids.get(0)[0]);
    assertEquals(KeyFactory.keyToString(e1.getKey()), ids.get(0)[1]);
    assertEquals(KeyFactory.keyToString(e2.getKey()), ids.get(1)[0]);
    assertEquals(KeyFactory.keyToString(e2.getKey()), ids.get(1)[1]);
    Flight f = pm.getObjectById(Flight.class, e1.getKey());
    assertEquals("jimmy", f.getName());
    f.setName("not jimmy");
    commitTxn();
    beginTxn();
    f = pm.getObjectById(Flight.class, e1.getKey());
    assertEquals("not jimmy", f.getName());
    commitTxn();
  }

  public void testRestrictFetchedFields_TwoFields() {
    Entity e1 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ldth.ds.put(e1);
    Query q = pm.newQuery("select origin, dest from " + Flight.class.getName());
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.execute();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals("bos", results.get(0)[0]);
    assertEquals("mia", results.get(0)[1]);

    Entity e2 = Flight.newFlightEntity("jimmy", "lax", null, 23, 24);
    ldth.ds.put(e2);
    commitTxn();
    beginTxn();

    @SuppressWarnings("unchecked")
    List<Object[]> results2 = (List<Object[]>) q.execute();
    assertEquals(2, results2.size());
    assertEquals(2, results2.get(0).length);
    assertEquals("bos", results2.get(0)[0]);
    assertEquals("mia", results2.get(0)[1]);
    assertEquals(2, results2.get(0).length);
    assertEquals("lax", results2.get(1)[0]);
    assertNull(results2.get(1)[1]);
  }

  public void testRestrictFetchedFields_TwoFields_IdIsFirst() {
    Entity e1 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ldth.ds.put(e1);
    commitTxn();
    beginTxn();
    Query q = pm.newQuery("select id, dest from " + Flight.class.getName());
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.execute();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results.get(0)[0]);
    assertEquals("mia", results.get(0)[1]);

    Entity e2 = Flight.newFlightEntity("jimmy", "lax", null, 23, 24);
    ldth.ds.put(e2);
    commitTxn();
    beginTxn();

    @SuppressWarnings("unchecked")
    List<Object[]> results2 = (List<Object[]>) q.execute();
    assertEquals(2, results2.size());
    assertEquals(2, results2.get(0).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results2.get(0)[0]);
    assertEquals("mia", results2.get(0)[1]);
    assertEquals(2, results2.get(0).length);
    assertEquals(KeyFactory.keyToString(e2.getKey()), results2.get(1)[0]);
    assertNull(results2.get(1)[1]);
  }

  public void testRestrictFetchedFields_TwoFields_IdIsSecond() {
    Entity e1 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ldth.ds.put(e1);
    commitTxn();
    beginTxn();
    Query q = pm.newQuery("select origin, id from " + Flight.class.getName());
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.execute();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals("bos", results.get(0)[0]);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results.get(0)[1]);

    Entity e2 = Flight.newFlightEntity("jimmy", "lax", null, 23, 24);
    ldth.ds.put(e2);
    commitTxn();
    beginTxn();

    @SuppressWarnings("unchecked")
    List<Object[]> results2 = (List<Object[]>) q.execute();
    assertEquals(2, results2.size());
    assertEquals(2, results2.get(0).length);
    assertEquals("bos", results2.get(0)[0]);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results2.get(0)[1]);
    assertEquals(2, results2.get(0).length);
    assertEquals("lax", results2.get(1)[0]);
    assertEquals(KeyFactory.keyToString(e2.getKey()), results2.get(1)[1]);
  }

  public void testRestrictFetchedFields_OneToOne() {
    Entity e1 = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(e1);
    Entity e2 = Flight.newFlightEntity(e1.getKey(), "key name", "jimmy", "bos", "mia", 23, 24, 25);
    ldth.ds.put(e2);
    Query q = pm.newQuery("select id, flight from " + HasOneToOneJDO.class.getName());
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.execute();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results.get(0)[0]);
    Flight f = pm.getObjectById(Flight.class, e2.getKey());
    assertEquals(f, results.get(0)[1]);
  }

  public void testRestrictFetchedFields_OneToMany() {
    Entity e1 = new Entity(HasOneToManyListJDO.class.getSimpleName());
    ldth.ds.put(e1);
    Entity e2 = Flight.newFlightEntity(e1.getKey(), "key name", "jimmy", "bos", "mia", 23, 24, 25);
    e2.setProperty("flights_INTEGER_IDX", 0);
    ldth.ds.put(e2);
    Query q = pm.newQuery("select id, flights from " + HasOneToManyListJDO.class.getName());
    @SuppressWarnings("unchecked")
    List<Object[]> results = (List<Object[]>) q.execute();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    assertEquals(KeyFactory.keyToString(e1.getKey()), results.get(0)[0]);
    Flight f = pm.getObjectById(Flight.class, e2.getKey());
    List<Flight> flights = (List<Flight>) results.get(0)[1];
    assertEquals(1, flights.size());
    assertEquals(f, flights.get(0));
  }

  public void testRestrictFetchedFields_AliasedField() {
    Entity e1 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ldth.ds.put(e1);
    Query q = pm.newQuery("select this.origin from " + Flight.class.getName());
    @SuppressWarnings("unchecked")
    List<String> origins = (List<String>) q.execute();
    assertEquals(1, origins.size());
    assertEquals("bos", origins.get(0));
  }

  public void testRestrictFetchedFieldsAndCount() {
    Entity e1 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ldth.ds.put(e1);
    Query q = pm.newQuery("select count(id), origin from " + Flight.class.getName());
    try {
      q.execute();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException e) {
      // good
    }

    q = pm.newQuery("select origin, count(id) from " + Flight.class.getName());
    try {
      q.execute();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException e) {
      // good
    }
  }

  public void testRestrictFetchedFields_EmbeddedField() {
    Entity entity = new Entity(Person.class.getSimpleName());
    entity.setProperty("first", "max");
    entity.setProperty("last", "ross");
    entity.setProperty("anotherFirst", "notmax");
    entity.setProperty("anotherLast", "notross");
    ldth.ds.put(entity);

    Query q = pm.newQuery("select name.first, anotherName.last from " + Person.class.getName());
    @SuppressWarnings("unchecked")
    List<Object[]> result = (List<Object[]>) q.execute();
    assertEquals(1, result.size());
  }

  public void testAggregateInFilterFails() {
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where you == max(you)");
    try {
      q.execute();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException e) {
      // good
    }
  }

  public void testQueryWithSingleCharacterLiteral() {
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where name == 'y'");
    List<Flight> result = (List<Flight>) q.execute();
    assertTrue(result.isEmpty());

    Entity e = Flight.newFlightEntity("y", "bos", "mia", 23, 24);
    ldth.ds.put(e);
    commitTxn();
    beginTxn();
    q.setUnique(true);
    Flight f = (Flight) q.execute();
    assertEquals(e.getKey(), KeyFactory.stringToKey(f.getId()));
  }

  public void testAccessResultsAfterClose() {
    for (int i = 0; i < 3; i++) {
      Entity e = Flight.newFlightEntity("this", "bos", "mia", 24, 25);
      ldth.ds.put(e);
    }
    Query q = pm.newQuery("select from " + Flight.class.getName());
    @SuppressWarnings("unchecked")
    List<Flight> results = (List<Flight>) q.execute();
    Iterator<Flight> iter = results.iterator();
    iter.next();
    commitTxn();
    pm.close();
    Flight f = iter.next();
    f.getDest();
    iter.next();
  }

  public void testParamReferencedTwice() {
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where name == p && origin == p");
    q.declareParameters("String p");
    q.execute("23");
  }

  public void testStartsWith_Literal() {
    Entity e1 = Flight.newFlightEntity("y", "bos", "mia", 24, 25);
    ldth.ds.put(e1);
    Entity e2 = Flight.newFlightEntity("yam", "bos", "mia", 24, 25);
    ldth.ds.put(e2);
    Entity e3 = Flight.newFlightEntity("z", "bos", "mia", 24, 25);
    ldth.ds.put(e3);
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where name.startsWith(\"y\")");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute();
    assertEquals(2, flights.size());

    q = pm.newQuery("select from " + Flight.class.getName() + " where name.startsWith(\"ya\")");
    @SuppressWarnings("unchecked")
    List<Flight> flights2 = (List<Flight>) q.execute();
    assertEquals(1, flights2.size());

    q = pm.newQuery("select from " + Flight.class.getName() + " where name.startsWith(\"za\")");
    @SuppressWarnings("unchecked")
    List<Flight> flights3 = (List<Flight>) q.execute();
    assertTrue(flights3.isEmpty());
  }

  public void testStartsWith_Param() {
    Entity e1 = Flight.newFlightEntity("y", "bos", "mia", 24, 25);
    ldth.ds.put(e1);
    Entity e2 = Flight.newFlightEntity("yam", "bos", "mia", 24, 25);
    ldth.ds.put(e2);
    Entity e3 = Flight.newFlightEntity("z", "bos", "mia", 24, 25);
    ldth.ds.put(e3);
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where name.startsWith(p)");
    q.declareParameters("String p");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute("y");
    assertEquals(2, flights.size());

    List<Flight> flights2 = (List<Flight>) q.execute("ya");
    assertEquals(1, flights2.size());

    List<Flight> flights3 = (List<Flight>) q.execute("za");
    assertTrue(flights3.isEmpty());
  }

  public void testStartsWith_ImplicitParam() {
    Entity e1 = Flight.newFlightEntity("y", "bos", "mia", 24, 25);
    ldth.ds.put(e1);
    Entity e2 = Flight.newFlightEntity("yam", "bos", "mia", 24, 25);
    ldth.ds.put(e2);
    Entity e3 = Flight.newFlightEntity("z", "bos", "mia", 24, 25);
    ldth.ds.put(e3);
    Query q = pm.newQuery("select from " + Flight.class.getName() + " where name.startsWith(:p)");
    @SuppressWarnings("unchecked")
    List<Flight> flights = (List<Flight>) q.execute("y");
    assertEquals(2, flights.size());

    List<Flight> flights2 = (List<Flight>) q.execute("ya");
    assertEquals(1, flights2.size());

    List<Flight> flights3 = (List<Flight>) q.execute("za");
    assertTrue(flights3.isEmpty());
  }

  public void testAncestorQueryForDifferentEntityGroupWithCurrentTxn() {
    Entity e1 = Flight.newFlightEntity("y", "bos", "mia", 24, 25);
    ldth.ds.put(e1);

    // Not used, but associates the txn with the flight's entity group
    Flight f = pm.getObjectById(Flight.class, e1.getKey());

    Query q = pm.newQuery(
        "select from " + HasKeyAncestorKeyPkJDO.class.getName() + " where ancestorKey == :p");
    try {
      q.execute(KeyFactory.createKey("yar", 33L));
      fail("expected iae");
    } catch (JDOFatalUserException e) {
      // good
    }
    Map<String, Object> extensions = Utils.newHashMap();
    extensions.put("gae.exclude-query-from-txn", false);
    q.setExtensions(extensions);
    try {
      q.execute(KeyFactory.createKey("yar", 33L));
      fail("expected iae");
    } catch (JDOFatalUserException e) {
      // good
    }
    extensions.put("gae.exclude-query-from-txn", true);
    q.setExtensions(extensions);
    q.execute(KeyFactory.createKey("yar", 33L));
  }

  public void testNullAncestorParam() {
    Query q = pm.newQuery(HasKeyAncestorStringPkJDO.class);
    q.setFilter("ancestorKey == :p");
    try {
      q.execute(null);
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException e) {
      // good
    }
  }

  public void testNonexistentClassThrowsReasonableException() {
    try {
      pm.newQuery("select from xyam order by date desc range 0,5").execute();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
  }

  public void testSubclassesNotSupported() {
    JDOQLQuery q = new JDOQLQuery(getObjectManager());
    q.setSubclasses(false);
    try {
      q.setSubclasses(true);
      fail("expected nue");
    } catch (NucleusUserException nue) {
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
      fail("expected exception for query <" + query + ">");
    } catch (JDOFatalUserException e) {
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
      // Expression.Operator doesn't override equals
      // so we just compare the string representation.
      assertEquals(unsupportedOp.toString(), uoe.getOperation().toString());
    }
  }

  private void assertQuerySupported(Class<?> clazz, String query,
      List<FilterPredicate> addedFilters, List<SortPredicate> addedSorts, Object... bindVariables) {
    Query q;
    if (query.equals("")) {
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

  private DatastoreQuery getDatastoreQuery(Query q) {
    return ((JDOQLQuery)((JDOQuery)q).getInternalQuery()).getDatastoreQuery();
  }

  private List<FilterPredicate> getFilterPredicates(Query q) {
    return getDatastoreQuery(q).getLatestDatastoreQuery().getFilterPredicates();
  }

  private List<SortPredicate> getSortPredicates(Query q) {
    return getDatastoreQuery(q).getLatestDatastoreQuery().getSortPredicates();
  }

  private void assertQuerySupportedWithExplicitParams(String query,
      List<FilterPredicate> addedFilters, List<SortPredicate> addedSorts, String explicitParams,
      Object... bindVariables) {
    Query q = pm.newQuery(query);
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
