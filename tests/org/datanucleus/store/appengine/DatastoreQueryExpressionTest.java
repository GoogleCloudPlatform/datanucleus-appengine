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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.Query.SortPredicate;

import junit.framework.TestCase;

import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.expression.LogicSetExpression;
import org.datanucleus.store.mapped.expression.QueryExpression;
import org.datanucleus.store.mapped.expression.ScalarExpression;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreQueryExpressionTest extends TestCase {

  public void testSetOrdering_DifferentArrayLengths() {
    DatastoreQueryExpression dqe = new DatastoreQueryExpression(null, null);
    try {
      dqe.setOrdering(new ScalarExpression[1], new boolean[2]);
      fail("expected iae");
    } catch (IllegalArgumentException iae) {
      // good
    }
  }

  public void testSetOrdering_NotDatastoreFieldExpression() {
    DatastoreQueryExpression dqe = new DatastoreQueryExpression(null, null);
    ScalarExpression expr = new ScalarExpression(dqe) {};
    try {
      dqe.setOrdering(new ScalarExpression[] {expr}, new boolean[1]);
      fail("expected uoe");
    } catch (UnsupportedOperationException uoe) {
      // good
    }
  }

  private static final LogicSetExpression LSE = new LogicSetExpression(null, null, null) {
    public String referenceColumn(DatastoreField col) {
      return null;
    }

    public String toString() {
      return null;
    }
  };

  private static final class MyDatastoreFieldExpression extends ScalarExpression.DatastoreFieldExpression {
    private final String propertyName;
    private MyDatastoreFieldExpression(QueryExpression qs, String propertyName) {
      super(qs, null, LSE);
      this.propertyName = propertyName;
    }

    public String toString() {
      return propertyName;
    }
  }

  private static final class MyDatastoreQueryExpression extends DatastoreQueryExpression {
    private final Map<String, Boolean> isPrimaryKeyMap;

    private MyDatastoreQueryExpression(Map<String, Boolean> primaryKeyMap) {
      super(null, null);
      isPrimaryKeyMap = primaryKeyMap;
    }

    @Override
    boolean isPrimaryKey(String propertyName) {
      return isPrimaryKeyMap.get(propertyName);
    }
  }

  public void testSetOrdering_PkFields() {
    Map<String, Boolean> isPrimaryKeyMap = new HashMap<String, Boolean>();

    DatastoreQueryExpression dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", false);
    MyDatastoreFieldExpression dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe, dfe} , new boolean[] {false, true});
    assertSortPredicatesEqual(dqe.getSortPredicates(), ascPred("yar"), descPred("yar"));

    // first sort is id ASC
    dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", true);
    dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    assertSortPredicatesEqual(dqe.getSortPredicates());

    // first sort is id ASC so other sorts are ignored
    dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", true);
    dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe, dfe, dfe} , new boolean[] {false, true, false});
    assertSortPredicatesEqual(dqe.getSortPredicates());

    // first sort is id ASC so other sorts are ignored
    dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", true);
    dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    assertSortPredicatesEqual(dqe.getSortPredicates());

    // first sort is id DESC
    dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", true);
    dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {true});
    assertSortPredicatesEqual(dqe.getSortPredicates(), descPred(Entity.KEY_RESERVED_PROPERTY));

    // first sort is id DESC so other sorts are ignored
    dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", true);
    dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe, dfe, dfe} , new boolean[] {true, true, false});
    assertSortPredicatesEqual(dqe.getSortPredicates(), descPred(Entity.KEY_RESERVED_PROPERTY));

    // first sort is id DESC so other sorts are ignored
    dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", true);
    dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {true});
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    assertSortPredicatesEqual(dqe.getSortPredicates(), descPred(Entity.KEY_RESERVED_PROPERTY));

    // second sort is id ASC
    dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", false);
    isPrimaryKeyMap.put("yam", true);
    dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    dfe = new MyDatastoreFieldExpression(dqe, "yam");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    assertSortPredicatesEqual(
        dqe.getSortPredicates(), ascPred("yar"), ascPred(Entity.KEY_RESERVED_PROPERTY));

    // second sort is id ASC, third sort not added
    dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", false);
    isPrimaryKeyMap.put("yam", true);
    isPrimaryKeyMap.put("yak", false);
    dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    dfe = new MyDatastoreFieldExpression(dqe, "yam");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    dfe = new MyDatastoreFieldExpression(dqe, "yak");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    assertSortPredicatesEqual(
        dqe.getSortPredicates(), ascPred("yar"), ascPred(Entity.KEY_RESERVED_PROPERTY));

    // second sort is id DESC
    dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", false);
    isPrimaryKeyMap.put("yam", true);
    dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    dfe = new MyDatastoreFieldExpression(dqe, "yam");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {true});
    assertSortPredicatesEqual(
        dqe.getSortPredicates(), ascPred("yar"), descPred(Entity.KEY_RESERVED_PROPERTY));

    // second sort is id DESC, third sort not added
    dqe = new MyDatastoreQueryExpression(isPrimaryKeyMap);
    isPrimaryKeyMap.put("yar", false);
    isPrimaryKeyMap.put("yam", true);
    isPrimaryKeyMap.put("yak", false);
    dfe = new MyDatastoreFieldExpression(dqe, "yar");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    dfe = new MyDatastoreFieldExpression(dqe, "yam");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {true});
    dfe = new MyDatastoreFieldExpression(dqe, "yak");
    dqe.setOrdering(new ScalarExpression[] {dfe} , new boolean[] {false});
    assertSortPredicatesEqual(
        dqe.getSortPredicates(), ascPred("yar"), descPred(Entity.KEY_RESERVED_PROPERTY));
  }

  private void assertSortPredicatesEqual(
      Collection<SortPredicate> sortPredicates, SortPredicate... expected) {
    assertEquals(Arrays.asList(expected), sortPredicates);
  }

  private static SortPredicate ascPred(String propName) {
    return new SortPredicate(propName, SortDirection.ASCENDING);
  }

  private static SortPredicate descPred(String propName) {
    return new SortPredicate(propName, SortDirection.DESCENDING);
  }
}
