// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.datanucleus.store.appengine.query.JDOQLQueryTest;
import org.datanucleus.store.appengine.query.JPQLQueryTest;
import org.datanucleus.store.appengine.query.StreamingQueryResultTest;

/**
 * All tests for the app engine datanucleus plugin.
 * This will be difficult to keep in sync but we'll do our best.
 *
 * @author Max Ross <maxr@google.com>
 */
public class AllTests {
  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTestSuite(DatastoreFieldManagerTest.class);
    suite.addTestSuite(JDOQLQueryTest.class);
    suite.addTestSuite(JPQLQueryTest.class);
    suite.addTestSuite(StreamingQueryResultTest.class);
    suite.addTestSuite(SerializationTest.class);
    suite.addTestSuite(SerializationManagerTest.class);
    suite.addTestSuite(JDOFetchTest.class);
    suite.addTestSuite(JDOInsertionTest.class);
    suite.addTestSuite(JDOUpdateTest.class);
    suite.addTestSuite(JDODeleteTest.class);
    suite.addTestSuite(JDOTransactionTest.class);
    suite.addTestSuite(JPAFetchTest.class);
    suite.addTestSuite(JPAInsertionTest.class);
    suite.addTestSuite(JPAUpdateTest.class);
    suite.addTestSuite(JPADeleteTest.class);
    suite.addTestSuite(JPATransactionTest.class);
    suite.addTestSuite(JDOAncestorTest.class);
    suite.addTestSuite(JPAAncestorTest.class);
    suite.addTestSuite(JDOTableAndColumnTest.class);
    suite.addTestSuite(JPAOneToOneTest.class);
    suite.addTestSuite(JDOOneToOneTest.class);
    suite.addTestSuite(JPAImplicitEntityGroupTest.class);
    suite.addTestSuite(JDOImplicitEntityGroupTest.class);
    suite.addTestSuite(JPAOneToManyTest.class);
    return suite;
  }
}
