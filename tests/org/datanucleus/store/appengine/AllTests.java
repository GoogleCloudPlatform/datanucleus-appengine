// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import junit.framework.TestSuite;
import junit.framework.Test;
import org.datanucleus.store.appengine.query.JDOQLQueryTest;
import org.datanucleus.store.appengine.query.JPQLQueryTest;
import org.datanucleus.store.appengine.query.StreamingQueryResultTest;

/**
 * @author Max Ross <maxr@google.com>
 */
public class AllTests {
  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTestSuite(DatastoreFieldManagerTest.class);
    suite.addTestSuite(JDOQLQueryTest.class);
    suite.addTestSuite(JPQLQueryTest.class);
    suite.addTestSuite(StreamingQueryResultTest.class);
    return suite;
  }
}
