package com.google.appengine.datanucleus.bugs.jdo;

import com.google.appengine.datanucleus.bugs.jdo.JDOBugTestCase;
import com.google.appengine.datanucleus.bugs.test.Issue73Child;
import com.google.appengine.datanucleus.bugs.test.Issue73Parent;

public class Issue73Test extends JDOBugTestCase {

  public void testMultipleOneToOne() {
    Issue73Parent p = new Issue73Parent();
    p.setName("First Parent");
    Issue73Child c1 = new Issue73Child();
    c1.setName("Child 1");
    p.setChild1(c1);
    Issue73Child c2 = new Issue73Child();
    c2.setName("Child 2");
    p.setChild1(c2);

    // Persist parent with 2 children
    Object id = null;
    beginTxn();
    pm.makePersistent(p);
    commitTxn();
    id = pm.getObjectId(p);
    pm.close();

    // Retrieve and check the results
    pm = pmf.getPersistenceManager();
    beginTxn();
    Issue73Parent p1 = (Issue73Parent)pm.getObjectById(id);
    Issue73Child c1a = p1.getChild1();
    Issue73Child c2a = p1.getChild2();
    assertNotNull(c1a);
    assertNotNull(c2a);
    assertEquals("Child 1", c1a.getName());
    assertEquals("Child 2", c2a.getName());

    commitTxn();
  }
}
