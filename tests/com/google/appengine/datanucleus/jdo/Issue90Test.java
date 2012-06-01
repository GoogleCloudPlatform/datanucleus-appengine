package com.google.appengine.datanucleus.jdo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.appengine.datanucleus.jdo.JDOTestCase;
import com.google.appengine.datanucleus.test.jdo.Issue90Child;
import com.google.appengine.datanucleus.test.jdo.Issue90Parent;

public class Issue90Test extends JDOTestCase {

  public void testPersist() {
    Issue90Parent p = new Issue90Parent();
    p.setName("First Parent");
    pm.makePersistent(p);
    Object id = pm.getObjectId(p);

    Issue90Child c1 = new Issue90Child();
    c1.setName("Child 1");
    Issue90Child c2 = new Issue90Child();
    c2.setName("Child 2");

    p.setChildren(Arrays.asList(c1, c2));
    pm.makePersistent(p);
    pm.close();

    // Retrieve and check the results
    pm = pmf.getPersistenceManager();
    beginTxn();
    Issue90Parent p1 = (Issue90Parent)pm.getObjectById(id);
    List<Issue90Child> children = p1.getChildren();
    assertNotNull(children);
    assertEquals(2, children.size());
    Iterator<Issue90Child> childIter = children.iterator();
    assertEquals("Child 1", childIter.next().getName());
    assertEquals("Child 2", childIter.next().getName());

    commitTxn();
  }
}
