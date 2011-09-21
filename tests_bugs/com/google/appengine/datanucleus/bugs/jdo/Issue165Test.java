package com.google.appengine.datanucleus.bugs.jdo;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.bugs.jdo.JDOBugTestCase;
import com.google.appengine.datanucleus.bugs.test.Issue165Child;
import com.google.appengine.datanucleus.bugs.test.Issue165Parent;

public class Issue165Test extends JDOBugTestCase {

  public void xtestInsertSeparate() {

    // Persist parent
    Object parentId = null;
    {
      Issue165Parent p = new Issue165Parent();
      p.setAString("Not important");
      NucleusLogger.GENERAL.info(">> pm.makePersistent(p)");
      pm.makePersistent(p);
      parentId = pm.getObjectId(p);
    }
    pm.evictAll();

    pm.currentTransaction().begin();
    Issue165Parent p = (Issue165Parent)pm.getObjectById(parentId);
    Issue165Child c = new Issue165Child();
    c.setAString("Child info");
    p.setChild(c);
    assertTrue(p.getChild() == c && c.getParent() == p);
    NucleusLogger.GENERAL.info(">> pm.makePersistent(p)");
    pm.currentTransaction().commit();

  }

  public void testInsert() {
    Issue165Child c = new Issue165Child();
    c.setAString("Child info");
    Issue165Parent p = new Issue165Parent();
    p.setAString("Not important");
    p.setChild(c);
    c.setParent(p);
    assertTrue(p.getChild() == c && c.getParent() == p);

    NucleusLogger.GENERAL.info(">> pm.makePersistent");
    pm.makePersistent(p);
    NucleusLogger.GENERAL.info(">> pm.makePersistent done");
  }
}
