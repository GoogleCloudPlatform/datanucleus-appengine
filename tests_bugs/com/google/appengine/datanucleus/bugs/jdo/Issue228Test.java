package com.google.appengine.datanucleus.bugs.jdo;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.bugs.jdo.JDOBugTestCase;
import com.google.appengine.datanucleus.bugs.test.AChild;
import com.google.appengine.datanucleus.bugs.test.AParent;

public class Issue228Test extends JDOBugTestCase {

  public void testInsert() {
    AChild c = new AChild();
    c.setAString("Child info");
    AParent p = new AParent();
    p.setAString("Not important");
    p.setChild(c);
    assertTrue(p.getChild() == c && c.getParent() == p);

    NucleusLogger.GENERAL.info(">> pm.makePersistent");
    pm.makePersistent(p);
    NucleusLogger.GENERAL.info(">> pm.makePersistent done");
  }
}
