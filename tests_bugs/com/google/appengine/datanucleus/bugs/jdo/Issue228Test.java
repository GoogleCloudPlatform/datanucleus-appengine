package com.google.appengine.datanucleus.bugs.jdo;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.bugs.jdo.JDOBugTestCase;
import com.google.appengine.datanucleus.bugs.test.Issue228Child;
import com.google.appengine.datanucleus.bugs.test.Issue228Parent;

public class Issue228Test extends JDOBugTestCase {

  public void testInsert() {
    Issue228Child c = new Issue228Child();
    c.setAString("Child info");
    Issue228Parent p = new Issue228Parent();
    p.setAString("Not important");
    p.setChild(c);
    assertTrue(p.getChild() == c && c.getParent() == p);

    NucleusLogger.GENERAL.info(">> pm.makePersistent");
    pm.makePersistent(p);
    NucleusLogger.GENERAL.info(">> pm.makePersistent done");
  }
}
