package com.google.appengine.datanucleus.bugs.jdo;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.bugs.jdo.JDOBugTestCase;
import com.google.appengine.datanucleus.bugs.test.Issue228Child;
import com.google.appengine.datanucleus.bugs.test.Issue228Parent;
import com.google.appengine.datanucleus.bugs.test.Issue228Related;
import com.google.appengine.datanucleus.bugs.test.Issue228Owner;

public class Issue228Test extends JDOBugTestCase {

  public void testOneToOneBidir() {
    Issue228Related c = new Issue228Related();
    c.setAString("Child info");
    Issue228Owner p = new Issue228Owner();
    p.setAString("Not important");
    p.setChild(c);
    assertTrue(p.getChild() == c && c.getParent() == p);

    NucleusLogger.GENERAL.info(">> pm.makePersistent");
    pm.makePersistent(p);
    NucleusLogger.GENERAL.info(">> pm.makePersistent done");
  }

  public void testManyToOneBidir() {
    Issue228Parent parent = new Issue228Parent();
    parent.setName("Owner name");
    Issue228Child child = new Issue228Child();
    child.setParent(parent);
    parent.getChildren().add(child);
    child.setName("Child name");

    beginTxn();
    NucleusLogger.GENERAL.info(">> pm.makePersistent(child)");
    pm.makePersistent(child);
    commitTxn();
  }
}
