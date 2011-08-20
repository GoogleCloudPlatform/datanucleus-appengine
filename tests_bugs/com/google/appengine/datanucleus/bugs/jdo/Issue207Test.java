package com.google.appengine.datanucleus.bugs.jdo;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.bugs.jdo.JDOBugTestCase;
import com.google.appengine.datanucleus.bugs.test.Issue207Child;
import com.google.appengine.datanucleus.bugs.test.Issue207Parent;

public class Issue207Test extends JDOBugTestCase {

  public void testOneToManyInterface() {
    Issue207Parent parent = new Issue207Parent("The parent");
    Issue207Child child = new Issue207Child("The child");
    parent.getChildren().add(child);
    child.setParent(parent);

    // Persist the objects
    beginTxn();
    NucleusLogger.GENERAL.info(">> inserting parent+child");
    pm.makePersistent(parent);
    commitTxn();
    pm.evictAll();

    // Check the results
  }
}
