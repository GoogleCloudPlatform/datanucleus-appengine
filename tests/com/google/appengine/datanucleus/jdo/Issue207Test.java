package com.google.appengine.datanucleus.jdo;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.test.jdo.Issue207Child;
import com.google.appengine.datanucleus.test.jdo.Issue207Parent;

public class Issue207Test extends JDOTestCase {

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
