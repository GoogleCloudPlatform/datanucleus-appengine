package com.google.appengine.datanucleus.bugs.jdo;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.bugs.jdo.JDOBugTestCase;
import com.google.appengine.datanucleus.bugs.test.Issue125Entity;

public class Issue125Test extends JDOBugTestCase {

  public void testInsert() {
    Issue125Entity pojo = new Issue125Entity();
    assertNull(pojo.getKey());
    beginTxn();
    NucleusLogger.GENERAL.info(">> inserting pojo");
    pm.makePersistent(pojo);
    commitTxn();
    assertNotNull(pojo.getKey());
  }
}
