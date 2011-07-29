package com.google.appengine.datanucleus.bugs.jdo;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.bugs.jdo.JDOBugTestCase;
import com.google.appengine.datanucleus.bugs.test.ChildPC;
import com.google.appengine.datanucleus.bugs.test.ParentPC;

public class Issue195Test extends JDOBugTestCase {

  public void testPerform() {
    ParentPC pi = new ParentPC();
    pi.setChild(new ChildPC(1, "Hi"));

    NucleusLogger.GENERAL.info(">> tx.begin");
    pm.currentTransaction().begin();
    NucleusLogger.GENERAL.info(">> Persisting Parent+Child");
    pm.makePersistent(pi);
    NucleusLogger.GENERAL.info(">> tx.commit");
    pm.currentTransaction().commit();
  }
}
