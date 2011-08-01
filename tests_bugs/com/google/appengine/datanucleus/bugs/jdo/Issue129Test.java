package com.google.appengine.datanucleus.bugs.jdo;

import com.google.appengine.datanucleus.bugs.jdo.JDOBugTestCase;
import com.google.appengine.datanucleus.bugs.test.Issue129Entity;

public class Issue129Test extends JDOBugTestCase {

  public void testInsert() {
    Issue129Entity entity;
    Issue129LifecycleListener listener = new Issue129LifecycleListener();
    pm.addInstanceLifecycleListener(listener, (Class[])null);

    // makePersistentAll
    entity = new Issue129Entity();
    pm.makePersistentAll(entity);
    assertTrue("PostStore didn't have id set when called for makePersistentAll", listener.idWasSetBeforePostStore());

    // makePersistent
    entity = new Issue129Entity();
    pm.makePersistent(entity);
    assertTrue("PostStore didn't have id set when called for makePersistent", listener.idWasSetBeforePostStore());
  }
}
