package com.google.appengine.datanucleus.bugs.jpa;

import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.bugs.test.Foo;

public class Issue198Test extends JPABugTestCase {
  public void testRun() {
    beginTxn();
    Foo pojo = new Foo();
    em.persist(pojo);
    commitTxn();
    try {
      com.google.appengine.api.datastore.Entity e =
        ds.get(KeyFactory.createKey("BugsJPA$Foo", pojo.getId()));
      assertTrue(e.hasProperty("barKey"));
    } catch (EntityNotFoundException enfe) {
      fail("Entity wasn't found");
    }
  }
}
