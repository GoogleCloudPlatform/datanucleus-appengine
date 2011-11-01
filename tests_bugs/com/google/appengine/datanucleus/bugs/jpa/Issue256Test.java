package com.google.appengine.datanucleus.bugs.jpa;

import javax.persistence.TypedQuery;

import com.google.appengine.datanucleus.bugs.test.Bar;

public class Issue256Test extends JPABugTestCase {
  public void testRun() {
    beginTxn();
    Bar pojo = new Bar();
    em.persist(pojo);
    commitTxn();
    em.clear();

    beginTxn();
    TypedQuery<Long> q = em.createQuery("SELECT COUNT(b) FROM " + Bar.class.getName() + " b", Long.class);
    Long result = q.getSingleResult();
    assertEquals(1, result.longValue());
    commitTxn();
  }
}
