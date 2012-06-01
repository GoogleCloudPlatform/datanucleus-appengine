package com.google.appengine.datanucleus.query;

import javax.persistence.TypedQuery;

import com.google.appengine.datanucleus.jpa.JPATestCase;
import com.google.appengine.datanucleus.test.jdo.Flight;

public class Issue256Test extends JPATestCase {
  public void testRun() {
    beginTxn();
    Flight fl = new Flight("CHI", "LHR", "BA201", 1, 16);
    em.persist(fl);
    commitTxn();
    em.clear();

    beginTxn();
    TypedQuery<Long> q = em.createQuery("SELECT COUNT(f) FROM " + Flight.class.getName() + " f", Long.class);
    Long result = q.getSingleResult();
    assertEquals(1, result.longValue());
    commitTxn();
  }
}
