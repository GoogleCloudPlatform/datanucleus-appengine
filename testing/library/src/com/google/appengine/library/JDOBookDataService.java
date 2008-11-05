// Copyright 2008 Google Inc. All Rights Reserved.

package com.google.appengine.library;

import java.util.Collection;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;

/**
 * @author kjin@google.com (Kevin Jin)
 * 
 */
final class JDOBookDataService implements BookDataService {
  private static final PersistenceManagerFactory pmf =
      JDOHelper.getPersistenceManagerFactory("datanucleus.properties");
  private final PersistenceManager pm = pmf.getPersistenceManager();
  private static final String FROM_CLAUSE = "SELECT FROM " + Book.class.getName();

  @SuppressWarnings("unchecked")
  @Override
  public Iterable<Book> asIterable(String jpqlQuery) {
    return (Iterable<Book>) pm.newQuery("javax.jdo.query.JPQL", FROM_CLAUSE + jpqlQuery).execute();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterable<Book> asIterable(String jpqlQuery, int limit, int offset) {
    Query query = pm.newQuery("javax.jdo.query.JPQL", FROM_CLAUSE + jpqlQuery);
    query.setRange(offset, offset + limit);
    return (Iterable<Book>) query.execute();
  }

  @SuppressWarnings("unchecked")
  @Override
  public int countEntities(String jpqlQuery) {
    return ((Collection) asIterable(jpqlQuery)).size();
  }

  @Override
  public void delete(Book book) {
    pm.deletePersistent(book);
  }

  @Override
  public void put(Book book) {
    pm.makePersistent(book);
  }

  @Override
  public void close() {
    pm.close();    
  }
}
