// Copyright 2008 Google Inc. All Rights Reserved.

package com.google.appengine.library;

import java.util.Collection;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.persistence.Query;

/**
 * TODO: figure out why EntityTransaction is needed for JPA.
 * 
 * @author kjin@google.com (Kevin Jin)
 * 
 */
final class JPABookDataService implements BookDataService {
  private static final EntityManagerFactory emf = Persistence.createEntityManagerFactory("book");
  private final EntityManager em = emf.createEntityManager();
  private static final String FROM_CLAUSE = "SELECT FROM " + Book.class.getName();

  @SuppressWarnings("unchecked")
  @Override
  public Iterable<Book> asIterable(String jpqlQuery) {
    return em.createQuery(FROM_CLAUSE + jpqlQuery).getResultList();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterable<Book> asIterable(String jpqlQuery, int limit, int offset) {
    Query query = em.createQuery(FROM_CLAUSE + jpqlQuery);
    query.setMaxResults(limit);
    query.setFirstResult(offset);
    return query.getResultList();
  }

  @SuppressWarnings("unchecked")
  @Override
  public int countEntities(String jpqlQuery) {
    return ((Collection) asIterable(jpqlQuery)).size();
  }

  @Override
  public void delete(Book book) {
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.remove(book);
    txn.commit();
  }

  @Override
  public void put(Book book) {
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(book);
    txn.commit();
  }

  @Override
  public void close() {
    em.close();
  }
}
