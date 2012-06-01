/**********************************************************************
Copyright (c) 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package com.google.appengine.datanucleus.jpa;

import java.util.List;

import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Root;

import com.google.appengine.datanucleus.DatastoreServiceInterceptor;
import com.google.appengine.datanucleus.WriteBlocker;
import com.google.appengine.datanucleus.test.jpa.Book;

/**
 * Simple tests for use of the JPA criteria API with GAE.
 * Note that in these tests we use the String form of metamodel fields, rather than the annotation-processor
 * generated variant for simplicity (the annotation-processor variant would need the compile process updating).
 */
public class JPACriteriaTest extends JPATestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    DatastoreServiceInterceptor.install(getStoreManager(), new WriteBlocker());
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      super.tearDown();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
  }

  public void testCandidate() {
    ds.put(Book.newBookEntity("Joe Blow", "67890", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "11111", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "12345", "Foo Book"));

    CriteriaBuilder cb = em.getCriteriaBuilder();
    CriteriaQuery cq = cb.createQuery();
    Root<Book> candidate = cq.from(Book.class);
    candidate.alias("b");
    cq.select(candidate);

    Query q = em.createQuery(cq);
    List<Book> books = q.getResultList();
    assertNotNull(books);
    assertEquals(3, books.size());
  }

  public void testFilter() {
    ds.put(Book.newBookEntity("Joe Blow", "67890", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "11111", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "12345", "Foo Book"));

    CriteriaBuilder cb = em.getCriteriaBuilder();
    CriteriaQuery cq = cb.createQuery();
    Root<Book> candidate = cq.from(Book.class);
    candidate.alias("b");
    cq.select(candidate);
    Path titleField = candidate.get("title");
    cq.where(cb.equal(titleField, "Bar Book"));

    Query q = em.createQuery(cq);
    List<Book> books = q.getResultList();
    assertNotNull(books);
    assertEquals(2, books.size());
    for (Book b : books) {
      assertEquals("Bar Book", b.getTitle());
    }
  }

  public void testFilterAndOrder() {
    ds.put(Book.newBookEntity("Joe Blow", "67890", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "11111", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "12345", "Foo Book"));

    CriteriaBuilder cb = em.getCriteriaBuilder();
    CriteriaQuery cq = cb.createQuery();
    Root<Book> candidate = cq.from(Book.class);
    candidate.alias("b");
    cq.select(candidate);

    Path titleField = candidate.get("title");
    cq.where(cb.equal(titleField, "Bar Book"));

    Path isbnField = candidate.get("isbn");
    cq.orderBy(cb.desc(isbnField));

    Query q = em.createQuery(cq);
    List<Book> books = q.getResultList();
    assertNotNull(books);
    assertEquals(2, books.size());
    Book b0 = books.get(0);
    Book b1 = books.get(1);
    assertEquals("Joe Blow", b0.getAuthor());
    assertEquals("Bar Book", b0.getTitle());
    assertEquals("67890", b0.getIsbn());
    assertEquals("Joe Blow", b1.getAuthor());
    assertEquals("Bar Book", b1.getTitle());
    assertEquals("11111", b1.getIsbn());
  }

}