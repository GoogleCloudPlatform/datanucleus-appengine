/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus.query;

import com.google.appengine.datanucleus.jpa.JPATestCase;
import com.google.appengine.datanucleus.test.Book;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class CriteriaTest extends JPATestCase {

  public void testSimpleCriteria() {
    ds.put(Book.newBookEntity("Joe Blow", "67890", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "11111", "Bar Book"));
    ds.put(Book.newBookEntity("Joe Blow", "12345", "Foo Book"));
    ds.put(Book.newBookEntity("Joe Blow", "54321", "A Book"));
    ds.put(Book.newBookEntity("Jane Blow", "13579", "Baz Book"));
    Book book = new Book();
    book.setAuthor("joe bob");
    book.setFirstPublished(1922);
    book.setIsbn("24242");
    book.setTitle("my title");
//    beginTxn();
//    em.persist(book);
//    commitTxn();
//
//    CriteriaBuilder builder = emf.getCriteriaBuilder();
//    CriteriaQuery<Book> crit = builder.createQuery(Book.class);
//    Root<Book> bookRoot = crit.from(Book.class);
//    crit.select(bookRoot);
//    crit.where(builder.equal(bookRoot.get("author"), "Joe Blow"));
//    crit.orderBy(builder.desc(bookRoot.get("title")), builder.asc(bookRoot.get("isbn")));
//
//    List<Book> result = em.createQuery(crit).getResultList();
//
//    assertEquals(4, result.size());
//    assertEquals("12345", result.get(0).getIsbn());
//    assertEquals("11111", result.get(1).getIsbn());
//    assertEquals("67890", result.get(2).getIsbn());
//    assertEquals("54321", result.get(3).getIsbn());
  }
}
