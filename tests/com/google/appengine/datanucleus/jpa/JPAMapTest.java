/**********************************************************************
Copyright (c) 2009 Google Inc.

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

import java.util.Map;

import com.google.appengine.datanucleus.test.jpa.Book;
import com.google.appengine.datanucleus.test.jpa.HasOneToManyMapJPA;

/**
 * Simple tests for persisting/retrieving maps in JPA.
 */
public class JPAMapTest extends JPATestCase {

  public void testInsert() {
    HasOneToManyMapJPA pojo = new HasOneToManyMapJPA();
    pojo.setVal("First");
    Book book1 = new Book();
    book1.setAuthor("Billy Nomates");
    book1.setTitle("Lonely Time");
    book1.setFirstPublished(1981);
    pojo.getBooksByName().put(book1.getTitle(), book1);

    pojo.getBasicMap().put(1, "First Entry");
    pojo.getBasicMap().put(2, "Second Entry");

    // Persist it
    String bookId = null;
    String pojoId = null;
    beginTxn();
    em.persist(pojo);
    commitTxn();
    pojoId = pojo.getId();
    bookId = book1.getId();
    em.clear();

    // Retrieve it and validate
    beginTxn();
    HasOneToManyMapJPA pojo2 = em.find(HasOneToManyMapJPA.class, pojoId);
    assertNotNull(pojo2);

    Map<String, Book> booksByName = pojo2.getBooksByName();
    assertNotNull(booksByName);
    assertEquals("Number of elements in first map is wrong", 1, booksByName.size());
    assertTrue(booksByName.containsKey("Lonely Time"));
    Book bk = booksByName.get("Lonely Time");
    assertEquals("Book id is incorrect", bookId, bk.getId());
    assertEquals("Book published is wrong", 1981, bk.getFirstPublished());

    Map<Integer, String> basicMap = pojo2.getBasicMap();
    assertNotNull(basicMap);
    assertEquals("Number of elements in second map is wrong", 2, basicMap.size());
    assertTrue(basicMap.containsKey(1));
    assertEquals("First Entry", basicMap.get(1));
    assertTrue(basicMap.containsKey(2));
    assertEquals("Second Entry", basicMap.get(2));

    commitTxn();
  }
}
