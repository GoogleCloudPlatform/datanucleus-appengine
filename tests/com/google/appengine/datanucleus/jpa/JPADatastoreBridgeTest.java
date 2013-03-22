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
package com.google.appengine.datanucleus.jpa;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultIterable;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.datanucleus.JPADatastoreBridge;
import com.google.appengine.datanucleus.test.jpa.Book;


import java.util.List;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JPADatastoreBridgeTest extends JPATestCase {

  public void testConvertQueryResultList_Empty() {
    QueryResultList<Entity> result =
        ds.prepare(new Query("blarg")).asQueryResultList(FetchOptions.Builder.withLimit(10));

    JPADatastoreBridge bridge = new JPADatastoreBridge();
    List<Book> books = bridge.toJPAResult(em, Book.class, result);
    assertEquals(0, books.size());
  }

  public void testConvertQueryResultList() {
    for (int i = 0; i < 5; i++) {
      Entity e = Book.newBookEntity("harold" + i, "isbn", "the title", 2004);
      ds.put(e);
    }
    QueryResultList<Entity> result =
        ds.prepare(new Query("Book")).asQueryResultList(FetchOptions.Builder.withLimit(10));

    JPADatastoreBridge bridge = new JPADatastoreBridge();
    List<Book> books = bridge.toJPAResult(em, Book.class, result);
    assertEquals(5, books.size());
    String id = books.get(0).getId();
    // make sure these books are connected
    beginTxn();
    books.get(0).setTitle("different title");
    commitTxn();
    beginTxn();
    Book f = em.find(Book.class, id);
    assertEquals("different title", f.getTitle());
    commitTxn();
    deleteAll();
  }

  public void testConvertQueryResultIterable() {
    for (int i = 0; i < 5; i++) {
      Entity e = Book.newBookEntity("harold" + i, "isbn", "the title", 2004);
      ds.put(e);
    }
    QueryResultIterable<Entity> result =
        ds.prepare(new Query("Book")).asQueryResultIterable();

    JPADatastoreBridge bridge = new JPADatastoreBridge();
    List<Book> books = bridge.toJPAResult(em, Book.class, result);
    assertEquals(5, books.size());
    String id = books.get(0).getId();
    // make sure these books are connected
    beginTxn();
    books.get(0).setTitle("different title");
    commitTxn();
    beginTxn();
    Book f = em.find(Book.class, id);
    assertEquals("different title", f.getTitle());
    commitTxn();
    deleteAll();
  }

  public void testAccessResultsAfterClose() {
    for (int i = 0; i < 3; i++) {
      Entity e = Book.newBookEntity("this", "that", "the other");
      ds.put(e);
    }
    QueryResultIterable<Entity> result =
        ds.prepare(new Query("Book")).asQueryResultIterable();
    beginTxn();
    JPADatastoreBridge bridge = new JPADatastoreBridge();
    List<Book> books = bridge.toJPAResult(em, Book.class, result);
    commitTxn();
    em.close();
    assertEquals(3, books.size());
    deleteAll();
  }

}