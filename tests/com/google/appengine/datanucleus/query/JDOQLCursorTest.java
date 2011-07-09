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

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.jdo.JDOTestCase;
import com.google.appengine.datanucleus.test.Book;
import com.google.appengine.datanucleus.test.Flight;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jdo.Query;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JDOQLCursorTest extends JDOTestCase {

  public void testGetCursor_List() {
    Entity e1 = Flight.newFlightEntity("harold", "bos", "mia", 23, 24);
    Entity e2 = Flight.newFlightEntity("harold", "bos", "mia", 23, 24);
    Entity e3 = Flight.newFlightEntity("harold", "bos", "mia", 23, 24);
    ds.put(Arrays.asList(e1, e2, e3));

    Map<String, Object> extensionMap = Utils.newHashMap();

    beginTxn();
    Query q = pm.newQuery(Flight.class);
    q.setRange(0, 1);
    List<Flight> flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(e1.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    Cursor c = JDOCursorHelper.getCursor(flights);
    assertNotNull(c);

    extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, c);
    q.setExtensions(extensionMap);
    flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(e2.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    assertNotNull(JDOCursorHelper.getCursor(flights));

    extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, c.toWebSafeString());
    q.setExtensions(extensionMap);
    flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(e2.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    c = JDOCursorHelper.getCursor(flights);
    assertNotNull(c);

    extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, c);
    q.setExtensions(extensionMap);
    flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(e3.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    assertNotNull(JDOCursorHelper.getCursor(flights));

    extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, c.toWebSafeString());
    q.setExtensions(extensionMap);
    flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(e3.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    assertNotNull(JDOCursorHelper.getCursor(flights));
    commitTxn();
  }
  
  public void testCursorEquality() {
    List<Key> keys = Utils.newArrayList();
    keys.add(ds.put(Book.newBookEntity("auth", "34", "yar")));
    keys.add(ds.put(Book.newBookEntity("auth", "34", "yar")));
    keys.add(ds.put(Book.newBookEntity("auth", "34", "yar")));

    com.google.appengine.api.datastore.Query query = new com.google.appengine.api.datastore.Query("Book");
    QueryResultIterator entityIter = ds.prepare(query).asQueryResultIterator();
    List<Cursor> lowLevelCursors = Utils.newArrayList();
    lowLevelCursors.add(entityIter.getCursor());
    entityIter.next();
    lowLevelCursors.add(entityIter.getCursor());
    entityIter.next();
    lowLevelCursors.add(entityIter.getCursor());
    entityIter.next();
    lowLevelCursors.add(entityIter.getCursor());

    beginTxn();
    Query q = pm.newQuery("select from " + Book.class.getName());
    Iterator<Book> bookIter = asIterator(q);
    List<Cursor> ormCursors = Utils.newArrayList();
    ormCursors.add(JDOCursorHelper.getCursor(bookIter));
    bookIter.next();
    ormCursors.add(JDOCursorHelper.getCursor(bookIter));
    bookIter.next();
    ormCursors.add(JDOCursorHelper.getCursor(bookIter));
    bookIter.next();
    ormCursors.add(JDOCursorHelper.getCursor(bookIter));
    commitTxn();
    
    assertEquals(lowLevelCursors, ormCursors);

    for (int i = 0; i < lowLevelCursors.size(); i++) {
      Cursor lowLevelCursor = lowLevelCursors.get(i);
      List<Entity> list = ds.prepare(query).asList(FetchOptions.Builder.withCursor(lowLevelCursor));
      assertEquals(3 - i, list.size());
    }
  }

  public void testGetCursor_Iterator() {
    List<Key> keys = Utils.newArrayList();
    for (int i = 0; i < 10; i++) {
      keys.add(ds.put(Book.newBookEntity("auth" + i, "34", "yar")));
    }

    beginTxn();
    Query q = pm.newQuery("select from " + Book.class.getName());
    Iterator<Book> bookIter = asIterator(q);
    assertCursorResults(JDOCursorHelper.getCursor(bookIter), keys);
    int index = 0;
    while (bookIter.hasNext()) {
      bookIter.next();
      assertCursorResults(JDOCursorHelper.getCursor(bookIter), keys.subList(++index, keys.size()));
    }
    // setting a max result means we call asQueryResultList(), and there are no
    // intermediate cursors available from a List.
    q.setRange(0, 1);

    bookIter = asIterator(q);
    assertNull(JDOCursorHelper.getCursor((asIterator(q))));
    while (bookIter.hasNext()) {
      bookIter.next();
      assertNull(JDOCursorHelper.getCursor((asIterator(q))));
    }
  }

  private <T> Iterator<T> asIterator(Query q) {
    return ((List) q.execute()).iterator();
  }

  public void testGetCursor_Iterable() {
    List<Key> keys = Utils.newArrayList();
    for (int i = 0; i < 10; i++) {
      keys.add(ds.put(Book.newBookEntity("auth" + i, "34", "yar")));
    }

    beginTxn();
    Query q = pm.newQuery("select from " + Book.class.getName());
    // no limit specified so what we get back doesn't have an end cursor
    List<Book> bookList = (List<Book>) q.execute();
    assertNull(JDOCursorHelper.getCursor(bookList));
    for (Book b : bookList) {
      // no cursor
      assertNull(JDOCursorHelper.getCursor(bookList));
    }
  }

  private void assertCursorResults(Cursor cursor, List<Key> expectedKeys) {
    Query q = pm.newQuery("select from " + Book.class.getName());
    q.addExtension(JDOCursorHelper.QUERY_CURSOR_PROPERTY_NAME, cursor);
    List<Key> keys = Utils.newArrayList();
    for (Object b : (Iterable) q.execute()) {
      keys.add(KeyFactory.stringToKey(((Book) b).getId()));
    }
    assertEquals(expectedKeys, keys);
  }
}
