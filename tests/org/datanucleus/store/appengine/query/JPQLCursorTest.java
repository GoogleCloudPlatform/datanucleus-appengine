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
package org.datanucleus.store.appengine.query;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.QueryResultIterator;

import org.datanucleus.store.appengine.JPATestCase;
import org.datanucleus.store.appengine.Utils;
import org.datanucleus.test.Book;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.persistence.FlushModeType;
import javax.persistence.Query;


/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JPQLCursorTest extends JPATestCase {

  public void testGetCursor_List() {
    Entity e1 = Book.newBookEntity("auth", "34", "yar");
    Entity e2 = Book.newBookEntity("auth", "34", "yar");
    Entity e3 = Book.newBookEntity("auth", "34", "yar");
    ldth.ds.put(Arrays.asList(e1, e2, e3));

    beginTxn();
    Query q = em.createQuery("select from " + Book.class.getName());
    q.setMaxResults(1);
    List<Book> books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(e1.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
    Cursor c = JPACursorHelper.getCursor(books);
    assertNotNull(c);

    q.setHint(JPACursorHelper.CURSOR_HINT, c);
    books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(e2.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
    assertNotNull(JPACursorHelper.getCursor(books));

    q.setHint(JPACursorHelper.CURSOR_HINT, c.toWebSafeString());
    books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(e2.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
    c = JPACursorHelper.getCursor(books);
    assertNotNull(c);

    q.setHint(JPACursorHelper.CURSOR_HINT, c);
    books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(e3.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
    assertNotNull(JPACursorHelper.getCursor(books));

    q.setHint(JPACursorHelper.CURSOR_HINT, c.toWebSafeString());
    books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(e3.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
    assertNotNull(JPACursorHelper.getCursor(books));

    commitTxn();
  }

  public void testCursorEquality() {
    List<Key> keys = Utils.newArrayList();
    keys.add(ldth.ds.put(Book.newBookEntity("auth", "34", "yar")));
    keys.add(ldth.ds.put(Book.newBookEntity("auth", "34", "yar")));
    keys.add(ldth.ds.put(Book.newBookEntity("auth", "34", "yar")));

    com.google.appengine.api.datastore.Query query = new com.google.appengine.api.datastore.Query("Book");
    QueryResultIterator entityIter = ldth.ds.prepare(query).asQueryResultIterator();
    List<Cursor> lowLevelCursors = Utils.newArrayList();
    lowLevelCursors.add(entityIter.getCursor());
    entityIter.next();
    lowLevelCursors.add(entityIter.getCursor());
    entityIter.next();
    lowLevelCursors.add(entityIter.getCursor());
    entityIter.next();
    lowLevelCursors.add(entityIter.getCursor());

    Query q = em.createQuery("select from " + Book.class.getName());
    Iterator<Book> bookIter = q.getResultList().iterator();
    List<Cursor> ormCursors = Utils.newArrayList();
    ormCursors.add(JPACursorHelper.getCursor(bookIter));
    bookIter.next();
    ormCursors.add(JPACursorHelper.getCursor(bookIter));
    bookIter.next();
    ormCursors.add(JPACursorHelper.getCursor(bookIter));
    bookIter.next();
    ormCursors.add(JPACursorHelper.getCursor(bookIter));

    assertEquals(lowLevelCursors, ormCursors);

    for (int i = 0; i < lowLevelCursors.size(); i++) {
      Cursor lowLevelCursor = lowLevelCursors.get(i);
      List<Entity> list = ldth.ds.prepare(query).asList(FetchOptions.Builder.withCursor(lowLevelCursor));
      assertEquals(3 - i, list.size());
    }
  }

  public void testGetCursor_Iterator() {
    List<Key> keys = Utils.newArrayList();
    for (int i = 0; i < 10; i++) {
      keys.add(ldth.ds.put(Book.newBookEntity("auth" + i, "34", "yar")));
    }

    beginTxn();
    Query q = em.createQuery("select from " + Book.class.getName());
    Iterator<Book> bookIter = q.getResultList().iterator();
    assertCursorResults(JPACursorHelper.getCursor(bookIter), keys);
    int index = 0;
    while (bookIter.hasNext()) {
      bookIter.next();
      assertCursorResults(JPACursorHelper.getCursor(bookIter), keys.subList(++index, keys.size()));
    }
    // setting a max result means we call asQueryResultList(), and there are no
    // intermediate cursors available from a List.
    q.setMaxResults(1);

    bookIter = q.getResultList().iterator();
    assertNull(JPACursorHelper.getCursor((bookIter)));
    while (bookIter.hasNext()) {
      bookIter.next();
      assertNull(JPACursorHelper.getCursor((bookIter)));
    }
  }

  public void testGetCursor_Iterable() {
    List<Key> keys = Utils.newArrayList();
    for (int i = 0; i < 10; i++) {
      keys.add(ldth.ds.put(Book.newBookEntity("auth" + i, "34", "yar")));
    }

    beginTxn();
    Query q = em.createQuery("select from " + Book.class.getName());
    // no limit specified so what we get back doesn't have an end cursor
    List<Book> bookList = q.getResultList();
    assertNull(JPACursorHelper.getCursor(bookList));
    for (Book b : bookList) {
      // no cursor
      assertNull(JPACursorHelper.getCursor(bookList));
    }
  }

  private void assertCursorResults(Cursor cursor, List<Key> expectedKeys) {
    Query q = em.createQuery("select from " + Book.class.getName());
    q.setHint(JPACursorHelper.QUERY_CURSOR_PROPERTY_NAME, cursor);
    // if FlushModeType is AUTO and there is another query in progress
    // in this txn, that query result will get flushed before this one
    // runs, and as a result the cursor will get bumped to the end of the
    // result set, which is not what we want for this test.  We set the
    // FlushModeType to COMMIT to prevent the flush from happening.
    q.setFlushMode(FlushModeType.COMMIT);
    List<Key> keys = Utils.newArrayList();
    for (Object b : q.getResultList()) {
      keys.add(KeyFactory.stringToKey(((Book) b).getId()));
    }
    assertEquals(expectedKeys, keys);
  }
}
