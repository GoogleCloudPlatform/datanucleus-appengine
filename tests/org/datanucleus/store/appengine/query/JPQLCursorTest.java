/*
 * Copyright (C) 2009 Max Ross.
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
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.store.appengine.JPATestCase;
import org.datanucleus.test.Book;

import java.util.Arrays;
import java.util.List;

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
    assertNull(JPACursorHelper.getCursor(books));

    q.setHint(JPACursorHelper.CURSOR_HINT, c.toWebSafeString());
    books = (List<Book>) q.getResultList();
    assertEquals(1, books.size());
    assertEquals(e3.getKey(), KeyFactory.stringToKey(books.get(0).getId()));
    assertNull(JPACursorHelper.getCursor(books));

    commitTxn();
  }

}
