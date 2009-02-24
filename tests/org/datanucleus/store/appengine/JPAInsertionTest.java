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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyPkJPA;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAInsertionTest extends JPATestCase {

  public void testSimpleInsert() throws EntityNotFoundException {
    Book b1 = new Book();
    b1.setAuthor("jimmy");
    b1.setIsbn("isbn");
    b1.setTitle("the title");
    assertNull(b1.getId());
    beginTxn();
    em.persist(b1);
    commitTxn();
    assertNotNull(b1.getId());
    Entity entity = ldth.ds.get(KeyFactory.stringToKey(b1.getId()));
    assertNotNull(entity);
    assertEquals("jimmy", entity.getProperty("author"));
    assertEquals("isbn", entity.getProperty("isbn"));
    assertEquals("the title", entity.getProperty("title"));
    assertEquals(Book.class.getSimpleName(), entity.getKind());
  }

  public void testSimpleInsertWithNamedKey() throws EntityNotFoundException {
    Book b1 = new Book("named key");
    b1.setAuthor("jimmy");
    b1.setIsbn("isbn");
    b1.setTitle("the title");
    beginTxn();
    em.persist(b1);
    commitTxn();
    assertEquals("named key", KeyFactory.stringToKey(b1.getId()).getName());
    Entity entity = ldth.ds.get(KeyFactory.stringToKey(b1.getId()));
    assertNotNull(entity);
    assertEquals("jimmy", entity.getProperty("author"));
    assertEquals("isbn", entity.getProperty("isbn"));
    assertEquals("the title", entity.getProperty("title"));
    assertEquals(Book.class.getSimpleName(), entity.getKind());
    assertEquals("named key", entity.getKey().getName());
  }

  public void testInsertWithKeyPk() {
    HasKeyPkJPA hk = new HasKeyPkJPA();

    beginTxn();
    em.persist(hk);
    commitTxn();

    assertNotNull(hk.getId());
    assertNull(hk.getAncestorId());
  }



  public void testInsertWithNamedKeyPk() {
    HasKeyPkJPA hk = new HasKeyPkJPA();
    hk.setId(KeyFactory.createKey(HasKeyPkJPA.class.getSimpleName(), "name"));
    beginTxn();
    em.persist(hk);
    commitTxn();

    assertNotNull(hk.getId());
    assertEquals("name", hk.getId().getName());
  }
}
