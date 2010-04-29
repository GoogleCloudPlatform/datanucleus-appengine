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
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasMultiValuePropsJPA;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAFetchTest extends JPATestCase {

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

  @Override
  protected EntityManagerFactoryName getEntityManagerFactoryName() {
    return EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed;
  }

  public void testSimpleFetch_Id() {
    Key key = ds.put(Book.newBookEntity("max", "47", "yam"));

    String keyStr = KeyFactory.keyToString(key);
    Book book = em.find(Book.class, keyStr);
    assertNotNull(book);
    assertEquals(keyStr, book.getId());
    assertEquals("max", book.getAuthor());
    assertEquals("47", book.getIsbn());
    assertEquals("yam", book.getTitle());
  }

  public void testSimpleFetchWithNonTransactionalDatasource() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Key key = ds.put(Book.newBookEntity("max", "47", "yam"));

    String keyStr = KeyFactory.keyToString(key);
    Book book = em.find(Book.class, keyStr);
    assertNotNull(book);
    assertEquals(keyStr, book.getId());
    assertEquals("max", book.getAuthor());
    assertEquals("47", book.getIsbn());
    assertEquals("yam", book.getTitle());
  }

  public void testSimpleFetch_Id_LongIdOnly() {
    Key key = ds.put(Book.newBookEntity("max", "47", "yam"));

    Book book = em.find(Book.class, key.getId());
    assertNotNull(book);
    String keyStr = KeyFactory.keyToString(key);
    assertEquals(keyStr, book.getId());
    assertEquals("max", book.getAuthor());
    assertEquals("47", book.getIsbn());
    assertEquals("yam", book.getTitle());
  }

  public void testSimpleFetch_Id_LongIdOnly_NotFound() {
    assertNull(em.find(Book.class, -1));
  }

  public void testSimpleFetch_Id_IntIdOnly() {
    Key key = ds.put(Book.newBookEntity("max", "47", "yam"));

    Book book = em.find(Book.class, Long.valueOf(key.getId()).intValue());
    assertNotNull(book);
    String keyStr = KeyFactory.keyToString(key);
    assertEquals(keyStr, book.getId());
    assertEquals("max", book.getAuthor());
    assertEquals("47", book.getIsbn());
    assertEquals("yam", book.getTitle());
  }

  public void testSimpleFetchWithNamedKey() {
    Key key = ds.put(Book.newBookEntity("named key", "max", "47", "yam"));

    String keyStr = KeyFactory.keyToString(key);
    Book book = em.find(Book.class, keyStr);
    assertNotNull(book);
    assertEquals(keyStr, book.getId());
    assertEquals("max", book.getAuthor());
    assertEquals("47", book.getIsbn());
    assertEquals("yam", book.getTitle());
    assertEquals("named key", KeyFactory.stringToKey(book.getId()).getName());
  }

  public void testSimpleFetchWithNamedKey_NameOnly() {
    Key key = ds.put(Book.newBookEntity("named key", "max", "47", "yam"));

    Book book = em.find(Book.class, key.getName());
    assertNotNull(book);
    String keyStr = KeyFactory.keyToString(key);
    assertEquals(keyStr, book.getId());
    assertEquals("max", book.getAuthor());
    assertEquals("47", book.getIsbn());
    assertEquals("yam", book.getTitle());
    assertEquals("named key", KeyFactory.stringToKey(book.getId()).getName());
  }

  public void testSimpleFetchWithNamedKey_NameOnly_NotFound() {
    assertNull(em.find(Book.class, "does not exist"));
  }

  public void testFetchNonExistent() {
    Key key = ds.put(Book.newBookEntity("max", "47", "yam"));
    ds.delete(key);
    String keyStr = KeyFactory.keyToString(key);
    assertNull(em.find(Book.class, keyStr));
  }

  public void testFetchSet() {
    Entity e = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    e.setProperty("strSet", Utils.newArrayList("a", "b", "c"));
    ds.put(e);

    HasMultiValuePropsJPA pojo = em.find(HasMultiValuePropsJPA.class, e.getKey().getId());
    assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrSet());
  }

  public void testFetchSetNonTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    testFetchSet();
  }

  public void testFetchArrayList() {
    Entity e = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    e.setProperty("strArrayList", Utils.newArrayList("a", "b", "c"));
    ds.put(e);

    HasMultiValuePropsJPA pojo = em.find(HasMultiValuePropsJPA.class, e.getKey().getId());
    assertEquals(Utils.newArrayList("a", "b", "c"), pojo.getStrArrayList());
  }

  public void testFetchArrayListNonTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    testFetchArrayList();
  }

  public void testFetchList() {
    Entity e = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    e.setProperty("strList", Utils.newArrayList("a", "b", "c"));
    ds.put(e);

    HasMultiValuePropsJPA pojo = em.find(HasMultiValuePropsJPA.class, e.getKey().getId());
    assertEquals(Utils.newArrayList("a", "b", "c"), pojo.getStrList());
  }

  public void testFetchListNonTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    testFetchList();
  }

  public void testFetchLinkedList() {
    Entity e = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    e.setProperty("strLinkedList", Utils.newArrayList("a", "b", "c"));
    ds.put(e);

    HasMultiValuePropsJPA pojo = em.find(HasMultiValuePropsJPA.class, e.getKey().getId());
    assertEquals(Utils.newLinkedList("a", "b", "c"), pojo.getStrLinkedList());
  }

  public void testFetchLinkedListNonTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    testFetchLinkedList();
  }

  public void testFetchHashSet() {
    Entity e = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    e.setProperty("strHashSet", Utils.newArrayList("a", "b", "c"));
    ds.put(e);

    HasMultiValuePropsJPA pojo = em.find(HasMultiValuePropsJPA.class, e.getKey().getId());
    assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrHashSet());
  }

  public void testFetchHashSetNonTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    testFetchHashSet();
  }

  public void testFetchLinkedHashSet() {
    Entity e = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    e.setProperty("strLinkedHashSet", Utils.newArrayList("a", "b", "c"));
    ds.put(e);

    HasMultiValuePropsJPA pojo = em.find(HasMultiValuePropsJPA.class, e.getKey().getId());
    assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrLinkedHashSet());
  }

  public void testFetchLinkedHashSetNonTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    testFetchLinkedHashSet();
  }

  public void testFetchSortedSet() {
    Entity e = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    e.setProperty("strSortedSet", Utils.newArrayList("c", "b", "a"));
    ds.put(e);

    HasMultiValuePropsJPA pojo = em.find(HasMultiValuePropsJPA.class, e.getKey().getId());
    assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrSortedSet());
  }

  public void testFetchSortedSetNonTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    testFetchSortedSet();
  }

  public void testFetchTreeSet() {
    Entity e = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    e.setProperty("strTreeSet", Utils.newArrayList("c", "b", "a"));
    ds.put(e);

    HasMultiValuePropsJPA pojo = em.find(HasMultiValuePropsJPA.class, e.getKey().getId());
    assertEquals(Utils.newTreeSet("a", "b", "c"), pojo.getStrTreeSet());
  }

  public void testFetchTreeSetNonTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    testFetchTreeSet();
  }

  public void testFetchCollection() {
    Entity e = new Entity(HasMultiValuePropsJPA.class.getSimpleName());
    e.setProperty("intColl", Utils.newArrayList(2, 3, 4));
    ds.put(e);

    HasMultiValuePropsJPA pojo = em.find(HasMultiValuePropsJPA.class, e.getKey().getId());
    assertEquals(Utils.newArrayList(2, 3, 4), pojo.getIntColl());
  }

  public void testFetchCollectionNonTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    testFetchCollection();
  }
}
