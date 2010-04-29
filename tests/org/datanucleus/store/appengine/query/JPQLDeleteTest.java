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
package org.datanucleus.store.appengine.query;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.store.appengine.DatastoreManager;
import org.datanucleus.store.appengine.JPATestCase;
import org.datanucleus.store.appengine.Utils;
import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyAncestorKeyPkJPA;
import org.datanucleus.test.HasOneToManyListJPA;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPQLDeleteTest extends JPATestCase {

  public void testDelete_Txn_MultipleEntityGroups() {
    ds.put(Book.newBookEntity("Bar Book", "Joe Blow", "67890"));
    ds.put(Book.newBookEntity("Bar Book", "Joe Blow", "67891"));

    Query q = em.createQuery("DELETE FROM " + Book.class.getName());
    beginTxn();
    try {
      q.executeUpdate();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good - can't delete books from multiple entity groups in a txn
    }
    rollbackTxn();
    assertEquals(2, countForClass(Book.class));
  }

  public void testDelete_Txn_OneEntityGroup() {
    ds.put(Book.newBookEntity("Bar Book", "Joe Blow", "67890"));
    ds.put(Book.newBookEntity("Bar Book", "Joe Blow", "67891"));

    Query q = em.createQuery("DELETE FROM " + Book.class.getName());
    beginTxn();
    try {
      q.executeUpdate();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good - can't delete books from multiple entity groups in a txn
    }
    rollbackTxn();
    assertEquals(2, countForClass(Book.class));
  }

  public void testDelete_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    ds.put(Book.newBookEntity("Bar Book", "Joe Blow", "67890"));
    ds.put(Book.newBookEntity("Bar Book", "Joe Blow", "67891"));

    Query q = em.createQuery("DELETE FROM " + Book.class.getName());
    assertEquals(2, q.executeUpdate());
    assertEquals(0, countForClass(Book.class));
  }

  public void testDeleteAncestorQuery_Txn() {
    Key parentKey = KeyFactory.createKey("yar", 23);
    Entity pojo1 = new Entity(HasKeyAncestorKeyPkJPA.class.getSimpleName(), parentKey);
    Entity pojo2 = new Entity(HasKeyAncestorKeyPkJPA.class.getSimpleName(), parentKey);

    ds.put(pojo1);
    ds.put(pojo2);

    Query q = em.createQuery("DELETE FROM " + HasKeyAncestorKeyPkJPA.class.getName() + " WHERE ancestorKey = :p1");
    q.setParameter("p1", parentKey);
    beginTxn();
    assertEquals(2, q.executeUpdate());
    commitTxn();
    assertEquals(0, countForClass(HasKeyAncestorKeyPkJPA.class));
  }

  public void testDeleteAncestorQuery_TxnRollback() throws EntityNotFoundException {
    Key parentKey = KeyFactory.createKey("yar", 23);
    Entity pojo1 = new Entity(HasKeyAncestorKeyPkJPA.class.getSimpleName(), parentKey);
    Entity pojo2 = new Entity(HasKeyAncestorKeyPkJPA.class.getSimpleName(), parentKey);

    ds.put(pojo1);
    ds.put(pojo2);

    Query q = em.createQuery("DELETE FROM " + HasKeyAncestorKeyPkJPA.class.getName() + " WHERE ancestorKey = :p1");
    q.setParameter("p1", parentKey);
    beginTxn();
    assertEquals(2, q.executeUpdate());
    rollbackTxn();
    assertEquals(2, countForClass(HasKeyAncestorKeyPkJPA.class));
  }

  public void testDeleteAncestorQuery_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Key parentKey = KeyFactory.createKey("yar", 23);
    Entity pojo1 = new Entity(HasKeyAncestorKeyPkJPA.class.getSimpleName(), parentKey);
    Entity pojo2 = new Entity(HasKeyAncestorKeyPkJPA.class.getSimpleName(), parentKey);

    ds.put(pojo1);
    ds.put(pojo2);

    Query q = em.createQuery("DELETE FROM " + HasKeyAncestorKeyPkJPA.class.getName() + " WHERE ancestorKey = :p1");
    q.setParameter("p1", parentKey);
    assertEquals(2, q.executeUpdate());
    assertEquals(0, countForClass(HasKeyAncestorKeyPkJPA.class));
  }

  public void testBatchDelete_NoTxn_FastButInaccurate() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e1 = Book.newBookEntity("title", "auth", "123432", -40);
    ds.put(e1);
    Entity e2 = Book.newBookEntity("title", "auth", "123432", -40);
    ds.put(e2);
    Entity e3 = Book.newBookEntity("title", "auth", "123432", -40);
    ds.put(e3);

    Key key = KeyFactory.createKey("yar", "does not exist");
    Query q = em.createQuery("delete from " + Book.class.getName() + " where id = :ids");
    q.setParameter("ids", Utils.newArrayList(key, e1.getKey(), e2.getKey()));
    assertEquals(3, q.executeUpdate());
    assertEquals(1, countForClass(Book.class));
  }

  public void testBatchDelete_NoTxn_SlowButAccurate() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e1 = Book.newBookEntity("title", "auth", "123432", -40);
    ds.put(e1);
    Entity e2 = Book.newBookEntity("title", "auth", "123432", -40);
    ds.put(e2);
    Entity e3 = Book.newBookEntity("title", "auth", "123432", -40);
    ds.put(e3);

    Key key = KeyFactory.createKey("yar", "does not exist");
    Query q = em.createQuery("delete from " + Book.class.getName() + " where id = :ids");
    q.setHint(DatastoreManager.SLOW_BUT_MORE_ACCURATE_JPQL_DELETE_QUERY, true);
    q.setParameter("ids", Utils.newArrayList(key, e1.getKey(), e2.getKey()));
    assertEquals(2, q.executeUpdate());
    assertEquals(1, countForClass(Book.class));
  }

  public void testBatchDelete_Txn() {
    Key parent = KeyFactory.createKey("yar", 23);
    Entity e1 = Book.newBookEntity(parent, "title", "auth", "123432", -40);
    ds.put(e1);
    Entity e2 = Book.newBookEntity(parent, "title", "auth", "123432", -40);
    ds.put(e2);
    Entity e3 = Book.newBookEntity(parent, "title", "auth", "123432", -40);
    ds.put(e3);

    beginTxn();
    Query q = em.createQuery("delete from " + Book.class.getName() + " where id = :ids");
    q.setParameter("ids", Utils.newArrayList(parent, e1.getKey(), e2.getKey()));
    assertEquals(3, q.executeUpdate());
    assertEquals(3, countForClass(Book.class));
    commitTxn();
    assertEquals(1, countForClass(Book.class));
  }

  public void testDeleteDoesNotCascade() {
    HasOneToManyListJPA parent = new HasOneToManyListJPA();
    Book b = new Book();
    b.setAuthor("author");
    parent.getBooks().add(b);
    beginTxn();
    em.persist(parent);
    commitTxn();
    assertEquals(1, countForClass(Book.class));
    assertEquals(1, countForClass(HasOneToManyListJPA.class));
    beginTxn();
    Query q = em.createQuery("delete from " + HasOneToManyListJPA.class.getName());
    assertEquals(1, q.executeUpdate());
    assertEquals(1, countForClass(Book.class));
    assertEquals(1, countForClass(HasOneToManyListJPA.class));
    commitTxn();
    assertEquals(1, countForClass(Book.class));
    assertEquals(0, countForClass(HasOneToManyListJPA.class));
  }
}
