/*
 * /**********************************************************************
 * Copyright (c) 2009 Google Inc.
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
 * **********************************************************************/

package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.test.BidirectionalChildListJPA;
import com.google.appengine.datanucleus.test.Book;
import com.google.appengine.datanucleus.test.DetachableJPA;
import com.google.appengine.datanucleus.test.DetachableWithMultiValuePropsJDO;
import com.google.appengine.datanucleus.test.HasGrandchildJPA;
import com.google.appengine.datanucleus.test.HasOneToManyListJPA;
import com.google.appengine.datanucleus.test.HasOneToManySetJPA;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.List;

import javax.jdo.JDOHelper;
import javax.jdo.ObjectState;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAAttachDetachTest extends JPATestCase {

  private <T extends Serializable> T toBytesAndBack(T obj)
      throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    return (T) ois.readObject();
  }

  public void testSimpleSerializeWithTxns()
      throws IOException, ClassNotFoundException, EntityNotFoundException {
    beginTxn();
    DetachableJPA pojo = new DetachableJPA();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    em.persist(pojo);
    commitTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    assertEquals(Date.class, pojo.getDate().getClass());
    em.close();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    em = emf.createEntityManager();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));

    pojo = toBytesAndBack(pojo);

    assertEquals("yar", pojo.getVal());
    assertEquals(now, pojo.getDate());
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    beginTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pojo.setVal("not yar");
    Date differentNow = new Date(now.getTime() + 1);
    pojo.setDate(differentNow);
    assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(pojo));
    pojo = em.merge(pojo);
    assertEquals(ObjectState.PERSISTENT_DIRTY, JDOHelper.getObjectState(pojo));
    commitTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    Entity e = ds.get(KeyFactory.createKey(DetachableJPA.class.getSimpleName(), pojo.getId()));
    assertEquals("not yar", e.getProperty("val"));
    assertEquals(differentNow, e.getProperty("date"));
  }

  public void testSimpleSerializeWithoutTxns() throws Exception {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    DetachableJPA pojo = new DetachableJPA();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    em.persist(pojo);
    assertEquals(ObjectState.PERSISTENT_NEW, JDOHelper.getObjectState(pojo));
    em.close();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    assertEquals(Date.class, pojo.getDate().getClass());
    em = emf.createEntityManager();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));

    pojo = toBytesAndBack(pojo);

    assertEquals("yar", pojo.getVal());
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pojo.setVal("not yar");
    Date differentNow = new Date(now.getTime() + 1);
    pojo.setDate(differentNow);
    assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(pojo));
    em.merge(pojo);
    em.close();
    Entity e = ds.get(KeyFactory.createKey(DetachableJPA.class.getSimpleName(), pojo.getId()));
    assertEquals("not yar", e.getProperty("val"));
    assertEquals(differentNow, e.getProperty("date"));
  }

  public void testMergeAfterFetch() throws Exception {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    DetachableJPA pojo = new DetachableJPA();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    em.persist(pojo);
    em.close();
    em = emf.createEntityManager();
//    beginTxn();
//    pojo = em.find(DetachableJPA.class, pojo.getId());
    pojo = (DetachableJPA) em.createQuery("select from " + DetachableJPA.class.getName()).getSingleResult();
//    commitTxn();
    em.close();
    em = emf.createEntityManager();
    assertEquals("yar", pojo.getVal());
    pojo.setVal("not yar");
    em.merge(pojo);
    em.close();
    Entity e = ds.get(KeyFactory.createKey(DetachableJPA.class.getSimpleName(), pojo.getId()));
    assertEquals("not yar", e.getProperty("val"));
  }

  public void testSerializeWithMultiValueProps() throws Exception {
    beginTxn();
    DetachableWithMultiValuePropsJDO pojo = new DetachableWithMultiValuePropsJDO();
    pojo.setStrList(Utils.newArrayList("c", "d"));
    em.persist(pojo);
    commitTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    em.close();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    em = emf.createEntityManager();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));

    pojo = toBytesAndBack(pojo);

    assertEquals(Utils.newArrayList("c", "d"), pojo.getStrList());
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    beginTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    // reattach to the pm - this turns our regular list field into a managed
    // list field
    pojo = em.merge(pojo);
    assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(pojo));
    pojo.getStrList().add("e");
    commitTxn();
    Entity e = ds.get(KeyFactory.createKey(DetachableWithMultiValuePropsJDO.class.getSimpleName(), pojo.getId()));
    assertEquals(3, ((List<String>)e.getProperty("strList")).size());
    assertEquals(Utils.newArrayList("c", "d", "e"), e.getProperty("strList"));
  }

  public void testSerializeWithOneToMany_AddChildToBidirectionalDetached() throws Exception {
    beginTxn();
    HasOneToManyListJPA pojo = new HasOneToManyListJPA();
    pojo.setVal("yar");
    BidirectionalChildListJPA bidir = new BidirectionalChildListJPA();
    bidir.setChildVal("yar2");
    pojo.getBidirChildren().add(bidir);
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();

    pojo = toBytesAndBack(pojo);
    assertEquals("yar", pojo.getVal());
    assertEquals(1, pojo.getBidirChildren().size());
    BidirectionalChildListJPA bidir2 = new BidirectionalChildListJPA();
    bidir.setChildVal("yar3");
    pojo.getBidirChildren().add(bidir2);
    // Don't set the parent - this ref won't get updated when we call
    // merge and we'll get an exception.
//    bidir2.setParent(pojo);
    beginTxn();
    pojo = em.merge(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertEquals(KeyFactory.stringToKey(pojo.getId()), e.getKey().getParent());
  }

  public void testSerializeWithOneToMany_AddChildToUnidirectionalDetached() throws Exception {
    beginTxn();
    HasOneToManyListJPA pojo = new HasOneToManyListJPA();
    pojo.setVal("yar");
    Book b = new Book();
    b.setAuthor("harry");
    pojo.getBooks().add(b);
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();

    pojo = toBytesAndBack(pojo);
    assertEquals("yar", pojo.getVal());
    assertEquals(1, pojo.getBooks().size());
    Book b2 = new Book();
    b2.setAuthor("yar3");
    pojo.getBooks().add(b2);
    // Don't set the parent - this ref won't get updated when we call
    // merge and we'll get an exception.
//    bidir2.setParent(pojo);
    beginTxn();
    pojo = em.merge(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.stringToKey(b2.getId()));
    assertEquals(KeyFactory.stringToKey(pojo.getId()), e.getKey().getParent());
  }

  public void testSerializeWithOneToMany_AddGrandchildToUnidirectionalDetached() throws Exception {
    beginTxn();
    HasGrandchildJPA hasGrandchild = new HasGrandchildJPA();
    HasOneToManySetJPA pojo = new HasOneToManySetJPA();
    hasGrandchild.getYar().add(pojo);
    pojo.setVal("yar");
    Book b = new Book();
    b.setAuthor("harry");
    pojo.getBooks().add(b);
    em.persist(hasGrandchild);
    commitTxn();
    em.close();
    em = emf.createEntityManager();

    hasGrandchild = toBytesAndBack(hasGrandchild);
    pojo = hasGrandchild.getYar().iterator().next();
    assertEquals("yar", pojo.getVal());
    assertEquals(1, pojo.getBooks().size());
    Book b2 = new Book();
    b2.setAuthor("yar3");
    pojo.getBooks().add(b2);
    // Don't set the parent - this ref won't get updated when we call
    // merge and we'll get an exception.
//    bidir2.setParent(pojo);
    beginTxn();
    hasGrandchild = em.merge(hasGrandchild);
    commitTxn();
    Entity e = ds.get(KeyFactory.stringToKey(b2.getId()));
    assertEquals(KeyFactory.stringToKey(pojo.getId()), e.getKey().getParent());
  }

  public void testSerializeWithOneToMany_AddChildToReattached() throws Exception {
    beginTxn();
    HasOneToManyListJPA pojo = new HasOneToManyListJPA();
    pojo.setVal("yar");
    BidirectionalChildListJPA bidir = new BidirectionalChildListJPA();
    bidir.setChildVal("yar2");
    pojo.getBidirChildren().add(bidir);
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();

    pojo = toBytesAndBack(pojo);
    assertEquals("yar", pojo.getVal());
    assertEquals(1, pojo.getBidirChildren().size());
    beginTxn();
    pojo = em.merge(pojo);
    BidirectionalChildListJPA bidir2 = new BidirectionalChildListJPA();
    bidir.setChildVal("yar3");
    pojo.getBidirChildren().add(bidir2);
    bidir2.setParent(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertEquals(KeyFactory.stringToKey(pojo.getId()), e.getKey().getParent());
  }
  
  public void testDeleteDetachedObject_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    DetachableJPA pojo = new DetachableJPA();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    em.persist(pojo);
    em.close();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    em = emf.createEntityManager();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(pojo));
    em.close();
    em = emf.createEntityManager();
    pojo = em.merge(pojo);
    // this is wrong and it will start to fail when we upgrade to DN 2.0
    // We're tracking this with bug
    // http://code.google.com/p/datanucleus-appengine/issues/detail?id=142
    assertEquals(ObjectState.PERSISTENT_NEW, JDOHelper.getObjectState(pojo));
//    assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(pojo));
//    em.remove(pojo);
//    assertEquals(ObjectState.PERSISTENT_NEW_DELETED, JDOHelper.getObjectState(pojo));
//    em.close();
//    em = emf.createEntityManager();
//    assertNull(em.find(pojo.getClass(), pojo.getId()));
  }
  
  public void testDeleteDetachedObject_Txn() {
    beginTxn();
    DetachableJPA pojo = new DetachableJPA();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    em.persist(pojo);
    commitTxn();
    Long id = pojo.getId();
    beginTxn();
    pojo = em.find(pojo.getClass(), pojo.getId());
    commitTxn();
    beginTxn();
    pojo = em.merge(pojo);
    em.remove(pojo);
    assertEquals(ObjectState.PERSISTENT_DELETED, JDOHelper.getObjectState(pojo));
    commitTxn();
    beginTxn();
    assertNull(em.find(pojo.getClass(), id));
    rollbackTxn();
  }


}