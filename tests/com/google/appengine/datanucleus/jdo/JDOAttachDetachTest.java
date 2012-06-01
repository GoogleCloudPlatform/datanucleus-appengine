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
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.jdo.BidirectionalChildListJDO;
import com.google.appengine.datanucleus.test.jdo.DetachableJDO;
import com.google.appengine.datanucleus.test.jdo.DetachableWithMultiValuePropsJDO;
import com.google.appengine.datanucleus.test.jdo.Flight;
import com.google.appengine.datanucleus.test.jdo.HasOneToManyListJDO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.List;

import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.ObjectState;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOAttachDetachTest extends JDOTestCase {

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
    pm.setDetachAllOnCommit(true);
    beginTxn();
    DetachableJDO pojo = new DetachableJDO();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    pm.makePersistent(pojo);
    commitTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    assertEquals(Date.class, pojo.getDate().getClass());
    pm.close();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pm = pmf.getPersistenceManager();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));

    pojo = toBytesAndBack(pojo);

    assertEquals("yar", pojo.getVal());
    assertEquals(now, pojo.getDate());
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    beginTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pojo.setVal("not yar");
    Date newDate = new Date(pojo.getDate().getTime() + 1);
    pojo.getDate().setTime(newDate.getTime());
    assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(pojo));
    pm.makePersistent(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.createKey(DetachableJDO.class.getSimpleName(), pojo.getId()));
    assertEquals("not yar", e.getProperty("val"));
    assertEquals(newDate, e.getProperty("date"));
  }

  public void testSimpleSerializeWithoutTxns() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    pm.setDetachAllOnCommit(true);
    DetachableJDO pojo = new DetachableJDO();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    pm.makePersistent(pojo);

    // DN3 changes this from P_NEW to DETACHED_CLEAN since detachAllOnCommit
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));

    pm.close();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    assertEquals(Date.class, pojo.getDate().getClass());
    pm = pmf.getPersistenceManager();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));

    pojo = toBytesAndBack(pojo);

    assertEquals("yar", pojo.getVal());
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pojo.setVal("not yar");
    Date newDate = new Date(pojo.getDate().getTime() + 1);
    pojo.getDate().setTime(newDate.getTime());
    assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(pojo));
    pm.makePersistent(pojo);
    pm.close();
    Entity e = ds.get(KeyFactory.createKey(DetachableJDO.class.getSimpleName(), pojo.getId()));
    assertEquals("not yar", e.getProperty("val"));
    assertEquals(newDate, e.getProperty("date"));
  }

  public void testSerializeWithMultiValueProps() throws Exception {
    pm.setDetachAllOnCommit(true);
    beginTxn();
    DetachableWithMultiValuePropsJDO pojo = new DetachableWithMultiValuePropsJDO();
    pojo.setStrList(Utils.newArrayList("c", "d"));
    pm.makePersistent(pojo);
    commitTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pm.close();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pm = pmf.getPersistenceManager();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));

    pojo = toBytesAndBack(pojo);

    assertEquals(Utils.newArrayList("c", "d"), pojo.getStrList());
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    beginTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    // reattach to the pm - this turns our regular list field into a managed
    // list field
    pojo = pm.makePersistent(pojo);
    assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(pojo));
    pojo.getStrList().add("e");
    commitTxn();
    Entity e = ds.get(KeyFactory.createKey(DetachableWithMultiValuePropsJDO.class.getSimpleName(), pojo.getId()));
    assertEquals(3, ((List<String>)e.getProperty("strList")).size());
    assertEquals(Utils.newArrayList("c", "d", "e"), e.getProperty("strList"));
  }

  public void testSerializeWithOneToMany_AddBidirectionalChildToDetached() throws Exception {
    pm.setDetachAllOnCommit(true);
    beginTxn();
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    pojo.setVal("yar");
    BidirectionalChildListJDO bidir = new BidirectionalChildListJDO();
    bidir.setChildVal("yar2");
    pojo.addBidirChild(bidir);
    pm.makePersistent(pojo);
    commitTxn();
    pm.close();
    pm = pmf.getPersistenceManager();

    pojo = toBytesAndBack(pojo);
    assertEquals("yar", pojo.getVal());
    assertEquals(1, pojo.getBidirChildren().size());
    BidirectionalChildListJDO bidir2 = new BidirectionalChildListJDO();
    bidir2.setChildVal("yar3");
    pojo.addBidirChild(bidir2);
    bidir2.setParent(pojo);
    beginTxn();
    pojo = pm.makePersistent(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertEquals(KeyFactory.stringToKey(pojo.getId()), e.getKey().getParent());
  }

  public void testSerializeWithOneToMany_AddUnidirectionalChildToDetached() throws Exception {
    pm.setDetachAllOnCommit(true);
    beginTxn();
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    pojo.setVal("yar");
    Flight flight = new Flight();
    flight.setName("harry");
    pojo.addFlight(flight);
    pm.makePersistent(pojo);
    commitTxn();
    pm.close();
    pm = pmf.getPersistenceManager();

    pojo = toBytesAndBack(pojo);
    assertEquals("yar", pojo.getVal());
    assertEquals(1, pojo.getFlights().size());
    Flight flight2 = new Flight();
    flight2.setName("not harry");
    pojo.addFlight(flight2);
    beginTxn();
    pojo = pm.makePersistent(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.stringToKey(flight2.getId()));
    assertEquals(KeyFactory.stringToKey(pojo.getId()), e.getKey().getParent());
  }

  public void testSerializeWithOneToMany_AddChildToReattached() throws Exception {
    pm.setDetachAllOnCommit(true);
    beginTxn();
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    pojo.setVal("yar");
    BidirectionalChildListJDO bidir = new BidirectionalChildListJDO();
    bidir.setChildVal("yar2");
    pojo.addBidirChild(bidir);
    pm.makePersistent(pojo);
    commitTxn();
    pm.close();
    pm = pmf.getPersistenceManager();

    pojo = toBytesAndBack(pojo);
    assertEquals("yar", pojo.getVal());
    assertEquals(1, pojo.getBidirChildren().size());
    beginTxn();
    pojo = pm.makePersistent(pojo);
    BidirectionalChildListJDO bidir2 = new BidirectionalChildListJDO();
    bidir.setChildVal("yar3");
    pojo.addBidirChild(bidir2);
    bidir2.setParent(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertEquals(KeyFactory.stringToKey(pojo.getId()), e.getKey().getParent());
  }

  public void testDeleteDetachedObject_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    DetachableJDO pojo = new DetachableJDO();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    pm.makePersistent(pojo);
    pm.close();
    pm = pmf.getPersistenceManager();
    pojo = pm.detachCopy(pm.getObjectById(pojo.getClass(), pojo.getId()));
    pm.close();
    pm = pmf.getPersistenceManager();
    pm.deletePersistent(pojo);
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pm.close();
    pm = pmf.getPersistenceManager();
    try {
      pm.getObjectById(pojo.getClass(), pojo.getId());
      fail("expected exception");
    } catch (JDOObjectNotFoundException e) {
      // good
    }
  }

  public void testDeleteDetachedNewObject_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    DetachableJDO pojo = new DetachableJDO();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    pm.makePersistent(pojo);
    pojo = pm.detachCopy(pojo);
    pm.close();
    pm = pmf.getPersistenceManager();
    pm.deletePersistent(pojo);
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pm.close();
    pm = pmf.getPersistenceManager();
    try {
      pm.getObjectById(pojo.getClass(), pojo.getId());
      fail("expected exception");
    } catch (JDOObjectNotFoundException e) {
      // good
    }
  }

  public void testDeleteDetachedObject_Txn() {
    beginTxn();
    DetachableJDO pojo = new DetachableJDO();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    pojo = pm.detachCopy(pm.getObjectById(pojo.getClass(), pojo.getId()));
    commitTxn();
    beginTxn();
    pm.deletePersistent(pojo);
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    commitTxn();
    beginTxn();
    try {
      pm.getObjectById(pojo.getClass(), pojo.getId());
      fail("expected exception");
    } catch (JDOObjectNotFoundException e) {
      // good
    }
    rollbackTxn();
  }

  public void testDeleteDetachedNewObject_Txn() {
    beginTxn();
    DetachableJDO pojo = new DetachableJDO();
    pojo.setVal("yar");
    Date now = new Date();
    pojo.setDate(now);
    pm.makePersistent(pojo);
    pojo = pm.detachCopy(pojo);
    commitTxn();
    beginTxn();
    pm.deletePersistent(pojo);
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    commitTxn();
    beginTxn();
    try {
      pm.getObjectById(pojo.getClass(), pojo.getId());
      fail("expected exception");
    } catch (JDOObjectNotFoundException e) {
      // good
    }
    rollbackTxn();
  }
}
