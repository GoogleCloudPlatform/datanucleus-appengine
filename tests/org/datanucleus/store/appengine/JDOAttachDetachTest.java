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

package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.BidirectionalChildListJDO;
import org.datanucleus.test.DetachableJDO;
import org.datanucleus.test.DetachableWithMultiValuePropsJDO;
import org.datanucleus.test.HasOneToManyListJDO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import javax.jdo.JDOHelper;
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
    pm.makePersistent(pojo);
    commitTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pm.close();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pm = pmf.getPersistenceManager();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));

    pojo = toBytesAndBack(pojo);

    assertEquals("yar", pojo.getVal());
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    beginTxn();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pojo.setVal("not yar");
    assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(pojo));
    pm.makePersistent(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(DetachableJDO.class.getSimpleName(), pojo.getId()));
    assertEquals("not yar", e.getProperty("val"));
  }

  public void testSimpleSerializeWithoutTxns() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    pm.setDetachAllOnCommit(true);
    DetachableJDO pojo = new DetachableJDO();
    pojo.setVal("yar");
    pm.makePersistent(pojo);
    assertEquals(ObjectState.PERSISTENT_NEW, JDOHelper.getObjectState(pojo));
    pm.close();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pm = pmf.getPersistenceManager();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));

    pojo = toBytesAndBack(pojo);

    assertEquals("yar", pojo.getVal());
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
    pojo.setVal("not yar");
    assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(pojo));
    pm.makePersistent(pojo);
    pm.close();
    Entity e = ldth.ds.get(KeyFactory.createKey(DetachableJDO.class.getSimpleName(), pojo.getId()));
    assertEquals("not yar", e.getProperty("val"));
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
    assertEquals(ObjectState.PERSISTENT_DIRTY, JDOHelper.getObjectState(pojo));
    pojo.getStrList().add("e");
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(DetachableWithMultiValuePropsJDO.class.getSimpleName(), pojo.getId()));
    assertEquals(3, ((List<String>)e.getProperty("strList")).size());
    assertEquals(Utils.newArrayList("c", "d", "e"), e.getProperty("strList"));
  }

  public void testSerializeWithOneToMany_AddChildToDetached() throws Exception {
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
    bidir.setChildVal("yar3");
    pojo.addBidirChild(bidir2);
    bidir2.setParent(pojo);
    beginTxn();
    pojo = pm.makePersistent(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(bidir2.getId()));
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
    Entity e = ldth.ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertEquals(KeyFactory.stringToKey(pojo.getId()), e.getKey().getParent());
  }
}
