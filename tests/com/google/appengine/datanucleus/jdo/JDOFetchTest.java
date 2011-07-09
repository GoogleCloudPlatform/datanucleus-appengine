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
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.DatastoreServiceInterceptor;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.WriteBlocker;
import com.google.appengine.datanucleus.test.Flight;
import com.google.appengine.datanucleus.test.HasKeyAncestorStringPkJDO;
import com.google.appengine.datanucleus.test.HasKeyPkJDO;
import com.google.appengine.datanucleus.test.HasMultiValuePropsJDO;
import com.google.appengine.datanucleus.test.HasStringAncestorKeyPkJDO;
import com.google.appengine.datanucleus.test.KitchenSink;
import com.google.appengine.datanucleus.test.Person;


import java.util.ArrayList;
import java.util.List;

import javax.jdo.JDOObjectNotFoundException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOFetchTest extends JDOTestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    DatastoreServiceInterceptor.install(getStoreManager(), new WriteBlocker());
    beginTxn();
  }

  @Override
  protected void tearDown() throws Exception {
    if (pm.currentTransaction().isActive()) {
      commitTxn();
    }
    try {
      super.tearDown();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
  }

  public void testSimpleFetch_Id() {
    Key key = ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));
    commitTxn();
    beginTxn();
    String keyStr = KeyFactory.keyToString(key);
    Flight flight = pm.getObjectById(Flight.class, keyStr);
    assertNotNull(flight);
    assertEquals(keyStr, flight.getId());
    assertEquals("yam", flight.getOrigin());
    assertEquals("bam", flight.getDest());
    assertEquals("1", flight.getName());
    assertEquals(1, flight.getYou());
    assertEquals(2, flight.getMe());
  }

  public void testSimpleFetch_Id_LongIdOnly() {
    Key key = ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));
    commitTxn();
    beginTxn();

    Flight flight = pm.getObjectById(Flight.class, key.getId());
    assertNotNull(flight);
    String keyStr = KeyFactory.keyToString(key);
    assertEquals(keyStr, flight.getId());
    assertEquals("yam", flight.getOrigin());
    assertEquals("bam", flight.getDest());
    assertEquals("1", flight.getName());
    assertEquals(1, flight.getYou());
    assertEquals(2, flight.getMe());
  }

  public void testSimpleFetch_Id_LongIdOnly_NotFound() {
    try {
      pm.getObjectById(Flight.class, -1);
      fail("expected onfe");
    } catch (JDOObjectNotFoundException e) {
      // good
    }
  }

  public void testSimpleFetch_Id_IntIdOnly() {
    Key key = ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));
    commitTxn();
    beginTxn();

    Flight flight = pm.getObjectById(Flight.class, Long.valueOf(key.getId()).intValue());
    assertNotNull(flight);
    String keyStr = KeyFactory.keyToString(key);
    assertEquals(keyStr, flight.getId());
    assertEquals("yam", flight.getOrigin());
    assertEquals("bam", flight.getDest());
    assertEquals("1", flight.getName());
    assertEquals(1, flight.getYou());
    assertEquals(2, flight.getMe());
  }

  public void testSimpleFetch_NamedKey() {
    Key key = ds.put(Flight.newFlightEntity("named key", "1", "yam", "bam", 1, 2));
    commitTxn();
    beginTxn();

    String keyStr = KeyFactory.keyToString(key);
    Flight flight = pm.getObjectById(Flight.class, keyStr);
    assertNotNull(flight);
    assertEquals(keyStr, flight.getId());
    assertEquals("named key", KeyFactory.stringToKey(flight.getId()).getName());
  }

  public void testSimpleFetch_NamedKey_NameOnly() {
    Key key = ds.put(Flight.newFlightEntity("named key", "1", "yam", "bam", 1, 2));
    commitTxn();
    beginTxn();

    Flight flight = pm.getObjectById(Flight.class, "named key");
    assertNotNull(flight);
    assertEquals(KeyFactory.keyToString(key), flight.getId());
    assertEquals("named key", KeyFactory.stringToKey(flight.getId()).getName());
  }

  public void testSimpleFetch_NamedKey_NameOnly_NotFound() {
    try {
      pm.getObjectById(Flight.class, "does not exist");
      fail("expected onfe");
    } catch (JDOObjectNotFoundException e) {
      // good
    }
  }

  public void testFetchNonExistent() {
    Key key = ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));
    ds.delete(key);
    String keyStr = KeyFactory.keyToString(key);
    try {
      pm.getObjectById(Flight.class, keyStr);
      fail("expected onfe");
    } catch (JDOObjectNotFoundException onfe) {
      // good
    }
  }

  public void testKitchenSinkFetch() {
    Key key = ds.put(KitchenSink.newKitchenSinkEntity(null));
    commitTxn();
    beginTxn();

    String keyStr = KeyFactory.keyToString(key);
    KitchenSink ks = pm.detachCopy(pm.getObjectById(KitchenSink.class, keyStr));
    assertNotNull(ks);
    assertEquals(keyStr, ks.key);
    assertEquals(KitchenSink.newKitchenSink(ks.key), ks);
  }

  public void testFetchWithKeyPk() {
    Entity e = new Entity(HasKeyPkJDO.class.getSimpleName());
    ds.put(e);
    commitTxn();
    beginTxn();
    HasKeyPkJDO hk = pm.getObjectById(HasKeyPkJDO.class, e.getKey());
    assertNotNull(hk.getKey());
    assertNull(hk.getAncestorKey());
  }

  public void testFetchWithKeyPkAndAncestor() {
    Entity parent = new Entity("yam");
    ds.put(parent);
    Entity child = new Entity(HasKeyPkJDO.class.getSimpleName(), parent.getKey());
    ds.put(child);
    commitTxn();
    beginTxn();
    HasKeyPkJDO hk = pm.getObjectById(HasKeyPkJDO.class, child.getKey());
    assertNotNull(hk.getKey());
    assertEquals(parent.getKey(), hk.getAncestorKey());
  }

  public void testFetchWithKeyPkAndStringAncestor() {
    Entity parent = new Entity("yam");
    ds.put(parent);
    Entity child = new Entity(HasStringAncestorKeyPkJDO.class.getSimpleName(), parent.getKey());
    ds.put(child);
    commitTxn();
    beginTxn();
    HasStringAncestorKeyPkJDO hk =
        pm.getObjectById(HasStringAncestorKeyPkJDO.class, child.getKey());
    assertNotNull(hk.getKey());
    assertEquals(parent.getKey(), KeyFactory.stringToKey(hk.getAncestorKey()));
  }

  public void testFetchWithStringPkAndKeyAncestor() {
    Entity parent = new Entity("yam");
    ds.put(parent);
    Entity child = new Entity(HasKeyAncestorStringPkJDO.class.getSimpleName(), parent.getKey());
    ds.put(child);
    commitTxn();
    beginTxn();
    HasKeyAncestorStringPkJDO hk =
        pm.getObjectById(HasKeyAncestorStringPkJDO.class, KeyFactory.keyToString(child.getKey()));
    assertNotNull(hk.getKey());
    assertEquals(parent.getKey(), hk.getAncestorKey());
  }

  public void testFetchWithWrongIdType_Key() {
    Entity entity = new Entity(HasStringAncestorKeyPkJDO.class.getSimpleName());
    ds.put(entity);
    commitTxn();
    beginTxn();

    // The model object's id is of type Key but we're going to look it up using
    // a string-encoded Key
    pm.getObjectById(HasStringAncestorKeyPkJDO.class, KeyFactory.keyToString(entity.getKey()));
  }

  public void testFetchWithWrongIdType_String() {
    Entity entity = new Entity(HasKeyAncestorStringPkJDO.class.getSimpleName());
    ds.put(entity);
    commitTxn();
    beginTxn();

    // The model object's id is of type String but we're going to look it up using
    // a Key
    pm.getObjectById(HasKeyAncestorStringPkJDO.class, entity.getKey());
  }

  public void testEmbeddable() {
    Entity e = new Entity(Person.class.getSimpleName());
    e.setProperty("first", "jimmy");
    e.setProperty("last", "jam");
    e.setProperty("anotherFirst", "anotherjimmy");
    e.setProperty("anotherLast", "anotherjam");
    ds.put(e);
    commitTxn();
    beginTxn();
    Person p = pm.getObjectById(Person.class, KeyFactory.keyToString(e.getKey()));
    assertNotNull(p);
    assertNotNull(p.getName());
    assertEquals("jimmy", p.getName().getFirst());
    assertEquals("jam", p.getName().getLast());
    assertNotNull(p.getAnotherName());
    assertEquals("anotherjimmy", p.getAnotherName().getFirst());
    assertEquals("anotherjam", p.getAnotherName().getLast());
  }

  public void testEmbeddableWithNull() {
    Entity e = new Entity(Person.class.getSimpleName());
    e.setProperty("first", "jimmy");
    e.setProperty("last", "jam");
    ds.put(e);
    commitTxn();
    beginTxn();
    Person p = pm.getObjectById(Person.class, KeyFactory.keyToString(e.getKey()));
    assertNotNull(p);
    assertNotNull(p.getName());
    assertEquals("jimmy", p.getName().getFirst());
    assertEquals("jam", p.getName().getLast());
    assertNotNull(p.getAnotherName());
    assertNull(p.getAnotherName().getFirst());
    assertNull(p.getAnotherName().getLast());
  }

  public void testFetchSet() {
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    e.setProperty("strSet", Utils.newArrayList("a", "b", "c"));
    ds.put(e);
    if (ds.getCurrentTransaction(null) != null) {
      commitTxn();
      beginTxn();
    }
    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, e.getKey().getId());
    assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrSet());
  }

  public void testFetchSetNonTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testFetchSet();
  }

  public void testFetchArrayList() {
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    e.setProperty("strArrayList", Utils.newArrayList("a", "b", "c"));
    ds.put(e);
    if (ds.getCurrentTransaction(null) != null) {
      commitTxn();
      beginTxn();
    }

    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, e.getKey().getId());
    assertEquals(Utils.newArrayList("a", "b", "c"), pojo.getStrArrayList());
  }

  public void testFetchArrayListNonTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testFetchArrayList();
  }

  public void testFetchList() {
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    e.setProperty("strList", Utils.newArrayList("a", "b", "c"));
    ds.put(e);
    if (ds.getCurrentTransaction(null) != null) {
      commitTxn();
      beginTxn();
    }

    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, e.getKey().getId());
    assertEquals(Utils.newArrayList("a", "b", "c"), pojo.getStrList());
  }

  public void testFetchListNonTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testFetchList();
  }

  public void testFetchLinkedList() {
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    e.setProperty("strLinkedList", Utils.newArrayList("a", "b", "c"));
    ds.put(e);
    if (ds.getCurrentTransaction(null) != null) {
      commitTxn();
      beginTxn();
    }

    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, e.getKey().getId());
    assertEquals(Utils.newLinkedList("a", "b", "c"), pojo.getStrLinkedList());
  }

  public void testFetchLinkedListNonTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testFetchLinkedList();
  }

  public void testFetchHashSet() {
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    e.setProperty("strHashSet", Utils.newArrayList("a", "b", "c"));
    ds.put(e);
    if (ds.getCurrentTransaction(null) != null) {
      commitTxn();
      beginTxn();
    }

    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, e.getKey().getId());
    assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrHashSet());
  }

  public void testFetchHashSetNonTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testFetchHashSet();
  }

  public void testFetchLinkedHashSet() {
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    e.setProperty("strLinkedHashSet", Utils.newArrayList("a", "b", "c"));
    ds.put(e);
    if (ds.getCurrentTransaction(null) != null) {
      commitTxn();
      beginTxn();
    }

    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, e.getKey().getId());
    assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrLinkedHashSet());
  }

  public void testFetchLinkedHashSetNonTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testFetchLinkedHashSet();
  }

  public void testFetchSortedSet() {
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    e.setProperty("strSortedSet", Utils.newArrayList("a", "b", "c"));
    ds.put(e);
    if (ds.getCurrentTransaction(null) != null) {
      commitTxn();
      beginTxn();
    }

    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, e.getKey().getId());
    assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrSortedSet());
  }

  public void testFetchSortedSetNonTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testFetchSortedSet();
  }

  public void testFetchTreeSet() {
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    e.setProperty("strTreeSet", Utils.newArrayList("c", "b", "a"));
    ds.put(e);
    if (ds.getCurrentTransaction(null) != null) {
      commitTxn();
      beginTxn();
    }

    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, e.getKey().getId());
    assertEquals(Utils.newTreeSet("a", "b", "c"), pojo.getStrTreeSet());
  }

  public void testFetchTreeSetNonTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testFetchTreeSet();
  }

  public void testFetchCollection() {
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    e.setProperty("intColl", Utils.newArrayList(2, 3, 4));
    ds.put(e);
    if (ds.getCurrentTransaction(null) != null) {
      commitTxn();
      beginTxn();
    }

    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, e.getKey().getId());
    assertEquals(Utils.newArrayList(2, 3, 4), pojo.getIntColl());
  }

  public void testFetchCollectionNonTxn() {
    commitTxn();
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testFetchCollection();
  }

  public void testNumberTooLarge() {
    Entity e = Flight.newFlightEntity("yar", "yar", "bos", "mia", 0, 44);
    // set the value of one of the int properties to be too big for an int field
    e.setProperty("you", Integer.MAX_VALUE + 1L);
    ds.put(e);
    commitTxn();
    beginTxn();

    Flight f = pm.getObjectById(Flight.class, "yar");
    // no exception, just overflow
    assertEquals(-2147483648, f.getYou());
  }

  public void testMultiValuePropWithOneNullElement() {
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    List<String> oneNullElement = new ArrayList<String>();
    oneNullElement.add(null);
    e.setProperty("strList", oneNullElement);
    Key key = ds.put(e);
    commitTxn();
    beginTxn();
    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, key);
    assertEquals(1, pojo.getStrList().size());
    assertNull(pojo.getStrList().get(0));
  }
}
