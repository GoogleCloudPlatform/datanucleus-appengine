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

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasPromotedTypesJDO;
import org.datanucleus.test.HasVersionNoFieldJDO;
import org.datanucleus.test.HasVersionWithFieldJDO;
import org.datanucleus.test.KitchenSink;
import org.datanucleus.test.Name;
import org.datanucleus.test.Person;

import java.util.Arrays;

import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOInsertionTest extends JDOTestCase {

  private static final String DEFAULT_VERSION_PROPERTY_NAME = "OPT_VERSION";

  public void testSimpleInsert() throws EntityNotFoundException {
    Flight f1 = new Flight();
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setMe(2);
    f1.setYou(4);
    f1.setName("Harold");
    assertNull(f1.getId());
    makePersistentInTxn(f1, TXN_START_END);
    assertNotNull(f1.getId());
    Entity entity = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    assertNotNull(entity);
    assertEquals("BOS", entity.getProperty("origin"));
    assertEquals("MIA", entity.getProperty("dest"));
    assertEquals("Harold", entity.getProperty("name"));
    assertEquals(2L, entity.getProperty("me"));
    assertEquals(4L, entity.getProperty("you"));
    assertEquals(1L, entity.getProperty(DEFAULT_VERSION_PROPERTY_NAME));
    assertEquals(Flight.class.getSimpleName(), entity.getKind());
  }

  public void testSimpleInsertWithNamedKey() throws EntityNotFoundException {
    Flight f = new Flight();
    f.setId(KeyFactory.keyToString(KeyFactory.createKey(Flight.class.getSimpleName(), "foo")));
    assertNotNull(f.getId());
    makePersistentInTxn(f, TXN_START_END);
    Entity entity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(entity);
    assertEquals("foo", entity.getKey().getName());
  }

  public void testKitchenSinkInsert() throws EntityNotFoundException {
    KitchenSink ks = KitchenSink.newKitchenSink();
    assertNull(ks.key);
    makePersistentInTxn(ks, TXN_START_END);
    assertNotNull(ks.key);

    Entity entity = ldth.ds.get(KeyFactory.stringToKey(ks.key));
    assertNotNull(entity);
    assertEquals(KitchenSink.class.getSimpleName(), entity.getKind());

    Entity sameEntity = KitchenSink.newKitchenSinkEntity(KeyFactory.stringToKey(ks.key));
    assertFalse(entity.getProperties().keySet().contains("an extra property"));
    entity.setProperty("an extra property", "yar!");
    assertEquals(sameEntity.getProperties(), entity.getProperties());
  }

  public void testKitchenSinkInsertWithNulls() throws EntityNotFoundException {
    KitchenSink allNulls = new KitchenSink();
    makePersistentInTxn(allNulls, TXN_START_END);
    Entity entityWithNulls = ldth.ds.get(KeyFactory.stringToKey(allNulls.key));
    assertNotNull(entityWithNulls);

    // now create a KitchenSink with non-null values
    KitchenSink noNulls = KitchenSink.newKitchenSink();
    makePersistentInTxn(noNulls, TXN_START_END);
    Entity entityWithoutNulls = ldth.ds.get(KeyFactory.stringToKey(noNulls.key));
    assertNotNull(entityWithoutNulls);

    assertEquals(entityWithNulls.getProperties().keySet(), entityWithoutNulls.getProperties().keySet());
  }

  public void testVersionInserts() throws EntityNotFoundException {
    HasVersionNoFieldJDO hv = new HasVersionNoFieldJDO();
    makePersistentInTxn(hv, TXN_START_END);

    Entity entity = ldth.ds.get(
        KeyFactory.createKey(HasVersionNoFieldJDO.class.getSimpleName(), hv.getId()));
    assertNotNull(entity);
    assertEquals(1L, entity.getProperty("myversioncolumn"));

    HasVersionWithFieldJDO hvwf = new HasVersionWithFieldJDO();
    beginTxn();
    pm.makePersistent(hvwf);
    Long id = hvwf.getId();
    assertNotNull(id);
    commitTxn();
    beginTxn();
    hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, id);
    entity = ldth.ds.get(TestUtils.createKey(hvwf, id));
    assertNotNull(entity);
    assertEquals(1L, entity.getProperty(DEFAULT_VERSION_PROPERTY_NAME));
    assertEquals(1L, hvwf.getVersion());
    commitTxn();
  }

  public void testInsertWithKeyPk() {
    HasKeyPkJDO hk = new HasKeyPkJDO();

    beginTxn();
    pm.makePersistent(hk);

    assertNotNull(hk.getKey());
    assertNull(hk.getAncestorKey());
    commitTxn();
  }

  public void testInsertWithNamedKeyPk() {
    HasKeyPkJDO hk = new HasKeyPkJDO();
    hk.setKey(KeyFactory.createKey(HasKeyPkJDO.class.getSimpleName(), "name"));
    makePersistentInTxn(hk, TXN_START_END);

    assertNotNull(hk.getKey());
    assertEquals("name", hk.getKey().getName());
  }

  public void testEmbeddable() throws EntityNotFoundException {
    Person p = new Person();
    p.setName(new Name());
    p.getName().setFirst("jimmy");
    p.getName().setLast("jam");
    p.setAnotherName(new Name());
    p.getAnotherName().setFirst("anotherjimmy");
    p.getAnotherName().setLast("anotherjam");
    makePersistentInTxn(p, TXN_START_END);

    assertNotNull(p.getId());

    Entity entity = ldth.ds.get(KeyFactory.createKey(Person.class.getSimpleName(), p.getId()));
    assertNotNull(entity);
    assertEquals("jimmy", entity.getProperty("first"));
    assertEquals("jam", entity.getProperty("last"));
    assertEquals("anotherjimmy", entity.getProperty("anotherFirst"));
    assertEquals("anotherjam", entity.getProperty("anotherLast"));
  }

  public void testNullEmbeddable() throws EntityNotFoundException {
    Person p = new Person();
    p.setName(new Name());
    p.getName().setFirst("jimmy");
    p.getName().setLast("jam");
    makePersistentInTxn(p, TXN_START_END);

    assertNotNull(p.getId());

    Entity entity = ldth.ds.get(KeyFactory.createKey(Person.class.getSimpleName(), p.getId()));
    assertNotNull(entity);
    assertEquals("jimmy", entity.getProperty("first"));
    assertEquals("jam", entity.getProperty("last"));
    assertNull(entity.getProperty("anotherFirst"));
    assertNull(entity.getProperty("anotherLast"));
  }

  public void testFetchOfCachedPojo() throws Exception {
    PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(
        PersistenceManagerFactoryName.nontransactional.name());
    PersistenceManager pm = pmf.getPersistenceManager();
    // TODO(maxr,bslatkin): Remove this next line once this test
    // is actually working properly.
    pm.currentTransaction().setRetainValues(false);
    
    try {
      HasPromotedTypesJDO pojo = new HasPromotedTypesJDO();
      pojo.setIntVal(33);
      pojo.setIntegerVal(34);
      pojo.setIntArray(new int[] {0, 1, 2});
      pojo.setIntegerArray(new Integer[] {3, 4, 5});
      pojo.setIntList(Utils.newArrayList(3, 4, 5));
      pojo.setIntSet(Utils.newHashSet(3, 4, 5));
      pojo.setIntLinkedList(Utils.newLinkedList(31, 41, 51));
      pojo.setIntLinkedHashSet(Utils.newLinkedHashSet(31, 41, 51));
      pojo.setLongPrimVal(35L);
      pojo.setLongVal(36L);
      pojo.setLongPrimArray(new long[] {6L, 7L, 8L});
      pojo.setLongArray(new Long[] {9L, 10L, 11L});
      pojo.setLongList(Utils.newArrayList(12L, 13L, 14L));
      pojo.setLongSet(Utils.newHashSet(12L, 13L, 14L));
      pojo.setLongLinkedList(Utils.newLinkedList(121L, 131L, 141L));
      pojo.setLongLinkedHashSet(Utils.newLinkedHashSet(121L, 131L, 141L));
      pm.currentTransaction().begin();
      pm.makePersistent(pojo);
      pm.currentTransaction().commit();

      pojo = pm.getObjectById(HasPromotedTypesJDO.class, pojo.getId());
      assertEquals(33, pojo.getIntVal());
      assertEquals(Integer.valueOf(34), pojo.getIntegerVal());
      assertTrue(Arrays.equals(new int[] {0, 1, 2}, pojo.getIntArray()));
      assertTrue(Arrays.equals(new Integer[] {3, 4, 5}, pojo.getIntegerArray()));
      assertEquals(Utils.newArrayList(3, 4, 5), pojo.getIntList());
      assertEquals(Utils.newHashSet(3, 4, 5), pojo.getIntSet());
      assertEquals(Utils.newLinkedList(31, 41, 51), pojo.getIntLinkedList());
      assertEquals(Utils.newLinkedHashSet(31, 41, 51), pojo.getIntLinkedHashSet());
      assertEquals(35L, pojo.getLongPrimVal());
      assertEquals(Long.valueOf(36L), pojo.getLongVal());
      assertTrue(Arrays.equals(new long[] {6L, 7L, 8L}, pojo.getLongPrimArray()));
      assertTrue(Arrays.equals(new Long[] {9L, 10L, 11L}, pojo.getLongArray()));
      assertEquals(Utils.newArrayList(12L, 13L, 14L), pojo.getLongList());
      assertEquals(Utils.newHashSet(12L, 13L, 14L), pojo.getLongSet());
      assertEquals(Utils.newLinkedList(121L, 131L, 141L), pojo.getLongLinkedList());
      assertEquals(Utils.newLinkedHashSet(121L, 131L, 141L), pojo.getLongLinkedHashSet());
    } finally {
      pm.close();
      pmf.close();
    }
  }

  public void testInsertMultipleEntityGroups() {
    Flight f1 = new Flight();
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setMe(2);
    f1.setYou(4);
    f1.setName("Harold");

    Flight f2 = new Flight();
    f2.setOrigin("BOS");
    f2.setDest("MIA");
    f2.setMe(2);
    f2.setYou(4);
    f2.setName("Harold");

    beginTxn();
    pm.makePersistent(f1);
    try {
      pm.makePersistent(f2);
      fail("expected exception");
    } catch (JDOFatalUserException iae) {
      // good
    } finally {
      rollbackTxn();
    }
  }
}

