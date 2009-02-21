// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyAncestorKeyStringPkJDO;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasStringAncestorKeyPkJDO;
import org.datanucleus.test.HasVersionNoFieldJDO;
import org.datanucleus.test.HasVersionWithFieldJDO;
import org.datanucleus.test.KitchenSink;
import org.datanucleus.test.Name;
import org.datanucleus.test.Person;

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
    makePersistentInTxn(f1);
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
    makePersistentInTxn(f);
    Entity entity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(entity);
    assertEquals("foo", entity.getKey().getName());
  }

  public void testKitchenSinkInsert() throws EntityNotFoundException {
    KitchenSink ks = KitchenSink.newKitchenSink();
    assertNull(ks.key);
    makePersistentInTxn(ks);
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
    makePersistentInTxn(allNulls);
    Entity entityWithNulls = ldth.ds.get(KeyFactory.stringToKey(allNulls.key));
    assertNotNull(entityWithNulls);

    // now create a KitchenSink with non-null values
    KitchenSink noNulls = KitchenSink.newKitchenSink();
    makePersistentInTxn(noNulls);
    Entity entityWithoutNulls = ldth.ds.get(KeyFactory.stringToKey(noNulls.key));
    assertNotNull(entityWithoutNulls);

    assertEquals(entityWithNulls.getProperties().keySet(), entityWithoutNulls.getProperties().keySet());
  }

  public void testVersionInserts() throws EntityNotFoundException {
    HasVersionNoFieldJDO hv = new HasVersionNoFieldJDO();
    makePersistentInTxn(hv);

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

  public void testInsertWithKeyPkAndAncestor() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasKeyPkJDO hk1 = new HasKeyPkJDO();
    hk1.setAncestorKey(e.getKey());
    beginTxn();
    pm.makePersistent(hk1);
    Key key = hk1.getKey();
    Key ancestorKey = hk1.getAncestorKey();
    assertNotNull(key);
    commitTxn();
    Entity reloaded = ldth.ds.get(hk1.getKey());
    assertEquals(ancestorKey, reloaded.getKey().getParent());
  }

  public void testInsertWithKeyPkAndStringAncestor() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasStringAncestorKeyPkJDO hk1 = new HasStringAncestorKeyPkJDO();
    hk1.setAncestorKey(KeyFactory.keyToString(e.getKey()));
    beginTxn();
    pm.makePersistent(hk1);
    Key key = hk1.getKey();
    String ancestorKey = hk1.getAncestorKey();
    commitTxn();
    Entity reloaded = ldth.ds.get(key);
    assertEquals(ancestorKey, KeyFactory.keyToString(reloaded.getKey().getParent()));
  }

  public void testInsertWithStringPkAndKeyAncestor() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasKeyAncestorKeyStringPkJDO hk1 = new HasKeyAncestorKeyStringPkJDO();
    hk1.setAncestorKey(e.getKey());
    beginTxn();
    pm.makePersistent(hk1);
    String key = hk1.getKey();
    Key ancestorKey = hk1.getAncestorKey();
    commitTxn();
    Entity reloaded = ldth.ds.get(KeyFactory.stringToKey(key));
    assertEquals(ancestorKey, reloaded.getKey().getParent());
  }

  public void testInsertWithNamedKeyPk() {
    HasKeyPkJDO hk = new HasKeyPkJDO();
    hk.setKey(KeyFactory.createKey(HasKeyPkJDO.class.getSimpleName(), "name"));
    makePersistentInTxn(hk);

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
    makePersistentInTxn(p);

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
    makePersistentInTxn(p);

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
      Flight f1 = new Flight();
      f1.setOrigin("BOS");
      f1.setDest("MIA");
      f1.setMe(2);
      f1.setYou(4);
      f1.setName("Harold");
      f1.setFlightNumber(23);

      pm.currentTransaction().begin();
      pm.makePersistent(f1);
      pm.currentTransaction().commit();

      Flight f2 = new Flight();
      f2.setOrigin("BOS");
      f2.setDest("MIA");
      f2.setMe(2);
      f2.setYou(4);
      f2.setName("Harold");
      f2.setFlightNumber(24);

      pm.makePersistent(f2);
      pm.getObjectById(Flight.class, f1.getId());
    } finally {
      pm.close();
      pmf.close();
    }
  }
}

