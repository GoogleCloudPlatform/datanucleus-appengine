// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.NullDataJPA;

import java.util.List;
import java.util.Set;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPANullValueTest extends JPATestCase {

  public void testFetchNullData() {
    Entity e = new Entity(NullDataJPA.class.getSimpleName());
    ldth.ds.put(e);
    beginTxn();
    NullDataJPA pojo = em.find(NullDataJPA.class, KeyFactory.keyToString(e.getKey()));
    assertNull(pojo.getArray());
    assertNull(pojo.getList());
    assertNull(pojo.getString());
    commitTxn();
  }

  public void testInsertNullData() throws EntityNotFoundException {
    NullDataJPA pojo = new NullDataJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(NullDataJPA.class.getSimpleName(), pojo.getId()));
    Set<String> props = e.getProperties().keySet();
    assertEquals(Utils.newHashSet("string", "array", "list", "set"), props);
  }

  public void testInsertContainersWithOneNullElement() throws EntityNotFoundException {
    NullDataJPA pojo = new NullDataJPA();
    List<String> list = Utils.newArrayList();
    list.add(null);
    pojo.setList(list);
    pojo.setArray(new String[] {null});
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(NullDataJPA.class.getSimpleName(), pojo.getId()));
    assertEquals(1, ((List<?>)e.getProperty("array")).size());
    assertEquals(1, ((List<?>)e.getProperty("list")).size());
  }
}