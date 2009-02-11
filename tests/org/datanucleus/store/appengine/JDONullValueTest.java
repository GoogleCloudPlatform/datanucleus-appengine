// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;

import org.datanucleus.test.NullDataJDO;
import org.datanucleus.test.NullDataWithDefaultValuesJDO;

import java.util.Set;
import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDONullValueTest extends JDOTestCase {

  public void testFetchNullData() {
    Entity e = new Entity(NullDataJDO.class.getSimpleName());
    ldth.ds.put(e);
    beginTxn();
    NullDataJDO pojo = pm.getObjectById(NullDataJDO.class, KeyFactory.keyToString(e.getKey()));
    assertNull(pojo.getArray());
    assertNull(pojo.getList());
    assertNull(pojo.getString());
    commitTxn();
  }

  public void testFetchNullDataWithDefaultValues() {
    Entity e = new Entity(NullDataWithDefaultValuesJDO.class.getSimpleName());
    ldth.ds.put(e);
    beginTxn();
    NullDataWithDefaultValuesJDO pojo =
        pm.getObjectById(NullDataWithDefaultValuesJDO.class, KeyFactory.keyToString(e.getKey()));
    assertNull(pojo.getArray());
    assertNull(pojo.getList());
    assertNull(pojo.getString());
    commitTxn();
  }

  public void testInsertNullData() throws EntityNotFoundException {
    NullDataJDO pojo = new NullDataJDO();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    Set<String> props = e.getProperties().keySet();
    assertEquals(Utils.newHashSet("string", "array", "list"), props);
  }

  public void testInsertContainersWithOneNullElement() throws EntityNotFoundException {
    NullDataJDO pojo = new NullDataJDO();
    List<String> list = Utils.newArrayList();
    list.add(null);
    pojo.setList(list);
    pojo.setArray(new String[] {null});
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(1, ((List<?>)e.getProperty("array")).size());
    assertEquals(1, ((List<?>)e.getProperty("list")).size());
  }
}
