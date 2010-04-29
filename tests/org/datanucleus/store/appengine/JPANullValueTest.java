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

import org.datanucleus.test.NullDataJPA;

import java.util.List;
import java.util.Set;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPANullValueTest extends JPATestCase {

  public void testFetchNullData() {
    Entity e = new Entity(NullDataJPA.class.getSimpleName());
    ds.put(e);
    beginTxn();
    NullDataJPA pojo = em.find(NullDataJPA.class, KeyFactory.keyToString(e.getKey()));
    assertNotNull(pojo.getArray());
    assertEquals(0, pojo.getArray().length);
    assertNotNull(pojo.getList());
    assertTrue(pojo.getList().isEmpty());
    assertNull(pojo.getString());
    commitTxn();
  }

  public void testFetchNullValue() {
    Entity e = new Entity(NullDataJPA.class.getSimpleName());
    e.setProperty("list", null);
    ds.put(e);
    beginTxn();
    NullDataJPA pojo = em.find(NullDataJPA.class, e.getKey());
    assertNotNull(pojo.getList());
    assertTrue(pojo.getList().isEmpty());
  }

  public void testFetchMultiValuePropWithOneNullEntry() {
    Entity e = new Entity(NullDataJPA.class.getSimpleName());
    e.setProperty("list", Utils.newArrayList((String) null));
    ds.put(e);
    beginTxn();
    NullDataJPA pojo = em.find(NullDataJPA.class, e.getKey());
    assertNotNull(pojo.getList());
    assertEquals(1, pojo.getList().size());
    assertNull(pojo.getList().get(0));
  }


  public void testInsertNullData() throws EntityNotFoundException {
    NullDataJPA pojo = new NullDataJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.createKey(NullDataJPA.class.getSimpleName(), pojo.getId()));
    Set<String> props = e.getProperties().keySet();
    assertEquals(Utils.newHashSet("string", "array", "list", "set"), props);
    assertEquals(Utils.newHashSet("string", "array", "list", "set"), props);
    for (Object val : e.getProperties().values()) {
      assertNull(val);
    }
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
    Entity e = ds.get(KeyFactory.createKey(NullDataJPA.class.getSimpleName(), pojo.getId()));
    assertEquals(1, ((List<?>)e.getProperty("array")).size());
    assertEquals(1, ((List<?>)e.getProperty("list")).size());
  }
}
