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
package com.google.appengine.datanucleus.jpa;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.NullDataJPA;

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
    assertTrue(pojo.getSet().isEmpty());
    assertTrue(pojo.getIntegerList().isEmpty());
    assertTrue(pojo.getIntegerSet().isEmpty());
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
    assertNotNull(pojo.getSet());
    assertNotNull(pojo.getIntegerList());
    assertNotNull(pojo.getIntegerSet());
    assertTrue(pojo.getList().isEmpty());
    assertTrue(pojo.getSet().isEmpty());
    assertTrue(pojo.getIntegerList().isEmpty());
    assertTrue(pojo.getIntegerSet().isEmpty());
  }

  public void testFetchMultiValuePropWithOneNullEntry() {
    Entity e = new Entity(NullDataJPA.class.getSimpleName());
    e.setProperty("list", Utils.newArrayList((String) null));
    e.setProperty("set", Utils.newArrayList((String) null));
    e.setProperty("integerList", Utils.newArrayList((Integer) null));
    e.setProperty("integerSet", Utils.newArrayList((Integer) null));
    ds.put(e);
    beginTxn();
    NullDataJPA pojo = em.find(NullDataJPA.class, e.getKey());
    assertNotNull(pojo.getList());
    assertEquals(1, pojo.getList().size());
    assertNull(pojo.getList().get(0));
    assertNotNull(pojo.getSet());
    assertEquals(1, pojo.getSet().size());
    assertNull(pojo.getSet().iterator().next());

    assertNotNull(pojo.getIntegerList());
    assertEquals(1, pojo.getIntegerList().size());
    assertNull(pojo.getIntegerList().get(0));
    assertNotNull(pojo.getIntegerSet());
    assertEquals(1, pojo.getIntegerSet().size());
    assertNull(pojo.getIntegerSet().iterator().next());
  }


  public void testInsertNullData() throws EntityNotFoundException {
    NullDataJPA pojo = new NullDataJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.createKey(NullDataJPA.class.getSimpleName(), pojo.getId()));
    Set<String> props = e.getProperties().keySet();
    assertEquals(Utils.newHashSet("string", "array", "list", "set", "integerList", "integerSet"), props);
    for (Object val : e.getProperties().values()) {
      assertNull(val);
    }
  }

  public void testInsertContainersWithOneNullElement() throws EntityNotFoundException {
    NullDataJPA pojo = new NullDataJPA();
    List<String> list = Utils.newArrayList();
    list.add(null);
    pojo.setList(list);
    Set<String> set = Utils.newHashSet();
    set.add(null);
    pojo.setSet(set);
    List<Integer> integerList = Utils.newArrayList();
    integerList.add(null);
    pojo.setIntegerList(integerList);
    Set<Integer> integerSet = Utils.newHashSet();
    integerSet.add(null);
    pojo.setIntegerSet(integerSet);
    pojo.setArray(new String[] {null});
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.createKey(NullDataJPA.class.getSimpleName(), pojo.getId()));
    assertEquals(1, ((List<?>)e.getProperty("array")).size());
    assertEquals(1, ((List<?>)e.getProperty("list")).size());
  }
}
