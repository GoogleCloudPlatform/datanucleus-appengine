/*
 * Copyright (C) 2010 Google.
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
 */
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.test.HasNonWritableFieldsJPA;


/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JPANonWritableFieldsTest extends JPATestCase {

  public void testInsert() throws EntityNotFoundException {
    HasNonWritableFieldsJPA pojo = new HasNonWritableFieldsJPA();
    pojo.setNotInsertable("insert");
    pojo.setNotUpdatable("update");
    pojo.setNotWritable("write");
    beginTxn();
    em.persist(pojo);
    commitTxn();
    beginTxn();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertNull(pojo.getNotInsertable());
    assertEquals("update", pojo.getNotUpdatable());
    assertNull(pojo.getNotWritable());
    commitTxn();
    Entity entity = ds.get(KeyFactory.createKey(kindForObject(pojo), pojo.getId()));
    assertEquals(1, entity.getProperties().size());
    assertEquals("update", entity.getProperty("notUpdatable"));
  }

  public void testUpdate() throws EntityNotFoundException {
    HasNonWritableFieldsJPA pojo = new HasNonWritableFieldsJPA();
    pojo.setNotInsertable("insert");
    pojo.setNotUpdatable("update");
    pojo.setNotWritable("write");
    beginTxn();
    em.persist(pojo);
    commitTxn();
    beginTxn();
    pojo = em.find(pojo.getClass(), pojo.getId());
    pojo.setNotInsertable("insert2");
    pojo.setNotUpdatable("update2");
    pojo.setNotWritable("write2");
    commitTxn();
    Entity entity = ds.get(KeyFactory.createKey(kindForObject(pojo), pojo.getId()));
    assertEquals(2, entity.getProperties().size());
    assertEquals("insert2", entity.getProperty("notInsertable"));
    assertEquals("update", entity.getProperty("notUpdatable"));
    beginTxn();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertEquals("insert2", pojo.getNotInsertable());
    assertEquals("update", pojo.getNotUpdatable());
    assertNull(pojo.getNotWritable());
    commitTxn();
  }
}
