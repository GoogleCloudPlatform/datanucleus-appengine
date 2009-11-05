/*
 * Copyright (C) 2009 Max Ross.
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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.AbstractBaseClassesJPA;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JPAAbstractBaseClassTest extends JPATestCase {

  public void testConcrete() throws EntityNotFoundException {
    AbstractBaseClassesJPA.Concrete1 concrete = new AbstractBaseClassesJPA.Concrete1();
    concrete.setBase1Str("base 1");
    concrete.setConcrete1Str("concrete");

    beginTxn();
    em.persist(concrete);
    commitTxn();

    Entity concreteEntity = ldth.ds.get(KeyFactory.createKey(kindForObject(concrete), concrete.getId()));
    assertEquals(2, concreteEntity.getProperties().size());
    assertEquals("base 1", concreteEntity.getProperty("base1Str"));
    assertEquals("concrete", concreteEntity.getProperty("concrete1Str"));

    beginTxn();
    concrete = em.find(concrete.getClass(), concrete.getId());
    assertEquals("base 1", concrete.getBase1Str());
    assertEquals("concrete", concrete.getConcrete1Str());

    concrete.setBase1Str("not base 1");
    concrete.setConcrete1Str("not concrete");
    commitTxn();

    concreteEntity = ldth.ds.get(KeyFactory.createKey(kindForObject(concrete), concrete.getId()));
    assertEquals(2, concreteEntity.getProperties().size());
    assertEquals("not base 1", concreteEntity.getProperty("base1Str"));
    assertEquals("not concrete", concreteEntity.getProperty("concrete1Str"));

    beginTxn();
    concrete = em.find(concrete.getClass(), concrete.getId());
    assertEquals("not base 1", concrete.getBase1Str());
    assertEquals("not concrete", concrete.getConcrete1Str());

    assertNotNull(em.createQuery(
        "select from " + concrete.getClass().getName() + " where base1Str = 'not base 1'").getSingleResult());
    assertNotNull(em.createQuery(
        "select from " + concrete.getClass().getName() + " where concrete1Str = 'not concrete'").getSingleResult());

    em.remove(concrete);
    commitTxn();

    assertEquals(0, countForClass(concrete.getClass()));
  }

  public void testConcrete2() throws EntityNotFoundException {
    AbstractBaseClassesJPA.Concrete2 concrete = new AbstractBaseClassesJPA.Concrete2();
    concrete.setBase1Str("base 1");
    concrete.setBase2Str("base 2");
    concrete.setConcrete2Str("concrete");

    beginTxn();
    em.persist(concrete);
    commitTxn();

    Entity concreteEntity = ldth.ds.get(KeyFactory.createKey(kindForObject(concrete), concrete.getId()));
    assertEquals(3, concreteEntity.getProperties().size());
    assertEquals("base 1", concreteEntity.getProperty("base1Str"));
    assertEquals("base 2", concreteEntity.getProperty("base2Str"));
    assertEquals("concrete", concreteEntity.getProperty("concrete2Str"));

    beginTxn();
    concrete = em.find(concrete.getClass(), concrete.getId());
    assertEquals("base 1", concrete.getBase1Str());
    assertEquals("base 2", concrete.getBase2Str());
    assertEquals("concrete", concrete.getConcrete2Str());

    concrete.setBase1Str("not base 1");
    concrete.setBase2Str("not base 2");
    concrete.setConcrete2Str("not concrete");
    commitTxn();

    concreteEntity = ldth.ds.get(KeyFactory.createKey(kindForObject(concrete), concrete.getId()));
    assertEquals(3, concreteEntity.getProperties().size());
    assertEquals("not base 1", concreteEntity.getProperty("base1Str"));
    assertEquals("not base 2", concreteEntity.getProperty("base2Str"));
    assertEquals("not concrete", concreteEntity.getProperty("concrete2Str"));

    beginTxn();
    concrete = em.find(concrete.getClass(), concrete.getId());
    assertEquals("not base 1", concrete.getBase1Str());
    assertEquals("not base 2", concrete.getBase2Str());
    assertEquals("not concrete", concrete.getConcrete2Str());

    assertNotNull(em.createQuery(
        "select from " + concrete.getClass().getName() + " where base1Str = 'not base 1'").getSingleResult());
    assertNotNull(em.createQuery(
        "select from " + concrete.getClass().getName() + " where base2Str = 'not base 2'").getSingleResult());
    assertNotNull(em.createQuery(
        "select from " + concrete.getClass().getName() + " where concrete2Str = 'not concrete'").getSingleResult());

    em.remove(concrete);
    commitTxn();

    assertEquals(0, countForClass(concrete.getClass()));
  }
}