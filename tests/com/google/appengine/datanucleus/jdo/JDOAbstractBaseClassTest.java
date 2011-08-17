/*
 * Copyright (C) 2010 Google Inc
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
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.AbstractBaseClassesJDO.Concrete1;
import com.google.appengine.datanucleus.test.AbstractBaseClassesJDO.Concrete2;
import com.google.appengine.datanucleus.test.AbstractBaseClassesJDO.Concrete3;
import com.google.appengine.datanucleus.test.AbstractBaseClassesJDO.Concrete4;

import java.util.Collection;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JDOAbstractBaseClassTest extends JDOTestCase {

  public void testConcrete() throws Exception {
    Concrete1 concrete = new Concrete1();
    concrete.setBase1Str("base 1");
    concrete.setConcrete1Str("concrete");
    Concrete3 concrete3 = new Concrete3();
    concrete3.setStr("str3");
    concrete.setConcrete3(concrete3);
    Concrete4 concrete4a = new Concrete4();
    concrete4a.setStr("str4a");
    Concrete4 concrete4b = new Concrete4();
    concrete4b.setStr("str4b");
    concrete.setConcrete4(Utils.newArrayList(concrete4a, concrete4b));

    beginTxn();
    pm.makePersistent(concrete);
    commitTxn();

    Entity concreteEntity = ds.get(KeyFactory.createKey(kindForObject(concrete), concrete.getId()));
    Entity concrete3Entity = ds.get(concrete3.getId());
    Entity concrete4aEntity = ds.get(concrete4a.getId());
    Entity concrete4bEntity = ds.get(concrete4b.getId());

    assertEquals(4, concreteEntity.getProperties().size());
    assertEquals("base 1", concreteEntity.getProperty("base1Str"));
    assertEquals(concrete3Entity.getKey(), concreteEntity.getProperty("concrete3_id_OID"));
    assertEquals(Utils.newArrayList(concrete4aEntity.getKey(), concrete4bEntity.getKey()),
                 concreteEntity.getProperty("concrete4"));

    assertEquals(1, concrete3Entity.getProperties().size());
    assertEquals("str3", concrete3Entity.getProperty("str"));

    assertEquals(2, concrete4aEntity.getProperties().size());
    assertEquals("str4a", concrete4aEntity.getProperty("str"));
    assertEquals(0L, concrete4aEntity.getProperty("concrete4_INTEGER_IDX"));

    assertEquals(2, concrete4bEntity.getProperties().size());
    assertEquals("str4b", concrete4bEntity.getProperty("str"));
    assertEquals(1L, concrete4bEntity.getProperty("concrete4_INTEGER_IDX"));

    beginTxn();
    concrete = pm.getObjectById(concrete.getClass(), concrete.getId());
    assertEquals("base 1", concrete.getBase1Str());
    assertEquals("concrete", concrete.getConcrete1Str());
    assertEquals(concrete3.getId(), concrete.getConcrete3().getId());
    assertEquals(concrete3.getStr(), concrete.getConcrete3().getStr());
    assertEquals(2, concrete.getConcrete4().size());
    assertEquals(concrete4a.getId(), concrete.getConcrete4().get(0).getId());
    assertEquals(concrete4a.getStr(), concrete.getConcrete4().get(0).getStr());
    assertEquals(concrete4b.getId(), concrete.getConcrete4().get(1).getId());
    Key concrete4bKey = concrete4b.getId();
    assertEquals(concrete4b.getStr(), concrete.getConcrete4().get(1).getStr());

    concrete.setBase1Str("not base 1");
    concrete.setConcrete1Str("not concrete");

    concrete.getConcrete3().setStr("blam3");
    concrete.getConcrete4().get(0).setStr("blam4");
    concrete.getConcrete4().remove(1);
    commitTxn();

    concreteEntity = ds.get(KeyFactory.createKey(kindForObject(concrete), concrete.getId()));
    concrete3Entity = ds.get(concrete3.getId());
    concrete4aEntity = ds.get(concrete4a.getId());

    assertEquals(4, concreteEntity.getProperties().size());
    assertEquals("not base 1", concreteEntity.getProperty("base1Str"));
    assertEquals("not concrete", concreteEntity.getProperty("concrete1Str"));
    assertEquals(concrete3Entity.getKey(), concreteEntity.getProperty("concrete3_id_OID"));
    assertEquals(Utils.newArrayList(concrete4aEntity.getKey()), concreteEntity.getProperty("concrete4"));

    assertEquals(1, concrete3Entity.getProperties().size());
    assertEquals("blam3", concrete3Entity.getProperty("str"));

    assertEquals(2, concrete4aEntity.getProperties().size());
    assertEquals("blam4", concrete4aEntity.getProperty("str"));
    assertEquals(0L, concrete4aEntity.getProperty("concrete4_INTEGER_IDX"));
    try {
      ds.get(concrete4bKey);
    } catch (EntityNotFoundException enfe) {
      // good
    }

    beginTxn();
    concrete = pm.getObjectById(concrete.getClass(), concrete.getId());
    assertEquals("not base 1", concrete.getBase1Str());
    assertEquals("not concrete", concrete.getConcrete1Str());
    assertEquals(concrete3.getId(), concrete.getConcrete3().getId());
    assertEquals("blam3", concrete.getConcrete3().getStr());
    assertEquals(1, concrete.getConcrete4().size());
    assertEquals(concrete4a.getId(), concrete.getConcrete4().get(0).getId());
    assertEquals("blam4", concrete.getConcrete4().get(0).getStr());

    assertEquals(1, ((Collection<Concrete2>) pm.newQuery(
        "select from " + concrete.getClass().getName() + " where base1Str == 'not base 1'").execute()).size());
    assertEquals(1, ((Collection<Concrete2>) pm.newQuery(
        "select from " + concrete.getClass().getName() + " where concrete1Str == 'not concrete'").execute()).size());

    pm.deletePersistent(concrete);
    commitTxn();

    assertEquals(0, countForClass(concrete.getClass()));
    assertEquals(0, countForClass(concrete3.getClass()));
    assertEquals(0, countForClass(concrete4a.getClass()));
  }

  // This test fails under the runtime enhancer when run right after the test
  // above.  It does fine when run on its own and under the compile-time
  // enhancer.  Sadness.
  public void testConcrete2() throws Exception {
    Concrete2 concrete = new Concrete2();
    concrete.setBase1Str("base 1");
    concrete.setBase2Str("base 2");
    concrete.setConcrete2Str("concrete");
    Concrete3 concrete3 = new Concrete3();
    concrete3.setStr("str3");
    concrete.setConcrete3(concrete3);
    Concrete4 concrete4a = new Concrete4();
    concrete4a.setStr("str4a");
    Concrete4 concrete4b = new Concrete4();
    concrete4b.setStr("str4b");
    concrete.setConcrete4(Utils.newArrayList(concrete4a, concrete4b));

    beginTxn();
    pm.makePersistent(concrete);
    commitTxn();

    Entity concreteEntity = ds.get(KeyFactory.createKey(kindForObject(concrete), concrete.getId()));
    Entity concrete3Entity = ds.get(concrete3.getId());
    Entity concrete4aEntity = ds.get(concrete4a.getId());
    Entity concrete4bEntity = ds.get(concrete4b.getId());

    assertEquals(5, concreteEntity.getProperties().size());
    assertEquals("base 1", concreteEntity.getProperty("base1Str"));
    assertEquals("base 2", concreteEntity.getProperty("base2Str"));
    assertEquals("concrete", concreteEntity.getProperty("concrete2Str"));
    assertEquals(concrete3Entity.getKey(), concreteEntity.getProperty("concrete3_id_OID"));
    assertEquals(Utils.newArrayList(concrete4aEntity.getKey(), concrete4bEntity.getKey()),
                 concreteEntity.getProperty("concrete4"));

    assertEquals(1, concrete3Entity.getProperties().size());
    assertEquals("str3", concrete3Entity.getProperty("str"));

    assertEquals(2, concrete4aEntity.getProperties().size());
    assertEquals("str4a", concrete4aEntity.getProperty("str"));
    assertEquals(0L, concrete4aEntity.getProperty("concrete4_INTEGER_IDX"));

    assertEquals(2, concrete4bEntity.getProperties().size());
    assertEquals("str4b", concrete4bEntity.getProperty("str"));
    assertEquals(1L, concrete4bEntity.getProperty("concrete4_INTEGER_IDX"));

    beginTxn();
    concrete = pm.getObjectById(concrete.getClass(), concrete.getId());
    assertEquals("base 1", concrete.getBase1Str());
    assertEquals("base 2", concrete.getBase2Str());
    assertEquals("concrete", concrete.getConcrete2Str());

    assertEquals(concrete3.getId(), concrete.getConcrete3().getId());
    assertEquals(concrete3.getStr(), concrete.getConcrete3().getStr());
    assertEquals(2, concrete.getConcrete4().size());
    assertEquals(concrete4a.getId(), concrete.getConcrete4().get(0).getId());
    assertEquals(concrete4a.getStr(), concrete.getConcrete4().get(0).getStr());
    assertEquals(concrete4b.getId(), concrete.getConcrete4().get(1).getId());
    assertEquals(concrete4b.getStr(), concrete.getConcrete4().get(1).getStr());
    Key concrete4bKey = concrete4b.getId();

    concrete.setBase1Str("not base 1");
    concrete.setBase2Str("not base 2");
    concrete.setConcrete2Str("not concrete");

    concrete.getConcrete3().setStr("blam3");
    concrete.getConcrete4().get(0).setStr("blam4");
    concrete.getConcrete4().remove(1);
    commitTxn();

    concreteEntity = ds.get(KeyFactory.createKey(kindForObject(concrete), concrete.getId()));
    concrete3Entity = ds.get(concrete3.getId());
    concrete4aEntity = ds.get(concrete4a.getId());

    assertEquals(5, concreteEntity.getProperties().size());
    assertEquals("not base 1", concreteEntity.getProperty("base1Str"));
    assertEquals("not base 2", concreteEntity.getProperty("base2Str"));
    assertEquals("not concrete", concreteEntity.getProperty("concrete2Str"));
    assertEquals(concrete3Entity.getKey(), concreteEntity.getProperty("concrete3_id_OID"));
    assertEquals(Utils.newArrayList(concrete4aEntity.getKey()),
                 concreteEntity.getProperty("concrete4"));

    assertEquals(1, concrete3Entity.getProperties().size());
    assertEquals("blam3", concrete3Entity.getProperty("str"));

    assertEquals(2, concrete4aEntity.getProperties().size());
    assertEquals("blam4", concrete4aEntity.getProperty("str"));
    assertEquals(0L, concrete4aEntity.getProperty("concrete4_INTEGER_IDX"));
    try {
      ds.get(concrete4bKey);
    } catch (EntityNotFoundException enfe) {
      // good
    }

    beginTxn();
    concrete = pm.getObjectById(concrete.getClass(), concrete.getId());
    assertEquals("not base 1", concrete.getBase1Str());
    assertEquals("not base 2", concrete.getBase2Str());
    assertEquals("not concrete", concrete.getConcrete2Str());
    assertEquals(concrete3.getId(), concrete.getConcrete3().getId());
    assertEquals("blam3", concrete.getConcrete3().getStr());
    assertEquals(1, concrete.getConcrete4().size());
    assertEquals(concrete4a.getId(), concrete.getConcrete4().get(0).getId());
    assertEquals("blam4", concrete.getConcrete4().get(0).getStr());

    assertEquals(1, ((Collection<Concrete2>) pm.newQuery(
        "select from " + concrete.getClass().getName() + " where base1Str == 'not base 1'").execute()).size());
    assertEquals(1, ((Collection<Concrete2>) pm.newQuery(
        "select from " + concrete.getClass().getName() + " where base2Str == 'not base 2'").execute()).size());
    assertEquals(1, ((Collection<Concrete2>) pm.newQuery(
        "select from " + concrete.getClass().getName() + " where concrete2Str == 'not concrete'").execute()).size());

    pm.deletePersistent(concrete);
    commitTxn();

    assertEquals(0, countForClass(concrete.getClass()));
    assertEquals(0, countForClass(concrete3.getClass()));
    assertEquals(0, countForClass(concrete4a.getClass()));
  }
}
