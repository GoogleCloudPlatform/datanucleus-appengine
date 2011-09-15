/**********************************************************************
Copyright (c) 2011 Google Inc.

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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.appengine.datanucleus.DatastoreServiceFactoryInternal;
import com.google.appengine.datanucleus.DatastoreServiceInterceptor;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.Utils.Function;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTop;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTopLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTopUnencodedStringPk;
import com.google.appengine.datanucleus.test.HasKeyPkJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyKeyPkJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyLongPkJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyUnencodedStringPkJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJPA;
import com.google.appengine.datanucleus.test.UnidirectionalSingeTableChildJPA;
import com.google.appengine.datanucleus.test.UnidirectionalSingeTableChildJPA.UnidirBottom;
import com.google.appengine.datanucleus.test.UnidirectionalSingeTableChildJPA.UnidirMiddle;
import com.google.appengine.datanucleus.test.UnidirectionalSingeTableChildJPA.UnidirTop;

import org.datanucleus.util.NucleusLogger;
import org.easymock.EasyMock;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import javax.jdo.JDOHelper;
import javax.persistence.PersistenceException;

import static com.google.appengine.datanucleus.PolymorphicTestUtils.getEntityKind;
import static com.google.appengine.datanucleus.TestUtils.assertKeyParentEquals;

abstract class JPAOneToManyPolymorphicTestCase extends JPATestCase {
  
  public enum UnidirLevel {
    Top(UnidirectionalSingeTableChildJPA.DISCRIMINATOR_TOP, UnidirTop.class),
    Middle(UnidirectionalSingeTableChildJPA.DISCRIMINATOR_MIDDLE, UnidirMiddle.class),
    Bottom(UnidirectionalSingeTableChildJPA.DISCRIMINATOR_BOTTOM, UnidirBottom.class);

    final String discriminator;
    final Class<?> clazz;
    
    UnidirLevel(String discriminator,  Class<?> clazz) {
      this.discriminator = discriminator;
      this.clazz = clazz;
    }
    
  }

  void testInsert_NewParentAndChild(BidirTop bidirChild, HasOneToManyJPA parent,
                                    StartEnd startEnd, UnidirLevel unidirLevel,
                                    int expectedParent, int expectedChildren)
      throws Exception {
    bidirChild.setChildVal("yam");

    UnidirTop unidir = newUnidir(unidirLevel);
    String expectedName = unidir.getName();
    String expectedStr = unidir.getStr();

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");

    parent.getBidirChildren().add(bidirChild);
    bidirChild.setParent(parent);
    parent.getUnidirChildren().add(unidir);
    parent.getHasKeyPks().add(hasKeyPk);
    parent.setVal("yar");

    startEnd.start();
    em.persist(parent);
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(unidir.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(bidirChild.getClass().getName(), bidirChildEntity.getProperty("DTYPE"));
    assertEquals(bidirChild.getPropertyCount(), bidirChildEntity.getProperties().size());
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirEntity);
    assertEquals(unidirLevel.discriminator, unidirEntity.getProperty("DTYPE"));
    assertEquals(expectedName, unidirEntity.getProperty("name"));
    assertEquals(expectedStr, unidirEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir.getId()), unidirEntity.getKey());
    assertKeyParentEquals(parent.getId(), unidirEntity, unidir.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ds.get(KeyFactory.stringToKey(parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Collections.singletonList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(unidirEntity.getKey()), parentEntity.getProperty("unidirChildren"));
    assertEquals(Collections.singletonList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(parent.getClass(), bidirChild.getClass(), expectedParent, expectedChildren);
  }

  void testInsert_ExistingParentNewChild(BidirTop bidirChild, HasOneToManyJPA pojo,
                                         StartEnd startEnd, UnidirLevel unidirLevel,
                                         int expectedParent, int expectedChildren) throws Exception {
    pojo.setVal("yar");

    NucleusLogger.GENERAL.info(">> txn.start");
    startEnd.start();
    NucleusLogger.GENERAL.info(">> em.persist of " + pojo);
    em.persist(pojo);
    NucleusLogger.GENERAL.info(">> txn.end");
    startEnd.end();
    assertNotNull(pojo.getId());
    assertTrue(pojo.getUnidirChildren().isEmpty());
    assertTrue(pojo.getHasKeyPks().isEmpty());
    assertTrue(pojo.getBidirChildren().isEmpty());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("unidirChildren"));
    assertNull(pojoEntity.getProperty("unidirChildren"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

    NucleusLogger.GENERAL.info(">> txn.start pojo.state=" + JDOHelper.getObjectState(pojo));
    startEnd.start();
    UnidirTop unidir = newUnidir(unidirLevel);
    String expectedName = unidir.getName();
    String expectedStr = unidir.getStr();
    NucleusLogger.GENERAL.info(">> pojo.unidirChildren.add(" + unidir + ")");
    pojo.getUnidirChildren().add(unidir);

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");
    NucleusLogger.GENERAL.info(">> pojo.haskeypks.add(" + hasKeyPk + ")");
    pojo.getHasKeyPks().add(hasKeyPk);

    bidirChild.setChildVal("yam");
    NucleusLogger.GENERAL.info(">> pojo.bidirChildren.add(" + bidirChild + ")");
    pojo.getBidirChildren().add(bidirChild);
    bidirChild.setParent(pojo);

    NucleusLogger.GENERAL.info(">> em.merge of " + pojo);
    em.merge(pojo);
    NucleusLogger.GENERAL.info(">> txn.end");
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(bidirChild.getParent());
    assertNotNull(unidir.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidirChild.getId());

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirEntity);
    assertEquals(expectedName, unidirEntity.getProperty("name"));
    assertEquals(expectedStr, unidirEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir.getId()), unidirEntity.getKey());
    assertKeyParentEquals(pojo.getId(), unidirEntity, unidir.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Collections.singletonList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(unidirEntity.getKey()), parentEntity.getProperty("unidirChildren"));
    assertEquals(Collections.singletonList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(pojo.getClass(), bidirChild.getClass(), expectedParent, expectedChildren);
  }

  void testUpdate_UpdateChildWithMerge(BidirTop bidir, HasOneToManyJPA pojo,
      StartEnd startEnd, UnidirLevel unidirLevel, int expectedParent, int expectedChildren) throws Exception {
    UnidirTop unidir = newUnidir(unidirLevel);
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getUnidirChildren().add(unidir);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertNotNull(unidir.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    startEnd.start();
    unidir.setName("yam");
    hasKeyPk.setStr("yar");
    bidir.setChildVal("yap");
    em.merge(pojo);
    startEnd.end();

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirEntity);
    assertEquals("yam", unidirEntity.getProperty("name"));
    assertKeyParentEquals(pojo.getId(), unidirEntity, unidir.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), expectedParent, expectedChildren);
  }

  void testUpdate_UpdateChild(BidirTop bidir, HasOneToManyJPA pojo,
                              StartEnd startEnd, UnidirLevel unidirLevel,
                              int expectedParent, int expectedChildren) throws Exception {
    UnidirTop unidir = newUnidir(unidirLevel);
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getUnidirChildren().add(unidir);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertNotNull(unidir.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    pojo.getUnidirChildren().iterator().next().setName("yam");
    pojo.getHasKeyPks().iterator().next().setStr("yar");
    pojo.getBidirChildren().iterator().next().setChildVal("yap");
    startEnd.end();

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirEntity);
    assertEquals("yam", unidirEntity.getProperty("name"));
    assertKeyParentEquals(pojo.getId(), unidirEntity, unidir.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), expectedParent, expectedChildren);
  }

  void testUpdate_NullOutChildren(BidirTop bidir, HasOneToManyJPA pojo,
                                  StartEnd startEnd, UnidirLevel unidirLevel,
                                  int expectedParent) throws Exception {
    UnidirTop unidir = newUnidir(unidirLevel);
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getUnidirChildren().add(unidir);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), expectedParent, 1);

    startEnd.start();
    pojo.nullUnidirChildren();
    pojo.nullHasKeyPks();
    pojo.nullBidirChildren();
    em.merge(pojo);
    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(unidir.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(hasKeyPk.getId());
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(KeyFactory.stringToKey(bidir.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(4, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("unidirChildren"));
    assertNull(pojoEntity.getProperty("unidirChildren"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), expectedParent, 0);
  }

  void testUpdate_ClearOutChildren(BidirTop bidir, HasOneToManyJPA pojo,
                                   StartEnd startEnd, UnidirLevel unidirLevel,
                                   int expectedParent) throws Exception {
    UnidirTop unidir = newUnidir(unidirLevel);
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getUnidirChildren().add(unidir);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), expectedParent, 1);

    startEnd.start();
    pojo.getUnidirChildren().clear();
    pojo.getHasKeyPks().clear();
    pojo.getBidirChildren().clear();
    em.merge(pojo);
    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(unidir.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(hasKeyPk.getId());
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(KeyFactory.stringToKey(bidir.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(4, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("unidirChildren"));
    assertNull(pojoEntity.getProperty("unidirChildren"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), expectedParent, 0);
  }

  void testFindWithOrderBy(Class<? extends HasPolymorphicRelationsListJPA.HasOneToManyWithOrderByJPA> pojoClass,
                           StartEnd startEnd) throws Exception {
    getExecutionContext().getNucleusContext().getPersistenceConfiguration().setProperty(
        "datanucleus.appengine.allowMultipleRelationsOfSameType", true);

    Entity pojoEntity = new Entity(getEntityKind(pojoClass));
    ds.put(pojoEntity);

    Entity unidirEntity1 = newUnidirEntity(UnidirLevel.Bottom, pojoEntity.getKey(), "name1", "str 1");
    ds.put(unidirEntity1);

    Entity unidirEntity2 = newUnidirEntity(UnidirLevel.Middle, pojoEntity.getKey(), "name2", "str 2");
    ds.put(unidirEntity2);

    Entity unidirEntity3 = newUnidirEntity(UnidirLevel.Top, pojoEntity.getKey(), "name1", "str 0");
    ds.put(unidirEntity3);

    startEnd.start();
    registerSubclasses();
    HasPolymorphicRelationsListJPA.HasOneToManyWithOrderByJPA pojo =
        em.find(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getUnidirByNameAndStr());
    assertEquals(3, pojo.getUnidirByNameAndStr().size());
    assertEquals("str 2", pojo.getUnidirByNameAndStr().get(0).getStr());
    assertTrue(pojo.getUnidirByNameAndStr().get(0) instanceof UnidirMiddle);
    assertEquals("str 0", pojo.getUnidirByNameAndStr().get(1).getStr());
    assertTrue(pojo.getUnidirByNameAndStr().get(1) instanceof UnidirTop);
    assertEquals("str 1", pojo.getUnidirByNameAndStr().get(2).getStr());
    assertTrue(pojo.getUnidirByNameAndStr().get(2) instanceof UnidirBottom);

    assertNotNull(pojo.getUnidirByIdAndName());
    assertEquals(3, pojo.getUnidirByIdAndName().size());
    assertEquals("str 0", pojo.getUnidirByIdAndName().get(0).getStr());
    assertTrue(pojo.getUnidirByIdAndName().get(0) instanceof UnidirTop);
    assertEquals("str 2", pojo.getUnidirByIdAndName().get(1).getStr());
    assertTrue(pojo.getUnidirByIdAndName().get(1) instanceof UnidirMiddle);
    assertEquals("str 1", pojo.getUnidirByIdAndName().get(2).getStr());
    assertTrue(pojo.getUnidirByIdAndName().get(2) instanceof UnidirBottom);

    assertNotNull(pojo.getUnidirByNameAndId());
    assertEquals(3, pojo.getUnidirByNameAndId().size());
    assertEquals("str 2", pojo.getUnidirByNameAndId().get(0).getStr());
    assertTrue(pojo.getUnidirByNameAndId().get(0) instanceof UnidirMiddle);
    assertEquals("str 1", pojo.getUnidirByNameAndId().get(1).getStr());
    assertTrue(pojo.getUnidirByNameAndId().get(1) instanceof UnidirBottom);
    assertEquals("str 0", pojo.getUnidirByNameAndId().get(2).getStr());
    assertTrue(pojo.getUnidirByNameAndId().get(2) instanceof UnidirTop);

    startEnd.end();
  }

  void testFind(Class<? extends HasOneToManyJPA> pojoClass,
                Class<? extends BidirTop> bidirClass,
                StartEnd startEnd, UnidirLevel unidirLevel, String bidirKind) throws Exception {
    Entity pojoEntity = new Entity(getEntityKind(pojoClass));
    ds.put(pojoEntity);

    Entity unidirEntity = newUnidirEntity(unidirLevel, pojoEntity.getKey(), "name1", "str 1");
    ds.put(unidirEntity);

    Entity hasKeyPkEntity = new Entity(getEntityKind(HasKeyPkJPA.class), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirKind, pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("DTYPE", bidirClass.getName());
    ds.put(bidirEntity);

    startEnd.start();
    registerSubclasses();
    HasOneToManyJPA pojo = em.find(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getUnidirChildren());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertEquals(unidirLevel.clazz, pojo.getUnidirChildren().iterator().next().getClass());
    assertEquals("name1", pojo.getUnidirChildren().iterator().next().getName());
    assertNotNull(pojo.getHasKeyPks());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals("yar", pojo.getHasKeyPks().iterator().next().getStr());
    assertNotNull(pojo.getBidirChildren());
    assertEquals(bidirClass, pojo.getBidirChildren().iterator().next().getClass());
    assertEquals("yap", pojo.getBidirChildren().iterator().next().getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().iterator().next().getParent());
    startEnd.end();
  }

  void testQuery(Class<? extends HasOneToManyJPA> pojoClass,
                 Class<? extends BidirTop> bidirClass,
                 StartEnd startEnd, UnidirLevel unidirLevel, String bidirKind) throws Exception {
    Entity pojoEntity = new Entity(getEntityKind(pojoClass));
    ds.put(pojoEntity);

    Entity unidirEntity = newUnidirEntity(unidirLevel, pojoEntity.getKey(), "name", "the str");
    ds.put(unidirEntity);

    Entity hasKeyPkEntity = new Entity(getEntityKind(HasKeyPkJPA.class), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirKind, pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("DTYPE", bidirClass.getName());
    ds.put(bidirEntity);

    startEnd.start();
    registerSubclasses();
    javax.persistence.Query q = em.createQuery(
        "select from " + pojoClass.getName() + " b where id = :key");
    q.setParameter("key", KeyFactory.keyToString(pojoEntity.getKey()));
    @SuppressWarnings("unchecked")
    List<HasOneToManyJPA> result = (List<HasOneToManyJPA>) q.getResultList();
    assertEquals(1, result.size());
    HasOneToManyJPA pojo = result.get(0);
    assertNotNull(pojo.getUnidirChildren());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertEquals(unidirLevel.clazz, pojo.getUnidirChildren().iterator().next().getClass());
    assertEquals("name", pojo.getUnidirChildren().iterator().next().getName());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertNotNull(pojo.getHasKeyPks());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals("yar", pojo.getHasKeyPks().iterator().next().getStr());
    assertNotNull(pojo.getBidirChildren());
    assertEquals(1, pojo.getBidirChildren().size());
    assertEquals(bidirClass, pojo.getBidirChildren().iterator().next().getClass());
    assertEquals("yap", pojo.getBidirChildren().iterator().next().getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().iterator().next().getParent());
    startEnd.end();
  }

  void testChildFetchedLazily(Class<? extends HasOneToManyJPA> pojoClass,
                              Class<? extends BidirTop> bidirClass,
                              UnidirLevel unidirLevel, String bidirKind) throws Exception {
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    // force a new emf to get created after we've installed our own DatastoreService mock
    if (emf.isOpen()) {
      emf.close();
    }
    tearDown();

    DatastoreService mockDatastore = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService(config);
    DatastoreServiceFactoryInternal.setDatastoreService(mockDatastore);
    try {
      setUp();

      Entity pojoEntity = new Entity(getEntityKind(pojoClass));
      ds.put(pojoEntity);

      Entity unidirEntity = newUnidirEntity(unidirLevel, pojoEntity.getKey(), "name", "the str");
      ds.put(unidirEntity);

      Entity hasKeyPkEntity = new Entity(getEntityKind(HasKeyPkJPA.class), pojoEntity.getKey());
      hasKeyPkEntity.setProperty("str", "yar");
      ds.put(hasKeyPkEntity);

      Entity bidirEntity = new Entity(bidirKind, pojoEntity.getKey());
      bidirEntity.setProperty("childVal", "yap");
      ds.put(bidirEntity);

      Transaction txn = EasyMock.createMock(Transaction.class);
      EasyMock.expect(txn.getId()).andReturn("1").times(2);
      txn.commit();
      EasyMock.expectLastCall();
      EasyMock.replay(txn);
      EasyMock.expect(mockDatastore.beginTransaction(EasyMock.isA(TransactionOptions.class))).andReturn(txn);
      // the only get we're going to perform is for the pojo
      EasyMock.expect(mockDatastore.get(txn, pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(mockDatastore);

      beginTxn();
      HasOneToManyJPA pojo = em.find(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      commitTxn();
    } finally {
      // need to close the pmf before we restore the original datastore service
      emf.close();
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(mockDatastore);
  }

  void testDeleteParentDeletesChild(Class<? extends HasOneToManyJPA> pojoClass,
                                    Class<? extends BidirTop> bidirClass,
                                    StartEnd startEnd, UnidirLevel unidirLevel, String bidirKind) throws Exception {
    Entity pojoEntity = new Entity(getEntityKind(pojoClass));
    ds.put(pojoEntity);

    Entity unidirEntity = newUnidirEntity(unidirLevel, pojoEntity.getKey(), "name", "the str");
    ds.put(unidirEntity);

    Entity hasKeyPkEntity = new Entity(getEntityKind(HasKeyPkJPA.class), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirKind, pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("DTYPE", bidirClass.getName());
    ds.put(bidirEntity);

    startEnd.start();
    registerSubclasses();
    HasOneToManyJPA pojo = em.find(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    em.remove(pojo);
    startEnd.end();
    assertCountsInDatastore(pojoClass, bidirClass, 0, 0);
  }

  private static int nextId = 0;

  static String nextNamedKey() {
    return "a" + nextId++;
  }

  public void testRemoveObject(HasOneToManyJPA pojo, BidirTop bidir1,
                               BidirTop bidir2,
                               StartEnd startEnd, UnidirLevel unidirLevel1,
                               UnidirLevel unidirLevel2, int count) throws Exception {
    pojo.setVal("yar");
    bidir1.setChildVal("yam1");
    bidir2.setChildVal("yam2");
    UnidirTop unidir1 = newUnidir(unidirLevel1);
    UnidirTop unidir2 = newUnidir(unidirLevel2);
    unidir2.setName("max");
    unidir2.setStr("another str");
    HasKeyPkJPA hasKeyPk1 = new HasKeyPkJPA(nextNamedKey());
    HasKeyPkJPA hasKeyPk2 = new HasKeyPkJPA(nextNamedKey());
    hasKeyPk2.setStr("yar 2");
    pojo.getUnidirChildren().add(unidir1);
    pojo.getUnidirChildren().add(unidir2);
    pojo.getHasKeyPks().add(hasKeyPk1);
    pojo.getHasKeyPks().add(hasKeyPk2);
    pojo.getBidirChildren().add(bidir1);
    pojo.getBidirChildren().add(bidir2);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), count,  count + 1);

    String bidir1Id = bidir1.getId();
    String unidir1Id = unidir1.getId();
    Key hasKeyPk1Key = hasKeyPk1.getId();
    pojo.getBidirChildren().remove(bidir1);
    pojo.getUnidirChildren().remove(unidir1);
    pojo.getHasKeyPks().remove(hasKeyPk1);

    startEnd.start();
    em.merge(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertNotNull(pojo.getId());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals(1, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(unidir2.getId());
    assertNotNull(hasKeyPk2.getId());
    assertEquals(bidir2.getId(), pojo.getBidirChildren().iterator().next().getId());
    assertEquals(hasKeyPk2.getId(), pojo.getHasKeyPks().iterator().next().getId());
    assertEquals(unidir2.getId(), pojo.getUnidirChildren().iterator().next().getId());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Collections.singletonList(KeyFactory.stringToKey(bidir2.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(KeyFactory.stringToKey(unidir2.getId())), pojoEntity.getProperty("unidirChildren"));
    assertEquals(Collections.singletonList(hasKeyPk2.getId()), pojoEntity.getProperty("hasKeyPks"));

    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(bidir1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(unidir1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(hasKeyPk1Key);
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(bidir2.getClass().getName(), bidirChildEntity.getProperty("DTYPE"));
    assertEquals(bidir2.getPropertyCount(), bidirChildEntity.getProperties().size());
    assertEquals("yam2", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir2.getId()));
    assertNotNull(unidirEntity);
    assertEquals("max", unidirEntity.getProperty("name"));
    assertEquals("another str", unidirEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir2.getId()), unidirEntity.getKey());
    assertKeyParentEquals(pojo.getId(), unidirEntity, unidir2.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk2.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar 2", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getId());

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Collections.singletonList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(unidirEntity.getKey()), parentEntity.getProperty("unidirChildren"));
    assertEquals(Collections.singletonList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), count, count);
  }

  void testChangeParent(HasOneToManyJPA pojo, HasOneToManyJPA pojo2,
                        StartEnd startEnd) throws Exception {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    UnidirTop b1 = new UnidirTop();
    pojo.getUnidirChildren().add(b1);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo2.getUnidirChildren().add(b1);
    try {
      em.persist(pojo2);
      startEnd.end();
      fail("expected exception");
    } catch (PersistenceException e) {
      rollbackTxn();
    }
  }

  void testNewParentNewChild_NamedKeyOnChild(HasOneToManyJPA pojo,
                                             StartEnd startEnd) throws Exception {
    UnidirTop b1 = new UnidirTop("named key");
    pojo.getUnidirChildren().add(b1);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(b1.getId()));
    assertEquals("named key", unidirEntity.getKey().getName());
  }

  void testAddAlreadyPersistedChildToParent_NoTxnDifferentEm(HasOneToManyJPA pojo) {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    UnidirTop unidir = new UnidirTop();
    em.persist(unidir);
    em.close();
    em = emf.createEntityManager();
    pojo.getUnidirChildren().add(unidir);
    try {
      em.persist(pojo);
      em.close();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }

    assertEquals(1, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(UnidirTop.class));
    em = emf.createEntityManager();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getUnidirChildren().size());
  }

  void testAddAlreadyPersistedChildToParent_NoTxnSameEm(HasOneToManyJPA pojo) {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    UnidirTop unidir = new UnidirTop();
    em.persist(unidir);
    em.close();
    em = emf.createEntityManager();
    pojo.getUnidirChildren().add(unidir);
    try {
      em.persist(pojo);
      em.close();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }

    assertEquals(1, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(UnidirTop.class));
    em = emf.createEntityManager();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getUnidirChildren().size());
  }

  void testFetchOfOneToManyParentWithKeyPk(HasOneToManyKeyPkJPA pojo,
                                           StartEnd startEnd) throws Exception {
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getUnidirChildren().size());
    startEnd.end();
  }

  void testFetchOfOneToManyParentWithLongPk(HasOneToManyLongPkJPA pojo,
                                            StartEnd startEnd) throws Exception {
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getUnidirChildren().size());
    startEnd.end();
  }

  void testFetchOfOneToManyParentWithUnencodedStringPk(HasOneToManyUnencodedStringPkJPA pojo,
                                                       StartEnd startEnd) throws Exception {
    pojo.setId("yar");
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getUnidirChildren().size());
    startEnd.end();
  }

  void testAddChildToOneToManyParentWithLongPk(
      HasOneToManyLongPkJPA pojo, BidirTopLongPk bidirChild,
      StartEnd startEnd, UnidirLevel unidirLevel, String discriminator) throws Exception {
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    UnidirTop unidir = newUnidir(unidirLevel);
    pojo.getUnidirChildren().add(unidir);
    pojo.getBidirChildren().add(bidirChild);
    bidirChild.setParent(pojo);
    startEnd.end();
    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertEquals(unidirLevel.discriminator, unidirEntity.getProperty("DTYPE"));
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertEquals(discriminator, bidirEntity.getProperty("DTYPE"));
    Entity pojoEntity = ds.get(KeyFactory.createKey(getEntityKind(pojo.getClass()), pojo.getId()));
    assertEquals(pojoEntity.getKey(), unidirEntity.getParent());
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddChildToOneToManyParentWithUnencodedStringPk(
      HasOneToManyUnencodedStringPkJPA pojo, BidirTopUnencodedStringPk bidirChild,
      StartEnd startEnd, UnidirLevel unidirLevel, String discriminator) throws Exception {
    pojo.setId("yar");
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    UnidirTop unidir = newUnidir(unidirLevel);
    pojo.getUnidirChildren().add(unidir);
    pojo.getBidirChildren().add(bidirChild);
    startEnd.end();
    
    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirEntity);
    
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(unidirEntity);
    assertEquals(bidirChild.getPropertyCount(), bidirEntity.getProperties().size());
    assertEquals(discriminator, bidirEntity.getProperty("TYPE"));
    
    Entity pojoEntity = ds.get(KeyFactory.createKey(getEntityKind(pojo.getClass()), pojo.getId()));
    assertEquals(pojoEntity.getKey(), unidirEntity.getParent());
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddQueriedParentToBidirChild(HasOneToManyJPA pojo, BidirTop bidir,
                                        StartEnd startEnd) throws Exception {
    startEnd.start();
    deleteAll(getEntityKind(pojo.getClass()));
    startEnd.end();
    
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = (HasOneToManyJPA) em.createQuery("select from " + pojo.getClass().getName() + " b").getSingleResult();
    bidir.setParent(pojo);
    em.persist(bidir);
    startEnd.end();
    startEnd.start();
    pojo = (HasOneToManyJPA) em.createQuery("select from " + pojo.getClass().getName() + " b").getSingleResult();
    assertEquals(1, pojo.getBidirChildren().size());
    startEnd.end();
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddFetchedParentToBidirChild(HasOneToManyJPA pojo, BidirTop bidir,
                                        StartEnd startEnd) throws Exception {
    startEnd.start();
    deleteAll(getEntityKind(pojo.getClass()));
    startEnd.end();

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();

    pojo = em.find(pojo.getClass(), pojo.getId());
    bidir.setParent(pojo);
    em.persist(bidir);
    startEnd.end();
    startEnd.start();
    pojo = (HasOneToManyJPA) em.createQuery("select from " + pojo.getClass().getName() + " b").getSingleResult();
    assertEquals(1, pojo.getBidirChildren().size());
    startEnd.end();
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  private static final class PutPolicy implements DatastoreServiceInterceptor.Policy {
    private final List<Object[]> putParamList = Utils.newArrayList();
    public void intercept(Object o, Method method, Object[] params) {
      if (method.getName().equals("put")) {
        putParamList.add(params);
      }
    }
  }

  PutPolicy setupPutPolicy(HasOneToManyJPA pojo, BidirTop bidir,
                           StartEnd startEnd) throws Throwable {
    PutPolicy policy = new PutPolicy();
    if (!em.isOpen()) {
      em = emf.createEntityManager();
    }
    DatastoreServiceInterceptor.install(getStoreManager(), policy);
    try {
      emf.close();
      switchDatasource(getEntityManagerFactoryName());
      UnidirTop unidir = new UnidirTop();
      pojo.getUnidirChildren().add(unidir);
      pojo.getBidirChildren().add(bidir);
      HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
      pojo.getHasKeyPks().add(hasKeyPk);

      startEnd.start();
      em.persist(pojo);
      startEnd.end();
      // 1 put for the parent, 3 puts for the children, 1 more put
      // to add the child keys back on the parent
      assertEquals(5, policy.putParamList.size());
      policy.putParamList.clear();
      return policy;
    } catch (Throwable t) {
      DatastoreServiceInterceptor.uninstall();
      throw t;
    }
  }

  void testOnlyOnePutOnChildUpdate(HasOneToManyJPA pojo, BidirTop bidir,
                                   StartEnd startEnd) throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = em.find(pojo.getClass(), pojo.getId());
      pojo.getUnidirChildren().iterator().next().setStr("some name");
      pojo.getBidirChildren().iterator().next().setChildVal("blarg");
      pojo.getHasKeyPks().iterator().next().setStr("double blarg");
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put for each child update
    assertEquals(3, policy.putParamList.size());
  }

  void testOnlyOneParentPutOnParentAndChildUpdate(HasOneToManyJPA pojo, BidirTop bidir,
                                                  StartEnd startEnd) throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = em.find(pojo.getClass(), pojo.getId());
      pojo.setVal("another val");
      pojo.getUnidirChildren().iterator().next().setStr("some name");
      pojo.getBidirChildren().iterator().next().setChildVal("blarg");
      pojo.getHasKeyPks().iterator().next().setStr("double blarg");
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put for the parent update, 1 put for each child update
    assertEquals(4, policy.putParamList.size());
  }

  void testOnlyOneParentPutOnChildDelete(HasOneToManyJPA pojo, BidirTop bidir,
                                         StartEnd startEnd,
                                         int expectedUpdatePuts) throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = em.find(pojo.getClass(), pojo.getId());
      pojo.setVal("another val");
      pojo.getUnidirChildren().clear();
      pojo.getBidirChildren().clear();
      pojo.getHasKeyPks().clear();
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    assertEquals(expectedUpdatePuts, policy.putParamList.size());
  }

  void assertCountsInDatastore(Class<? extends HasOneToManyJPA> pojoClass,
                               Class<? extends BidirTop> bidirClass,
                               int expectedParent, int expectedChildren) {
    assertEquals(pojoClass.getName(), expectedParent, countForClass(pojoClass));
    assertEquals(bidirClass.getName(), expectedChildren, countForClass(bidirClass));
    assertEquals(UnidirTop.class.getName(), expectedChildren, countForClass(UnidirTop.class));
    assertEquals(HasKeyPkJPA.class.getName(), expectedChildren, countForClass(HasKeyPkJPA.class));
  }

  UnidirTop newUnidir(UnidirLevel unidirLevel) {
    UnidirTop unidir = null;
    switch (unidirLevel) {
    case Top:
      unidir = new UnidirTop(nextNamedKey());
      unidir.setName("top name");
      unidir.setStr("top str");
      break;
    case Middle:
      unidir = new UnidirMiddle(nextNamedKey());
      unidir.setName("middle name");
      unidir.setStr("middle str");
      break;
    case Bottom:
      unidir = new UnidirBottom(nextNamedKey());
      unidir.setName("bottom name");
      unidir.setStr("bottom str");
      break;
    }
    return unidir;
  }

  protected Entity newUnidirEntity(UnidirLevel unidirLevel, Key parentKey, String name, String str) {
    Entity e;
    String kind = getEntityKind(UnidirTop.class);
    if (parentKey != null) {
      e = new Entity(kind, parentKey);
    } else {
      e = new Entity(kind);
    }
    e.setProperty("name", name);
    e.setProperty("str", str);
    switch (unidirLevel) {
      case Top:
        e.setProperty("DTYPE", unidirLevel.discriminator);
        break;
      case Middle:
        e.setProperty("DTYPE", unidirLevel.discriminator);
        break;
      case Bottom:
        e.setProperty("DTYPE", unidirLevel.discriminator);
        break;
    }
    return e;
  }
  
  private void deleteAll(String kind) {
    Query q = new Query(kind);
     q.setKeysOnly();
     ds.delete(Utils.transform(ds.prepare(q).asIterable(), new Function<Entity, Key>() {
       public Key apply(Entity from) {
 	return from.getKey();
       }}));
  }

  
  protected abstract void registerSubclasses();
}
