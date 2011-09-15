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
package com.google.appengine.datanucleus.jdo;

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
import com.google.appengine.datanucleus.PolymorphicTestUtils;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.Utils.Function;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildJDO;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildJDO.BidirTop;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildJDO.BidirTopLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildJDO.BidirTopUnencodedStringPkJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyKeyPkJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyLongPkJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyUnencodedStringPkJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyWithOrderByJDO;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirBottom;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirMiddle;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirTop;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirTopWithIndexColumn;

import org.datanucleus.store.ExecutionContext;
import org.easymock.EasyMock;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.jdo.JDOFatalUserException;

import static com.google.appengine.datanucleus.PolymorphicTestUtils.getEntityKind;

abstract class JDOOneToManyPolymorphicTestCase extends JDOTestCase {
  
  public enum UnidirLevel {
    Top(UnidirectionalSuperclassTableChildJDO.DISCRIMINATOR_TOP, UnidirTop.class),
    Middle(UnidirectionalSuperclassTableChildJDO.DISCRIMINATOR_MIDDLE, UnidirMiddle.class),
    Bottom(UnidirectionalSuperclassTableChildJDO.DISCRIMINATOR_BOTTOM, UnidirBottom.class);

    final String discriminator;
    final Class<?> clazz;
    
    UnidirLevel(String discriminator,  Class<?> clazz) {
      this.discriminator = discriminator;
      this.clazz = clazz;
    }
    
  }
  
  public final StartEnd NEW_PM_DETACH_ON_CLOSE_START_END = new StartEnd() {
    public void start() {
      if (pm.isClosed()) {
        pm = pmf.getPersistenceManager();
        getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
      }
    }

    public void end() {
      pm.close();
    }

    public PersistenceManagerFactoryName getPmfName() {
      return PersistenceManagerFactoryName.nontransactional;
    }
  };


  @Override
  protected void tearDown() throws Exception {
    try {
      if (!pm.isClosed() && pm.currentTransaction().isActive()) {
        pm.currentTransaction().rollback();
      }
      pmf.close();
    } finally {
      super.tearDown();
    }
  }

  void testInsert_NewParentAndChild(HasOneToManyJDO parent,
      BidirTop bidirChild, StartEnd startEnd, UnidirLevel unidirLevel,
      String expectedBidirKind, String expectedUnidirKind,
      int expectedParent, int expectedChildren) throws EntityNotFoundException {
    bidirChild.setChildVal("yam");

    UnidirTop unidirChild = newUnidir(unidirLevel);
    String expectedStr = unidirChild.getStr();

    parent.addBidirChild(bidirChild);
    bidirChild.setParent(parent);
    parent.addUnidirChild(unidirChild);
    parent.setVal("yar");

    startEnd.start();
    pm.makePersistent(parent);
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(unidirChild.getId());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertEquals(expectedBidirKind, bidirChildEntity.getKind());
    assertNotNull(bidirChildEntity);
    assertEquals(bidirChild.getClass().getName(), bidirChildEntity.getProperty("DISCRIMINATOR"));
    assertEquals(bidirChild.getPropertyCount() + getIndexPropertyCount(), bidirChildEntity.getProperties().size());
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(parent.getId(), bidirChildEntity, bidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidirChild.getId()));
    assertEquals(expectedUnidirKind, unidirEntity.getKind());
    assertNotNull(unidirEntity);
    assertEquals(unidirLevel.discriminator, unidirEntity.getProperty("TYPE"));
    assertEquals(unidirChild.getPropertyCount() + getIndexPropertyCount(), unidirEntity.getProperties().size());
    assertEquals(expectedStr, unidirEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidirChild.getId()), unidirEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(parent.getId(), unidirEntity, unidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, unidirEntity.getProperty("unidirChildren_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.stringToKey(parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(unidirEntity.getKey()), parentEntity.getProperty("unidirChildren"));

    assertCountsInDatastore(parent.getClass(), bidirChild.getClass(), expectedParent, expectedChildren);
  }

  void testInsert_ExistingParentNewChild(HasOneToManyJDO pojo,
      BidirectionalSuperclassTableChildJDO.BidirTop bidirChild, StartEnd startEnd, UnidirLevel unidirLevel,
      String expectedBidirKind, String expectedUnidirKind,
      int expectedParent, int expectedChildren) throws EntityNotFoundException {
    pojo.setVal("yar");

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    
    startEnd.start();
    assertNotNull(pojo.getId());
    assertTrue(pojo.getUnidirChildren().isEmpty());
    assertTrue(pojo.getBidirChildren().isEmpty());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(3, pojoEntity.getProperties().size());
    assertEquals("yar", pojoEntity.getProperty("val"));
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("unidirChildren"));
    assertNull(pojoEntity.getProperty("unidirChildren"));
    startEnd.end();
    
    startEnd.start();
    pojo = pm.makePersistent(pojo);
    assertEquals("yar", pojo.getVal());
    UnidirTop unidirChild = newUnidir(unidirLevel);
    String exepectedStr = unidirChild.getStr();
    pojo.addUnidirChild(unidirChild);
    bidirChild.setChildVal("yam");
    pojo.addBidirChild(bidirChild);
    startEnd.end();

    startEnd.start();
    assertNotNull(bidirChild.getId());
    assertNotNull(bidirChild.getParent());
    assertNotNull(unidirChild.getId());
    startEnd.end();
    
    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(expectedBidirKind, bidirChildEntity.getKind());
    assertEquals(bidirChild.getPropertyCount() + getIndexPropertyCount(), bidirChildEntity.getProperties().size());
    assertEquals(bidirChild.getClass().getName(), bidirChildEntity.getProperty("DISCRIMINATOR"));
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity unidirChildEntity = ds.get(KeyFactory.stringToKey(unidirChild.getId()));
    assertNotNull(unidirChildEntity);
    assertEquals(expectedUnidirKind, unidirChildEntity.getKind());
    assertEquals(unidirChild.getPropertyCount() + getIndexPropertyCount(), unidirChildEntity.getProperties().size());
    assertEquals(unidirLevel.discriminator, unidirChildEntity.getProperty("TYPE"));
    assertEquals(exepectedStr, unidirChildEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidirChild.getId()), unidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), unidirChildEntity, unidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, unidirChildEntity.getProperty("unidirChildren_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(unidirChildEntity.getKey()), parentEntity.getProperty("unidirChildren"));

    assertCountsInDatastore(pojo.getClass(), bidirChild.getClass(), expectedParent, expectedChildren);
  }

  void testSwapAtPosition(HasOneToManyJDO pojo,
      BidirectionalSuperclassTableChildJDO.BidirTop bidir1,
      BidirectionalSuperclassTableChildJDO.BidirTop bidir2, StartEnd startEnd,
      String expectedBidirKind, String expectedUnidirKind,
      int expectedParent, int expectedChildren) throws EntityNotFoundException {
    pojo.setVal("yar");
    bidir2.setChildVal("yam");
    UnidirTop unidir = newUnidir(UnidirLevel.Middle);

    pojo.addUnidirChild(unidir);
    pojo.addBidirChild(bidir1);
    bidir1.setParent(pojo);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), expectedParent, expectedChildren);

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().iterator().next().getId();
    String unidir1Id = pojo.getUnidirChildren().iterator().next().getId();
    pojo.addBidirChildAtPosition(bidir2, 0);

    UnidirTop unidir2 = newUnidir(UnidirLevel.Top);
    unidir2.setStr("another str");
    pojo.addUnidirAtPosition(unidir2, 0);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertNotNull(pojo.getId());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertEquals(1, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(unidir2.getId());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(3, pojoEntity.getProperties().size());
    assertEquals(Utils.newArrayList(KeyFactory.stringToKey(bidir2.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(KeyFactory.stringToKey(unidir2.getId())), pojoEntity.getProperty("unidirChildren"));
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

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(expectedBidirKind, bidirChildEntity.getKind());
    assertEquals(bidir2.getPropertyCount() + getIndexPropertyCount(), bidirChildEntity.getProperties().size());
    assertEquals(bidir2.getClass().getName(), bidirChildEntity.getProperty("DISCRIMINATOR"));
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity unidirChildEntity = ds.get(KeyFactory.stringToKey(unidir2.getId()));
    assertNotNull(unidirChildEntity);
    assertEquals(expectedUnidirKind, unidirChildEntity.getKind());
    assertEquals(unidir2.getPropertyCount() + getIndexPropertyCount(), unidirChildEntity.getProperties().size());
    assertEquals(UnidirLevel.Top.discriminator, unidirChildEntity.getProperty("TYPE"));
    assertEquals("another str", unidirChildEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir2.getId()), unidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), unidirChildEntity, unidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, unidirChildEntity.getProperty("unidirChildren_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(unidirChildEntity.getKey()), parentEntity.getProperty("unidirChildren"));

    assertCountsInDatastore(pojo.getClass(), bidir2.getClass(), expectedParent, expectedChildren);
  }

  void testRemoveAtPosition(HasOneToManyJDO pojo,
      BidirectionalSuperclassTableChildJDO.BidirTop bidir1,
      BidirectionalSuperclassTableChildJDO.BidirTop bidir2,
      BidirectionalSuperclassTableChildJDO.BidirTop bidir3, StartEnd startEnd,
      String expectedBidirKind, String expectedUnidirKind,
      int count) throws EntityNotFoundException {
    pojo.setVal("yar");
    bidir2.setChildVal("another yam");
    bidir3.setChildVal("yet another yam");
    UnidirTop unidir = newUnidir(UnidirLevel.Top);
    UnidirTop unidir2 = newUnidir(UnidirLevel.Bottom);
    UnidirTop unidir3 = newUnidir(UnidirLevel.Middle);
    unidir2.setStr("another str");
    unidir3.setStr("yet another str");

    pojo.addUnidirChild(unidir);
    pojo.addUnidirChild(unidir2);
    pojo.addUnidirChild(unidir3);
    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addBidirChild(bidir3);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), count, (count-1) * 2 + 3);

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().iterator().next().getId();
    String unidir1Id = pojo.getUnidirChildren().iterator().next().getId();
    pojo.removeBidirChildAtPosition(0);
    pojo.removeUnidirChildAtPosition(0);
    startEnd.end();

    startEnd.start();
    assertNotNull(pojo.getId());
    assertEquals(2, pojo.getUnidirChildren().size());
    assertEquals(2, pojo.getBidirChildren().size());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(3, pojoEntity.getProperties().size());
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(bidir2.getId()),
        KeyFactory.stringToKey(bidir3.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(unidir2.getId()),
        KeyFactory.stringToKey(unidir3.getId())), pojoEntity.getProperty("unidirChildren"));
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

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(expectedBidirKind, bidirChildEntity.getKind());
    assertEquals(bidir2.getPropertyCount() + getIndexPropertyCount(), bidirChildEntity.getProperties().size());
    assertEquals(bidir2.getClass().getName(), bidirChildEntity.getProperty("DISCRIMINATOR"));
    assertEquals("another yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir3.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(expectedBidirKind, bidirChildEntity.getKind());
    assertEquals(bidir3.getPropertyCount() + getIndexPropertyCount(), bidirChildEntity.getProperties().size());
    assertEquals(bidir3.getClass().getName(), bidirChildEntity.getProperty("DISCRIMINATOR"));
    assertEquals("yet another yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir3.getId()), bidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(1L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity unidirChildEntity = ds.get(KeyFactory.stringToKey(unidir2.getId()));
    assertNotNull(unidirChildEntity);
    assertEquals(expectedUnidirKind, unidirChildEntity.getKind());
    assertEquals(unidir2.getPropertyCount() + getIndexPropertyCount(), unidirChildEntity.getProperties().size());
    assertEquals(UnidirLevel.Bottom.discriminator, unidirChildEntity.getProperty("TYPE"));
    assertEquals("another str", unidirChildEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir2.getId()), unidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), unidirChildEntity, unidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, unidirChildEntity.getProperty("unidirChildren_INTEGER_IDX"));
    }

    unidirChildEntity = ds.get(KeyFactory.stringToKey(unidir3.getId()));
    assertNotNull(unidirChildEntity);
    assertEquals(expectedUnidirKind, unidirChildEntity.getKind());
    assertEquals(UnidirLevel.Middle.discriminator, unidirChildEntity.getProperty("TYPE"));
    assertEquals("yet another str", unidirChildEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir3.getId()), unidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), unidirChildEntity, unidir2.getId());
    if (isIndexed()) {
      assertEquals(1L, unidirChildEntity.getProperty("unidirChildren_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(bidir2.getId()),
        KeyFactory.stringToKey(bidir3.getId())), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(unidir2.getId()),
        KeyFactory.stringToKey(unidir3.getId())), parentEntity.getProperty("unidirChildren"));

    assertCountsInDatastore(pojo.getClass(), bidir2.getClass(), count, 2 * count);
  }

  void testAddAtPosition(HasOneToManyJDO pojo,
      BidirectionalSuperclassTableChildJDO.BidirTop bidir1,
      BidirectionalSuperclassTableChildJDO.BidirTop bidir2, StartEnd startEnd,
      String expectedBidirKind, String expectedUnidirKind, int count) throws EntityNotFoundException {
    pojo.setVal("yar");
    bidir2.setChildVal("yam");
    UnidirTop unidir = newUnidir(UnidirLevel.Middle);
    UnidirTop unidir2 = newUnidir(UnidirLevel.Bottom);
    unidir2.setStr("another str");

    pojo.addUnidirChild(unidir);
    pojo.addBidirChild(bidir1);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), count, 2 *(count-1) + 1);

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().iterator().next().getId();
    String unidir1Id = pojo.getUnidirChildren().iterator().next().getId();
    pojo.addAtPosition(0, bidir2);
    pojo.addAtPosition(0, unidir2);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertNotNull(pojo.getId());
    assertEquals(2, pojo.getUnidirChildren().size());
    assertEquals(2, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(unidir2.getId());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(3, pojoEntity.getProperties().size());
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(bidir2.getId()),
        KeyFactory.stringToKey(bidir1.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(unidir2.getId()),
        KeyFactory.stringToKey(unidir.getId())), pojoEntity.getProperty("unidirChildren"));
    startEnd.end();

    ds.get(KeyFactory.stringToKey(bidir1Id));
    ds.get(KeyFactory.stringToKey(unidir1Id));
    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(expectedBidirKind, bidirChildEntity.getKind());
    assertEquals(bidir2.getPropertyCount() + getIndexPropertyCount(), bidirChildEntity.getProperties().size());
    assertEquals(bidir2.getClass().getName(), bidirChildEntity.getProperty("DISCRIMINATOR"));
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity unidirChildEntity = ds.get(KeyFactory.stringToKey(unidir2.getId()));
    assertNotNull(unidirChildEntity);
    assertEquals(expectedUnidirKind, unidirChildEntity.getKind());
    assertEquals(unidir2.getPropertyCount() + getIndexPropertyCount(), unidirChildEntity.getProperties().size());
    assertEquals(UnidirLevel.Bottom.discriminator, unidirChildEntity.getProperty("TYPE"));
    assertEquals("another str", unidirChildEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir2.getId()), unidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), unidirChildEntity, unidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, unidirChildEntity.getProperty("unidirChildren_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(bidir2.getId()),
        KeyFactory.stringToKey(bidir1.getId())), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(unidir2.getId()),
        KeyFactory.stringToKey(unidir.getId())), parentEntity.getProperty("unidirChildren"));

    assertCountsInDatastore(pojo.getClass(), bidir2.getClass(), count, 2 * count);
  }

  void testUpdate_UpdateChildWithMerge(HasOneToManyJDO pojo,
      BidirectionalSuperclassTableChildJDO.BidirTop bidir, StartEnd startEnd,
      String expectedBidirKind, String expectedUnidirKind, 
      UnidirLevel unidirLevel, int count) throws EntityNotFoundException {
    UnidirTop unidir = newUnidir(unidirLevel);

    pojo.addUnidirChild(unidir);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertNotNull(unidir.getId());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    startEnd.start();
    unidir.setStr("yam");
    bidir.setChildVal("yap");
    pm.makePersistent(pojo);
    startEnd.end();

    Entity unidirChildEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirChildEntity);
    assertEquals(expectedUnidirKind, unidirChildEntity.getKind());
    assertEquals(unidir.getPropertyCount() + getIndexPropertyCount(), unidirChildEntity.getProperties().size());
    assertEquals(unidirLevel.discriminator, unidirChildEntity.getProperty("TYPE"));
    assertEquals("yam", unidirChildEntity.getProperty("str"));
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), unidirChildEntity, unidir.getId());
    if (isIndexed()) {
      assertEquals(0L, unidirChildEntity.getProperty("unidirChildren_INTEGER_IDX"));
    }

    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals(expectedBidirKind, bidirEntity.getKind());
    assertEquals(bidir.getPropertyCount() + getIndexPropertyCount(), bidirEntity.getProperties().size());
    assertEquals(bidir.getClass().getName(), bidirEntity.getProperty("DISCRIMINATOR"));
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), count, count);
  }

  
  void testUpdate_UpdateChild(HasOneToManyJDO pojo,
      BidirectionalSuperclassTableChildJDO.BidirTop bidir, StartEnd startEnd,
      String expectedBidirKind, String expectedUnidirKind, 
      UnidirLevel unidirLevel, int count) throws EntityNotFoundException {
    UnidirTop unidir = newUnidir(unidirLevel);

    pojo.addUnidirChild(unidir);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertNotNull(unidir.getId());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    pojo.getUnidirChildren().iterator().next().setStr("yam");
    pojo.getBidirChildren().iterator().next().setChildVal("yap");
    startEnd.end();

    Entity unidirChildEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirChildEntity);
    assertEquals(expectedUnidirKind, unidirChildEntity.getKind());
    assertEquals(unidir.getPropertyCount() + getIndexPropertyCount(), unidirChildEntity.getProperties().size());
    assertEquals(unidirLevel.discriminator, unidirChildEntity.getProperty("TYPE"));
    assertEquals("yam", unidirChildEntity.getProperty("str"));
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), unidirChildEntity, unidir.getId());
    if (isIndexed()) {
      assertEquals(0L, unidirChildEntity.getProperty("unidirChildren_INTEGER_IDX"));
    }

    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals(expectedBidirKind, bidirEntity.getKind());
    assertEquals(bidir.getPropertyCount() + getIndexPropertyCount(), bidirEntity.getProperties().size());
    assertEquals(bidir.getClass().getName(), bidirEntity.getProperty("DISCRIMINATOR"));
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    PolymorphicTestUtils.assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), count, count);
  }

  
  void testUpdate_NullOutChildren(HasOneToManyJDO pojo,
    BidirectionalSuperclassTableChildJDO.BidirTop bidir, StartEnd startEnd,
    UnidirLevel unidirLevel, int count) throws EntityNotFoundException {
    UnidirTop unidir = newUnidir(unidirLevel);

    pojo.addUnidirChild(unidir);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), count, 1);

    startEnd.start();
    String unidirId = unidir.getId();
    String bidirChildId = bidir.getId();

    pojo.nullUnidirChildren();
    pojo.nullBidirChildren();
    pm.makePersistent(pojo);
    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(unidirId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(KeyFactory.stringToKey(bidirChildId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("unidirChildren"));
    assertNull(pojoEntity.getProperty("unidirChildren"));

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), count, 0);
  }

  
  void testUpdate_ClearOutChildren(HasOneToManyJDO pojo,
      BidirectionalSuperclassTableChildJDO.BidirTop bidir, StartEnd startEnd,
      UnidirLevel unidirLevel, int count) throws EntityNotFoundException {
    UnidirTop unidir = newUnidir(unidirLevel);

    pojo.addUnidirChild(unidir);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    String unidirId = unidir.getId();
    String bidirChildId = bidir.getId();
    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), count, 1);

    startEnd.start();
    pojo = pm.makePersistent(pojo);
    pojo.clearUnidirChildren();
    pojo.clearBidirChildren();
    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(unidirId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(KeyFactory.stringToKey(bidirChildId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("unidirChildren"));
    assertNull(pojoEntity.getProperty("unidirChildren"));

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), count, 0);
  }

  void testFindWithOrderBy(Class<? extends HasOneToManyWithOrderByJDO> pojoClass, StartEnd startEnd)
      throws EntityNotFoundException {
    getExecutionContext().getNucleusContext().getPersistenceConfiguration().setProperty(
        "datanucleus.appengine.allowMultipleRelationsOfSameType", true);
    Entity pojoEntity = new Entity(getEntityKind(pojoClass));
    ds.put(pojoEntity);

    Entity unidirChildEntity1 = newUnidir(UnidirLevel.Bottom, pojoEntity.getKey(), "str1", "name 1", new Date(), 1);
    unidirChildEntity1.setProperty("unidirByStrAndName_INTEGER_IDX", 0);
    unidirChildEntity1.setProperty("unidirByIdAndStr_INTEGER_IDX", 0);
    unidirChildEntity1.setProperty("unidirByStrAndId_INTEGER_IDX", 0);
    ds.put(unidirChildEntity1);

    Entity unidirChildEntity2 = newUnidir(UnidirLevel.Top, pojoEntity.getKey(), "str2", "name 2", null, null);
    unidirChildEntity2.setProperty("unidirByStrAndName_INTEGER_IDX", 1);
    unidirChildEntity2.setProperty("unidirByIdAndStr_INTEGER_IDX", 1);
    unidirChildEntity2.setProperty("unidirByStrAndId_INTEGER_IDX", 1);
    ds.put(unidirChildEntity2);

    Entity unidirChildEntity3 = newUnidir(UnidirLevel.Middle, pojoEntity.getKey(), "str1", "name 0", new Date(), null);
    unidirChildEntity3.setProperty("unidirByStrAndName_INTEGER_IDX", 2);
    unidirChildEntity3.setProperty("unidirByIdAndStr_INTEGER_IDX", 2);
    unidirChildEntity3.setProperty("unidirByStrAndId_INTEGER_IDX", 2);
    ds.put(unidirChildEntity3);

    Entity explicitIndexEntity1 =
        new Entity(getEntityKind(UnidirTopWithIndexColumn.class), pojoEntity.getKey());
    explicitIndexEntity1.setProperty("index", 3);
    ds.put(explicitIndexEntity1);

    Entity explicitIndexEntity2 =
        new Entity(getEntityKind(UnidirTopWithIndexColumn.class), pojoEntity.getKey());
    explicitIndexEntity2.setProperty("index", 2);
    ds.put(explicitIndexEntity2);

    Entity explicitIndexEntity3 =
        new Entity(getEntityKind(UnidirTopWithIndexColumn.class), pojoEntity.getKey());
    explicitIndexEntity3.setProperty("index", 1);
    ds.put(explicitIndexEntity3);

    startEnd.start();
    registerSubclasses();

    HasOneToManyWithOrderByJDO pojo = pm.getObjectById(
        pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getUndirByStrAndName());
    assertEquals(3, pojo.getUndirByStrAndName().size());
    assertEquals("name 2", pojo.getUndirByStrAndName().get(0).getName());
    assertTrue(pojo.getUndirByStrAndName().get(0) instanceof UnidirTop);
    assertEquals("name 0", pojo.getUndirByStrAndName().get(1).getName());
    assertTrue(pojo.getUndirByStrAndName().get(1) instanceof UnidirMiddle);
    assertEquals("name 1", pojo.getUndirByStrAndName().get(2).getName());
    assertTrue(pojo.getUndirByStrAndName().get(2) instanceof UnidirBottom);

    assertNotNull(pojo.getUnidirByIdAndStr());
    assertEquals(3, pojo.getUnidirByIdAndStr().size());
    assertEquals("name 0", pojo.getUnidirByIdAndStr().get(0).getName());
    assertTrue(pojo.getUnidirByIdAndStr().get(0) instanceof UnidirMiddle);
    assertEquals("name 2", pojo.getUnidirByIdAndStr().get(1).getName());
    assertTrue(pojo.getUnidirByIdAndStr().get(1) instanceof UnidirTop);
    assertEquals("name 1", pojo.getUnidirByIdAndStr().get(2).getName());
    assertTrue(pojo.getUnidirByIdAndStr().get(2) instanceof UnidirBottom);

    assertNotNull(pojo.getUnidirByStrAndId());
    assertEquals(3, pojo.getUnidirByStrAndId().size());
    assertEquals("name 2", pojo.getUnidirByStrAndId().get(0).getName());
    assertTrue(pojo.getUnidirByStrAndId().get(0) instanceof UnidirTop);
    assertEquals("name 1", pojo.getUnidirByStrAndId().get(1).getName());
    assertTrue(pojo.getUnidirByStrAndId().get(1) instanceof UnidirBottom);
    assertEquals("name 0", pojo.getUnidirByStrAndId().get(2).getName());
    assertTrue(pojo.getUnidirByStrAndId().get(2) instanceof UnidirMiddle);

    assertNotNull(pojo.getUnidirWithIndexColumn());
    assertEquals(3, pojo.getUnidirWithIndexColumn().size());
    assertEquals(explicitIndexEntity3.getKey(), KeyFactory.stringToKey(pojo.getUnidirWithIndexColumn().get(0).getId()));
    assertEquals(explicitIndexEntity2.getKey(), KeyFactory.stringToKey(pojo.getUnidirWithIndexColumn().get(1).getId()));
    assertEquals(explicitIndexEntity1.getKey(), KeyFactory.stringToKey(pojo.getUnidirWithIndexColumn().get(2).getId()));

    startEnd.end();
  }

  void testSaveWithOrderBy(HasOneToManyWithOrderByJDO pojo,
      StartEnd startEnd) throws EntityNotFoundException {
    getExecutionContext().getNucleusContext().getPersistenceConfiguration()
	.setProperty("datanucleus.appengine.allowMultipleRelationsOfSameType",
	    true);
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    
    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    UnidirTop unidir = newUnidir(UnidirLevel.Bottom);
    unidir.setStr("str1");
    unidir.setName("name 1");
    pojo.getUndirByStrAndName().add(unidir);
    unidir = newUnidir(UnidirLevel.Top);
    unidir.setStr("str2");
    unidir.setName("name 2");
    pojo.getUndirByStrAndName().add(unidir);
    unidir = newUnidir(UnidirLevel.Bottom);
    unidir.setStr("str1");
    unidir.setName("name 0");
    pojo.getUndirByStrAndName().add(unidir);
    startEnd.end();
    
    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    
    assertNotNull(pojo);
    assertNotNull(pojo.getUndirByStrAndName());
    assertEquals(3, pojo.getUndirByStrAndName().size());
    assertEquals("name 2", pojo.getUndirByStrAndName().get(0).getName());
    assertTrue(pojo.getUndirByStrAndName().get(0) instanceof UnidirTop);
    assertEquals("name 0", pojo.getUndirByStrAndName().get(1).getName());
    assertTrue(pojo.getUndirByStrAndName().get(1) instanceof UnidirMiddle);
    assertEquals("name 1", pojo.getUndirByStrAndName().get(2).getName());
    assertTrue(pojo.getUndirByStrAndName().get(2) instanceof UnidirBottom);

    assertNotNull(pojo.getUnidirByIdAndStr());
    assertEquals(3, pojo.getUnidirByIdAndStr().size());
    assertEquals("name 0", pojo.getUnidirByIdAndStr().get(0).getName());
    assertTrue(pojo.getUnidirByIdAndStr().get(0) instanceof UnidirMiddle);
    assertEquals("name 2", pojo.getUnidirByIdAndStr().get(1).getName());
    assertTrue(pojo.getUnidirByIdAndStr().get(1) instanceof UnidirTop);
    assertEquals("name 1", pojo.getUnidirByIdAndStr().get(2).getName());
    assertTrue(pojo.getUnidirByIdAndStr().get(2) instanceof UnidirBottom);

    assertNotNull(pojo.getUnidirByStrAndId());
    assertEquals(3, pojo.getUnidirByStrAndId().size());
    assertEquals("name 2", pojo.getUnidirByStrAndId().get(0).getName());
    assertTrue(pojo.getUnidirByStrAndId().get(0) instanceof UnidirTop);
    assertEquals("name 1", pojo.getUnidirByStrAndId().get(1).getName());
    assertTrue(pojo.getUnidirByStrAndId().get(1) instanceof UnidirBottom);
    assertEquals("name 0", pojo.getUnidirByStrAndId().get(2).getName());
    assertTrue(pojo.getUnidirByStrAndId().get(2) instanceof UnidirMiddle);

    startEnd.end();    
  }

 
  void testFind(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalSuperclassTableChildJDO.BidirTop> bidirClass, StartEnd startEnd,
      String bidirKind, UnidirLevel unidirLevel) throws EntityNotFoundException {
    Entity pojoEntity = new Entity(getEntityKind(pojoClass));
    ds.put(pojoEntity);

    Entity unidirChildEntity = newUnidir(unidirLevel, pojoEntity.getKey(), "str1", "name 1", null, null);
    ds.put(unidirChildEntity);

    Entity bidirEntity = new Entity(bidirKind, pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("DISCRIMINATOR", bidirClass.getName());
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ds.put(bidirEntity);

    startEnd.start();
    registerSubclasses();

    HasOneToManyJDO pojo =
        pm.getObjectById(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getUnidirChildren());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertEquals("str1", pojo.getUnidirChildren().iterator().next().getStr());
    assertEquals(unidirLevel.clazz, pojo.getUnidirChildren().iterator().next().getClass());
    assertNotNull(pojo.getBidirChildren());
    assertEquals(1, pojo.getBidirChildren().size());
    assertEquals("yap", pojo.getBidirChildren().iterator().next().getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().iterator().next().getParent());
    assertEquals(bidirClass, pojo.getBidirChildren().iterator().next().getClass());
    startEnd.end();
  }

  
  void testQuery(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalSuperclassTableChildJDO.BidirTop> bidirClass, StartEnd startEnd,
      String bidirKind, UnidirLevel unidirLevel) throws EntityNotFoundException {
    Entity pojoEntity = new Entity(getEntityKind(pojoClass));
    ds.put(pojoEntity);

    Entity unidirEntity = newUnidir(unidirLevel, pojoEntity.getKey(), "str", "name", null, null);
    ds.put(unidirEntity);

    Entity bidirEntity = new Entity(bidirKind, pojoEntity.getKey());
    bidirEntity.setProperty("DISCRIMINATOR", bidirClass.getName());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ds.put(bidirEntity);

    startEnd.start();
    registerSubclasses();
    javax.jdo.Query q = pm.newQuery(
        "select from " + pojoClass.getName() + " where id == key parameters String key");
    @SuppressWarnings("unchecked")
    List<HasOneToManyJDO> result =
        (List<HasOneToManyJDO>) q.execute(KeyFactory.keyToString(pojoEntity.getKey()));
    assertEquals(1, result.size());
    HasOneToManyJDO pojo = result.get(0);
    assertNotNull(pojo.getUnidirChildren());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertEquals("str", pojo.getUnidirChildren().iterator().next().getStr());
    assertEquals("name", pojo.getUnidirChildren().iterator().next().getName());
    assertEquals(unidirLevel.clazz, pojo.getUnidirChildren().iterator().next().getClass());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertNotNull(pojo.getBidirChildren());
    assertEquals(1, pojo.getBidirChildren().size());
    assertEquals("yap", pojo.getBidirChildren().iterator().next().getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().iterator().next().getParent());
    assertEquals(bidirClass, pojo.getBidirChildren().iterator().next().getClass());
    startEnd.end();
  }

  
  void testChildFetchedLazily(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalSuperclassTableChildJDO.BidirTop> bidirClass) throws Exception {
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    tearDown();
    DatastoreService mockDatastore = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService(config);
    DatastoreServiceFactoryInternal.setDatastoreService(mockDatastore);
    try {
      setUp();

      Entity pojoEntity = new Entity(getEntityKind(pojoClass));
      ds.put(pojoEntity);

      Entity unidirEntity = newUnidir(UnidirLevel.Top, pojoEntity.getKey(), "str", "name", null, null);
      ds.put(unidirEntity);

      Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
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
      HasOneToManyJDO pojo =
          pm.getObjectById(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      commitTxn();
    } finally {
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(mockDatastore);
  }

  
  void testDeleteParentDeletesChild(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalSuperclassTableChildJDO.BidirTop> bidirClass, StartEnd startEnd,
      String bidirKind, UnidirLevel unidirLevel) throws Exception {
    Entity pojoEntity = new Entity(getEntityKind(pojoClass));
    ds.put(pojoEntity);

    Entity unidirEntity = newUnidir(unidirLevel, pojoEntity.getKey(), "str", "name", null, null);
    ds.put(unidirEntity);

    Entity bidirEntity = new Entity(bidirKind, pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    bidirEntity.setProperty("DISCRIMINATOR", bidirClass.getName());
    ds.put(bidirEntity);

    startEnd.start();
    registerSubclasses();
    HasOneToManyJDO pojo = pm.getObjectById(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    pm.deletePersistent(pojo);
    startEnd.end();
    assertCountsInDatastore(pojoClass, bidirClass, 0, 0);
  }

  void testRemoveAll(HasOneToManyJDO pojo, BidirectionalSuperclassTableChildJDO.BidirTop bidir1,
                     BidirectionalSuperclassTableChildJDO.BidirTop bidir2, BidirectionalSuperclassTableChildJDO.BidirTop bidir3, StartEnd startEnd)
      throws EntityNotFoundException {
    UnidirTop unidir1 = new UnidirTop();
    UnidirTop unidir2 = new UnidirMiddle();
    UnidirTop unidir3 = new UnidirBottom();
    pojo.addUnidirChild(unidir1);
    pojo.addUnidirChild(unidir2);
    pojo.addUnidirChild(unidir3);

    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addBidirChild(bidir3);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.makePersistent(pojo);
    String unidir2Id = unidir2.getId();
    String bidir2Id = bidir2.getId();
    pojo.removeUnidirChildren(Collections.singleton(unidir2));
    pojo.removeBidirChildren(Collections.singleton(bidir2));
    startEnd.end();
    startEnd.start();

    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());

    assertEquals(2, pojo.getUnidirChildren().size());
    Set<String> unidirIds = Utils.newHashSet(unidir1.getId(), unidir2.getId(), unidir3.getId());
    for (UnidirTop unidir : pojo.getUnidirChildren()) {
      unidirIds.remove(unidir.getId());
    }
    assertEquals(1, unidirIds.size());
    assertEquals(unidir2.getId(), unidirIds.iterator().next());

    assertEquals(2, pojo.getBidirChildren().size());
    Set<String> bidirIds = Utils.newHashSet(bidir1.getId(), bidir2.getId(), bidir3.getId());
    for (BidirectionalSuperclassTableChildJDO.BidirTop b : pojo.getBidirChildren()) {
      bidirIds.remove(b.getId());
    }
    assertEquals(1, bidirIds.size());
    assertEquals(bidir2.getId(), bidirIds.iterator().next());
    startEnd.end();

    Entity unidirEntity1 = ds.get(KeyFactory.stringToKey(unidir1.getId()));
    Entity unidirEntity3 = ds.get(KeyFactory.stringToKey(unidir3.getId()));
    Entity bidirEntity1 = ds.get(KeyFactory.stringToKey(bidir1.getId()));
    Entity bidirEntity3 = ds.get(KeyFactory.stringToKey(bidir3.getId()));
    if (isIndexed()) {
      assertEquals(0L, unidirEntity1.getProperty("unidirChildren_INTEGER_IDX"));
      assertEquals(1L, unidirEntity3.getProperty("unidirChildren_INTEGER_IDX"));
      assertEquals(0L, bidirEntity1.getProperty("bidirChildren_INTEGER_IDX"));
      assertEquals(1L, bidirEntity3.getProperty("bidirChildren_INTEGER_IDX"));
    }
    try {
      ds.get(KeyFactory.stringToKey(unidir2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(bidir2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }


  void testRemoveAll_LongPkOnParent(HasOneToManyLongPkJDO pojo, BidirTopLongPk bidir1,
      BidirTopLongPk bidir2, BidirTopLongPk bidir3, StartEnd startEnd)
      throws EntityNotFoundException {
    UnidirTop unidir1 = new UnidirBottom();
    UnidirTop unidir2 = new UnidirMiddle();
    UnidirTop unidir3 = new UnidirTop();
    pojo.addUnidirChild(unidir1);
    pojo.addUnidirChild(unidir2);
    pojo.addUnidirChild(unidir3);

    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addBidirChild(bidir3);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.makePersistent(pojo);
    String f2Id = unidir2.getId();
    String bidir2Id = bidir2.getId();
    pojo.removeUnidirChildren(Collections.singleton(unidir2));
    pojo.removeBidirChildren(Collections.singleton(bidir2));
    startEnd.end();
    startEnd.start();

    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(2, pojo.getUnidirChildren().size());
    Set<String> unidirIds = Utils.newHashSet(unidir1.getId(), unidir2.getId(), unidir3.getId());
    for (UnidirTop f : pojo.getUnidirChildren()) {
      unidirIds.remove(f.getId());
    }
    assertEquals(1, unidirIds.size());
    assertEquals(unidir2.getId(), unidirIds.iterator().next());

    assertEquals(2, pojo.getBidirChildren().size());
    Set<String> bidirIds = Utils.newHashSet(bidir1.getId(), bidir2.getId(), bidir3.getId());
    for (BidirTopLongPk b : pojo.getBidirChildren()) {
      bidirIds.remove(b.getId());
    }
    assertEquals(1, bidirIds.size());
    assertEquals(bidir2.getId(), bidirIds.iterator().next());
    startEnd.end();

    Entity unidirEntity1 = ds.get(KeyFactory.stringToKey(unidir1.getId()));
    Entity unidirEntity3 = ds.get(KeyFactory.stringToKey(unidir3.getId()));
    Entity bidirEntity1 = ds.get(KeyFactory.stringToKey(bidir1.getId()));
    Entity bidirEntity3 = ds.get(KeyFactory.stringToKey(bidir3.getId()));
    if (isIndexed()) {
      assertEquals(0L, unidirEntity1.getProperty("unidirChildren_INTEGER_IDX_longpk"));
      assertEquals(1L, unidirEntity3.getProperty("unidirChildren_INTEGER_IDX_longpk"));
      assertEquals(0L, bidirEntity1.getProperty("bidirChildren_INTEGER_IDX"));
      assertEquals(1L, bidirEntity3.getProperty("bidirChildren_INTEGER_IDX"));
    }
    try {
      ds.get(KeyFactory.stringToKey(f2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(bidir2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  
  void testRemoveAll_UnencodedStringPkOnParent(HasOneToManyUnencodedStringPkJDO pojo, BidirTopUnencodedStringPkJDO bidir1,
      BidirTopUnencodedStringPkJDO bidir2, 
      BidirTopUnencodedStringPkJDO bidir3, StartEnd startEnd)
      throws EntityNotFoundException {
    UnidirTop unidir1 = new UnidirBottom();
    UnidirTop unidir2 = new UnidirTop();
    UnidirTop unidir3 = new UnidirMiddle();
    pojo.addUnidirChild(unidir1);
    pojo.addUnidirChild(unidir2);
    pojo.addUnidirChild(unidir3);

    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addBidirChild(bidir3);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.makePersistent(pojo);
    String f2Id = unidir2.getId();
    String bidir2Id = bidir2.getId();
    pojo.removeUnidirChildren(Collections.singleton(unidir2));
    pojo.removeBidirChildren(Collections.singleton(bidir2));
    startEnd.end();
    startEnd.start();

    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());

    assertEquals(2, pojo.getUnidirChildren().size());
    Set<String> unidirIds = Utils.newHashSet(unidir1.getId(), unidir2.getId(), unidir3.getId());
    for (UnidirTop f : pojo.getUnidirChildren()) {
      unidirIds.remove(f.getId());
    }
    assertEquals(1, unidirIds.size());
    assertEquals(unidir2.getId(), unidirIds.iterator().next());

    assertEquals(2, pojo.getBidirChildren().size());
    Set<String> bidirIds = Utils.newHashSet(bidir1.getId(), bidir2.getId(), bidir3.getId());
    for (BidirTopUnencodedStringPkJDO b : pojo.getBidirChildren()) {
      bidirIds.remove(b.getId());
    }
    assertEquals(1, bidirIds.size());
    assertEquals(bidir2.getId(), bidirIds.iterator().next());
    startEnd.end();

    Entity unidirEntity1 = ds.get(KeyFactory.stringToKey(unidir1.getId()));
    Entity unidirEntity3 = ds.get(KeyFactory.stringToKey(unidir3.getId()));
    Entity bidirEntity1 = ds.get(KeyFactory.stringToKey(bidir1.getId()));
    bidirEntity1.setProperty("DISCRIMINATOR", "B");
    Entity bidirEntity3 = ds.get(KeyFactory.stringToKey(bidir3.getId()));
    bidirEntity3.setProperty("DISCRIMINATOR", "M");
    if (isIndexed()) {
      assertEquals(0L, unidirEntity1.getProperty("unidirChildren_INTEGER_IDX_unencodedstringpk"));
      assertEquals(1L, unidirEntity3.getProperty("unidirChildren_INTEGER_IDX_unencodedstringpk"));
      assertEquals(0L, bidirEntity1.getProperty("bidirChildren_INTEGER_IDX"));
      assertEquals(1L, bidirEntity3.getProperty("bidirChildren_INTEGER_IDX"));
    }
    try {
      ds.get(KeyFactory.stringToKey(f2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(bidir2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  
  void testChangeParent(HasOneToManyJDO pojo, HasOneToManyJDO pojo2, 
      StartEnd startEnd, UnidirLevel unidirLevel) {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    UnidirTop unidir1 = newUnidir(unidirLevel);
    pojo.addUnidirChild(unidir1);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo2.addUnidirChild(unidir1);
    try {
      pm.makePersistent(pojo2);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      if (pm.currentTransaction().isActive()) {
        rollbackTxn();
      }
    }
  }

  void testNewParentNewChild_NamedKeyOnChild(HasOneToManyJDO pojo, StartEnd startEnd,
      UnidirLevel unidirLevel) throws EntityNotFoundException {
    UnidirTop unidir1 = newUnidir(unidirLevel);
    pojo.addUnidirChild(unidir1);
    unidir1.setId(KeyFactory.keyToString(
        KeyFactory.createKey(getEntityKind(UnidirTop.class), "named key")));
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir1.getId()));
    assertEquals("named key", unidirEntity.getKey().getName());
  }

  void testAddAlreadyPersistedChildToParent_NoTxnSamePm(HasOneToManyJDO pojo,
      UnidirLevel unidirLevel, int count) {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    UnidirTop unidir = newUnidir(unidirLevel);
    pm.makePersistent(unidir);
    pojo.addUnidirChild(unidir);
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
    pm.close();

    assertEquals(0, countForClass(pojo.getClass()));
    assertEquals(count, countForClass(UnidirTop.class));
  }

  void testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(HasOneToManyJDO pojo,
      UnidirLevel unidirLevel, int count) {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    UnidirTop unidir1 = newUnidir(unidirLevel);
    pm.makePersistent(unidir1);
    unidir1 = pm.detachCopy(unidir1);
    pm.close();
    pm = pmf.getPersistenceManager();
    unidir1 = pm.makePersistent(unidir1);
    pojo.addUnidirChild(unidir1);
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
    pm.close();

    assertEquals(0, countForClass(pojo.getClass()));
    assertEquals(count, countForClass(UnidirTop.class));
  }

  void testFetchOfOneToManyParentWithKeyPk(HasOneToManyKeyPkJDO pojo, StartEnd startEnd) {
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getUnidirChildren().size());
    startEnd.end();
  }

  void testFetchOfOneToManyParentWithLongPk(HasOneToManyLongPkJDO pojo, StartEnd startEnd) {
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getUnidirChildren().size());
    startEnd.end();
  }

  void testFetchOfOneToManyParentWithUnencodedStringPk(HasOneToManyUnencodedStringPkJDO pojo, StartEnd startEnd) {
    pojo.setId("yar");
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getUnidirChildren().size());
    startEnd.end();
  }

 
  void testAddChildToOneToManyParentWithLongPk(
      HasOneToManyLongPkJDO pojo, BidirTopLongPk bidirChild, StartEnd startEnd,
      UnidirLevel unidirLevel) throws EntityNotFoundException {
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    UnidirTop unidir = newUnidir(unidirLevel);
    pojo.addUnidirChild(unidir);
    pojo.addBidirChild(bidirChild);
    startEnd.end();
    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    Entity pojoEntity = ds.get(KeyFactory.createKey(getEntityKind(pojo.getClass()), pojo.getId()));
    assertEquals(pojoEntity.getKey(), unidirEntity.getParent());
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddChildToOneToManyParentWithUnencodedStringPk(
      HasOneToManyUnencodedStringPkJDO pojo, BidirTopUnencodedStringPkJDO bidirChild, 
      StartEnd startEnd, UnidirLevel unidirLevel, String id) throws EntityNotFoundException {
    pojo.setId(id);
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    UnidirTop unidir = newUnidir(unidirLevel);
    pojo.addUnidirChild(unidir);
    pojo.addBidirChild(bidirChild);
    startEnd.end();
    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    Entity pojoEntity = ds.get(KeyFactory.createKey(getEntityKind(pojo.getClass()), pojo.getId()));
    assertEquals(pojoEntity.getKey(), unidirEntity.getParent());
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddQueriedParentToBidirChild(HasOneToManyJDO pojo, BidirectionalSuperclassTableChildJDO.BidirTop bidir,
      StartEnd startEnd, String bidirKind) throws EntityNotFoundException {
    deleteAll(bidirKind);
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = (HasOneToManyJDO) ((List<?>)pm.newQuery(pojo.getClass()).execute()).get(0);
    bidir.setParent(pojo);
    pm.makePersistent(bidir);
    pojo.addBidirChild(bidir);
    startEnd.end();
    assertEquals(1, countForClass(bidir.getClass()));
    Entity e = ds.prepare(new Query(bidirKind)).asSingleEntity();
    assertNotNull(e.getParent());
    startEnd.start();
    pojo = (HasOneToManyJDO) ((List<?>)pm.newQuery(pojo.getClass()).execute()).get(0);
    assertEquals(1, pojo.getBidirChildren().size());
    startEnd.end();
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

   void testAddFetchedParentToBidirChild(HasOneToManyJDO pojo, BidirectionalSuperclassTableChildJDO.BidirTop bidir, 
       StartEnd startEnd, String bidirKind) throws EntityNotFoundException {
    deleteAll(bidirKind);
    deleteAll(getEntityKind(pojo.getClass()));
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    bidir.setParent(pojo);
    pm.makePersistent(bidir);
    pojo.addBidirChild(bidir);
    startEnd.end();
    assertEquals(1, countForClass(bidir.getClass()));
    Entity e = ds.prepare(new Query(bidirKind)).asSingleEntity();
    assertNotNull(e.getParent());
    startEnd.start();
    pojo = (HasOneToManyJDO) ((List<?>)pm.newQuery(pojo.getClass()).execute()).get(0);
    assertEquals(1, pojo.getBidirChildren().size());
    startEnd.end();
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testReplaceBidirColl(HasOneToManyJDO parent,
                            BidirectionalSuperclassTableChildJDO.BidirTop bidir1,
                            Collection<? extends BidirectionalSuperclassTableChildJDO.BidirTop> newColl, StartEnd startEnd) {
    bidir1.setParent(parent);
    parent.addBidirChild(bidir1);

    startEnd.start();
    pm.makePersistent(parent);
    startEnd.end();
    String childKey = bidir1.getId();
    startEnd.start();
    parent = pm.getObjectById(parent.getClass(), KeyFactory.stringToKey(parent.getId()));
    parent.setBidirChildren(newColl);
    startEnd.end();

    startEnd.start();
    parent = pm.getObjectById(parent.getClass(), KeyFactory.stringToKey(parent.getId()));
    assertEquals(2, parent.getBidirChildren().size());
    Iterator<? extends BidirectionalSuperclassTableChildJDO.BidirTop> childIter = parent.getBidirChildren().iterator();
    assertFalse(childKey.equals(childIter.next().getId()));
    assertFalse(childKey.equals(childIter.next().getId()));
    startEnd.end();
    assertEquals(2, countForClass(newColl.iterator().next().getClass()));
  }

  private static final class PutPolicy implements DatastoreServiceInterceptor.Policy {
    private final List<Object[]> putParamList = Utils.newArrayList();
    public void intercept(Object o, Method method, Object[] params) {
      if (method.getName().equals("put")) {
        putParamList.add(params);
      }
    }
  }

  PutPolicy setupPutPolicy(HasOneToManyJDO pojo, BidirectionalSuperclassTableChildJDO.BidirTop bidir, StartEnd startEnd)
      throws Throwable {
    PutPolicy policy = new PutPolicy();
    DatastoreServiceInterceptor.install(getStoreManager(), policy);
    try {
      pmf.close();
      switchDatasource(startEnd.getPmfName());
      UnidirTop unidir = new UnidirTop();
      pojo.addUnidirChild(unidir);
      pojo.addBidirChild(bidir);

      startEnd.start();
      pm.makePersistent(pojo);
      startEnd.end();
      // 1 put for the parent, 2 puts for the children, 1 more put
      // to add the child keys back on the parent
      assertEquals(4, policy.putParamList.size());
      policy.putParamList.clear();
      return policy;
    } catch (Throwable t) {
      DatastoreServiceInterceptor.uninstall();
      throw t;
    }
  }

  void testOnlyOnePutOnChildUpdate(HasOneToManyJDO pojo, BidirectionalSuperclassTableChildJDO.BidirTop bidir, StartEnd startEnd)
      throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
      pojo.getUnidirChildren().iterator().next().setStr("str");
      pojo.getBidirChildren().iterator().next().setChildVal("blarg");
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put for each child update
    assertEquals(2, policy.putParamList.size());
  }

  void testOnlyOneParentPutOnParentAndChildUpdate(HasOneToManyJDO pojo, BidirectionalSuperclassTableChildJDO.BidirTop bidir, StartEnd startEnd)
      throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
      pojo.setVal("another val");
      pojo.getUnidirChildren().iterator().next().setStr("str");
      pojo.getBidirChildren().iterator().next().setChildVal("blarg");
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put for the parent update, 1 put for each child update
    assertEquals(3, policy.putParamList.size());
  }

  void testOnlyOneParentPutOnChildDelete(HasOneToManyJDO pojo, BidirectionalSuperclassTableChildJDO.BidirTop bidir, StartEnd startEnd)
      throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
      pojo.setVal("another val");
      pojo.nullUnidirChildren();
      pojo.nullBidirChildren();
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put to remove the keys
    assertEquals(1, policy.putParamList.size());
  }

  void testNonTxnAddOfChildToParentFailsPartwayThrough(HasOneToManyJDO pojo)
      throws Throwable {
    UnidirTop unidir1 = new UnidirTop();
    pojo.addUnidirChild(unidir1);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    final String kind = kindForObject(pojo);
    DatastoreServiceInterceptor.Policy policy = new DatastoreServiceInterceptor.Policy() {
      public void intercept(Object o, Method method, Object[] params) {
        if (method.getName().equals("put") && ((Entity) params[0]).getKind().equals(kind)) {
          throw new ConcurrentModificationException("kaboom");
        }
      }
    };
    DatastoreServiceInterceptor.install(getStoreManager(), policy);
    UnidirTop unidir2 = new UnidirBottom();
    try {
      pmf.close();
      switchDatasource(PersistenceManagerFactoryName.nontransactional);
      pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
      pojo.addUnidirChild(unidir2);
      pm.close();
      fail("expected exception");
    } catch (ConcurrentModificationException cme) {
      // good
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // prove that the book entity exists
    ds.get(KeyFactory.stringToKey(unidir2.getId()));
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    // the parent has a reference to the first book that was already there
    // but no reference to the second book
    assertEquals(
        Collections.singletonList(KeyFactory.stringToKey(unidir1.getId())),
        pojoEntity.getProperty("unidirChildren"));
  }

  void assertCountsInDatastore(Class<? extends HasOneToManyJDO> parentClass,
      Class<? extends BidirectionalSuperclassTableChildJDO.BidirTop> bidirClass,
      int expectedParent, int expectedChildren) {
    assertEquals(parentClass.getName(), expectedParent, countForClass(parentClass));
    assertEquals(bidirClass.getName(), expectedChildren, countForClass(bidirClass));
    assertEquals(UnidirTop.class.getName(), expectedChildren, countForClass(UnidirTop.class));
  }
  
  UnidirTop newUnidir(UnidirLevel level) {
    switch (level) {
    case Top:
      UnidirTop top = new UnidirTop();
      top.setStr("top");
      top.setName("top name");
      return top;

    case Middle:
      UnidirMiddle middle = new UnidirMiddle();
      middle.setStr("middle");
      middle.setName("middle name");
      return middle;

    case Bottom:
      UnidirBottom bottom = new UnidirBottom();
      bottom.setStr("bottom");
      bottom.setName("bottom name");
      return bottom;

    default:
      return null;
    }
  }

  
  Entity newUnidir(UnidirLevel level, Key parentKey, String str, String name, Date date, Integer integer) {
    switch (level) {
      case Top:
        Entity topEntity = new Entity(getEntityKind(UnidirTop.class), parentKey);
        topEntity.setProperty("str", str);
        topEntity.setProperty("name", name);
        topEntity.setProperty("unidirChildren_INTEGER_IDX", 1);
        topEntity.setProperty("TYPE", UnidirectionalSuperclassTableChildJDO.DISCRIMINATOR_TOP);
        return topEntity;
      case Middle:
        Entity middleEntity = new Entity(getEntityKind(UnidirTop.class), parentKey);
        middleEntity.setProperty("str", str);
        middleEntity.setProperty("name", name);
        middleEntity.setProperty("date", date);
        middleEntity.setProperty("unidirChildren_INTEGER_IDX", 1);
        middleEntity.setProperty("TYPE", UnidirectionalSuperclassTableChildJDO.DISCRIMINATOR_MIDDLE);
        return middleEntity;
      case Bottom:
        Entity bottomEntity = new Entity(getEntityKind(UnidirTop.class), parentKey);
        bottomEntity.setProperty("str", str);
        bottomEntity.setProperty("name", name);
        bottomEntity.setProperty("date", date);
        bottomEntity.setProperty("integer", integer);
        bottomEntity.setProperty("unidirChildren_INTEGER_IDX", 1);
        bottomEntity.setProperty("TYPE", UnidirectionalSuperclassTableChildJDO.DISCRIMINATOR_BOTTOM);
        return bottomEntity;
      default:
	return null;
    }
  }

  protected int getIndexPropertyCount() {
    return isIndexed() ? 1 : 0;
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
  
  abstract boolean isIndexed();
  
}
