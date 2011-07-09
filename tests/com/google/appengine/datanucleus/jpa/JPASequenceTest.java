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

import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.KeyRange;
import com.google.appengine.datanucleus.BaseDatastoreServiceDelegate;
import com.google.appengine.datanucleus.DatastoreServiceFactoryInternal;
import com.google.appengine.datanucleus.SequenceTestLock;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.SequenceExamplesJPA.HasSequence;
import com.google.appengine.datanucleus.test.SequenceExamplesJPA.HasSequenceOnNonPkFields;
import com.google.appengine.datanucleus.test.SequenceExamplesJPA.HasSequenceWithNoSequenceName;
import com.google.appengine.datanucleus.test.SequenceExamplesJPA.HasSequenceWithSequenceGenerator;
import com.google.appengine.datanucleus.test.SequenceExamplesJPA.HasSequenceWithUnencodedStringPk;
import com.google.appengine.datanucleus.valuegenerator.SequenceGenerator;

import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPASequenceTest extends JPATestCase {

  private final List<String> sequenceNames = Utils.newArrayList();
  private final List<Long> sequenceBatchSizes = Utils.newArrayList();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    DatastoreServiceFactoryInternal.setDatastoreService(
        new BaseDatastoreServiceDelegate(DatastoreServiceFactoryInternal.getDatastoreService(config)) {
      @Override
      public KeyRange allocateIds(String kind, long size) {
        sequenceNames.add(kind);
        sequenceBatchSizes.add(size);
        return super.allocateIds(kind, size);
      }
    });
    SequenceTestLock.LOCK.acquire();
    SequenceGenerator.setSequencePostfixAppendage("JPA");
  }

  @Override
  protected void tearDown() throws Exception {
    SequenceGenerator.clearSequencePostfixAppendage();
    SequenceTestLock.LOCK.release();
    DatastoreServiceFactoryInternal.setDatastoreService(null);
    sequenceNames.clear();
    super.tearDown();
  }

  public void testSimpleInsert() throws EntityNotFoundException {
    String kind = getKind(HasSequence.class);
    HasSequence pojo = new HasSequence();
    pojo.setVal("jpa1");
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals("jpa1", e.getProperty("val"));

    HasSequence pojo2 = new HasSequence();
    pojo2.setVal("jpa2");
    beginTxn();
    em.persist(pojo2);
    commitTxn();
    e = ds.get(KeyFactory.createKey(kind, pojo2.getId()));
    assertEquals("jpa2", e.getProperty("val"));
    // the local datastore id allocator is a single sequence so if there
    // are any other allocations happening we can't assert on exact values.
    // uncomment this check and the others below when we bring the local
    // allocator in line with the prod allocator
//    assertEquals(pojo.getId().longValue(), pojo2.getId() - 1);
    assertTrue(pojo.getId().longValue() < pojo2.getId());
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__JPA", kind + "_SEQUENCE__JPA"), sequenceNames);
    assertEquals(Utils.newArrayList(1L, 1L), sequenceBatchSizes);
  }

  public void testInsertWithSequenceGenerator() throws EntityNotFoundException {
    String kind = getKind(HasSequenceWithSequenceGenerator.class);
    HasSequenceWithSequenceGenerator pojo = new HasSequenceWithSequenceGenerator();
    pojo.setVal("jpa1");
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals("jpa1", e.getProperty("val"));
    assertEquals(Utils.newArrayList("jpathat"), sequenceNames);
    assertEquals(Utils.newArrayList(12L), sequenceBatchSizes);
  }

  public void testInsertWithSequenceGenerator_NoSequenceName() throws EntityNotFoundException {
    String kind = getKind(HasSequenceWithNoSequenceName.class);
    KeyRange keyRange = ds.allocateIds(kind, 5);
    HasSequenceWithNoSequenceName pojo = new HasSequenceWithNoSequenceName();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    ds.get(KeyFactory.createKey(kind, pojo.getId()));
    // the local datastore id allocator is a single sequence so if there
    // are any other allocations happening we can't assert on exact values.
    // uncomment this check and the others below when we bring the local
    // allocator in line with the prod allocator
//    assertEquals(keyRange.getEnd().getId(), pojo.getId() - 1);
    assertTrue(keyRange.getEnd().getId() < pojo.getId());
    keyRange = ds.allocateIds(kind, 1);
//    assertEquals(pojo.getId() + 12, keyRange.getStart().getId());
    assertTrue(pojo.getId() + 12 <= keyRange.getStart().getId());
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__JPA"), sequenceNames);
    assertEquals(Utils.newArrayList(12L), sequenceBatchSizes);
  }

  public void testSequenceWithUnencodedStringPk() throws EntityNotFoundException {
    String kind = getKind(HasSequenceWithUnencodedStringPk.class);
    HasSequenceWithUnencodedStringPk pojo = new HasSequenceWithUnencodedStringPk();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    ds.get(KeyFactory.createKey(kind, pojo.getId()));

    HasSequenceWithUnencodedStringPk pojo2 = new HasSequenceWithUnencodedStringPk();
    beginTxn();
    em.persist(pojo2);
    commitTxn();
    ds.get(KeyFactory.createKey(kind, pojo2.getId()));
    // the local datastore id allocator is a single sequence so if there
    // are any other allocations happening we can't assert on exact values.
    // uncomment this check and the others below when we bring the local
    // allocator in line with the prod allocator
//    assertEquals(Long.parseLong(pojo.getId()), Long.parseLong(pojo2.getId()) - 1);
    assertTrue(Long.parseLong(pojo.getId()) < Long.parseLong(pojo2.getId()));
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__JPA", kind + "_SEQUENCE__JPA"), sequenceNames);
    assertEquals(Utils.newArrayList(1L, 1L), sequenceBatchSizes);
  }

  public void testSequenceOnNonPkFields() {
    String kind = getKind(HasSequenceOnNonPkFields.class);
    HasSequenceOnNonPkFields pojo = new HasSequenceOnNonPkFields();
    pojo.setId("jpa");
    beginTxn();
    em.persist(pojo);
    commitTxn();
    // the local datastore id allocator is a single sequence so if there
    // are any other allocations happening we can't assert on exact values.
    // uncomment this check and the others below when we bring the local
    // allocator in line with the prod allocator
//    assertEquals(pojo.getVal(), pojo.getVal2() - 1);
    assertTrue(pojo.getVal() < pojo.getVal2());

    HasSequenceOnNonPkFields pojo2 = new HasSequenceOnNonPkFields();
    pojo2.setId("jpa");
    beginTxn();
    em.persist(pojo2);
    commitTxn();
//    assertEquals(pojo.getVal2(), pojo2.getVal() - 1);
    assertTrue(pojo.getVal2() < pojo2.getVal());
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__JPA", kind + "_SEQUENCE__JPA",
                                    kind + "_SEQUENCE__JPA", kind + "_SEQUENCE__JPA"), sequenceNames);
    assertEquals(Utils.newArrayList(1L, 1L, 1L, 1L), sequenceBatchSizes);
  }

  private String getKind(Class<?> cls) {
    return cls.getName().substring(cls.getName().lastIndexOf(".") + 1);
  }

}
