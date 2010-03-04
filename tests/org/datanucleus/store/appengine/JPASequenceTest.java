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

import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.KeyRange;

import org.datanucleus.test.SequenceExamplesJPA.HasSequence;
import org.datanucleus.test.SequenceExamplesJPA.HasSequenceOnNonPkFields;
import org.datanucleus.test.SequenceExamplesJPA.HasSequenceWithNoSequenceName;
import org.datanucleus.test.SequenceExamplesJPA.HasSequenceWithSequenceGenerator;
import org.datanucleus.test.SequenceExamplesJPA.HasSequenceWithUnencodedStringPk;

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
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfig();
    DatastoreServiceFactoryInternal.setDatastoreService(
        new BaseDatastoreServiceDelegate(DatastoreServiceFactoryInternal.getDatastoreService(config)) {
      @Override
      public KeyRange allocateIds(String kind, long size) {
        sequenceNames.add(kind);
        sequenceBatchSizes.add(size);
        return super.allocateIds(kind, size);
      }
    });
  }

  @Override
  protected void tearDown() throws Exception {
    DatastoreServiceFactoryInternal.setDatastoreService(null);
    sequenceNames.clear();
    super.tearDown();
  }

  public void testSimpleInsert() throws EntityNotFoundException {
    String kind = getKind(HasSequence.class);
    HasSequence pojo = new HasSequence();
    pojo.setVal("yar1");
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals("yar1", e.getProperty("val"));

    HasSequence pojo2 = new HasSequence();
    pojo2.setVal("yar2");
    beginTxn();
    em.persist(pojo2);
    commitTxn();
    e = ldth.ds.get(KeyFactory.createKey(kind, pojo2.getId()));
    assertEquals("yar2", e.getProperty("val"));
    assertEquals(pojo.getId().longValue(), pojo2.getId() - 1);
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__", kind + "_SEQUENCE__"), sequenceNames);
    assertEquals(Utils.newArrayList(1L, 1L), sequenceBatchSizes);
  }

  public void testInsertWithSequenceGenerator() throws EntityNotFoundException {
    String kind = getKind(HasSequenceWithSequenceGenerator.class);
    HasSequenceWithSequenceGenerator pojo = new HasSequenceWithSequenceGenerator();
    pojo.setVal("yar1");
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals("yar1", e.getProperty("val"));
    assertEquals(Utils.newArrayList("that"), sequenceNames);
    assertEquals(Utils.newArrayList(12L), sequenceBatchSizes);
  }

  public void testInsertWithSequenceGenerator_NoSequenceName() throws EntityNotFoundException {
    String kind = getKind(HasSequenceWithNoSequenceName.class);
    KeyRange keyRange = ldth.ds.allocateIds(kind, 5);
    HasSequenceWithNoSequenceName pojo = new HasSequenceWithNoSequenceName();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals(keyRange.getEnd().getId(), pojo.getId() - 1);
    keyRange = ldth.ds.allocateIds(kind, 1);
    assertEquals(pojo.getId() + 12, keyRange.getStart().getId());
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__"), sequenceNames);
    assertEquals(Utils.newArrayList(12L), sequenceBatchSizes);
  }

  public void testSequenceWithUnencodedStringPk() throws EntityNotFoundException {
    String kind = getKind(HasSequenceWithUnencodedStringPk.class);
    HasSequenceWithUnencodedStringPk pojo = new HasSequenceWithUnencodedStringPk();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));

    HasSequenceWithUnencodedStringPk pojo2 = new HasSequenceWithUnencodedStringPk();
    beginTxn();
    em.persist(pojo2);
    commitTxn();
    ldth.ds.get(KeyFactory.createKey(kind, pojo2.getId()));
    assertEquals(Long.parseLong(pojo.getId()), Long.parseLong(pojo2.getId()) - 1);
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__", kind + "_SEQUENCE__"), sequenceNames);
    assertEquals(Utils.newArrayList(1L, 1L), sequenceBatchSizes);
  }

  public void testSequenceOnNonPkFields() {
    String kind = getKind(HasSequenceOnNonPkFields.class);
    HasSequenceOnNonPkFields pojo = new HasSequenceOnNonPkFields();
    pojo.setId("yar");
    beginTxn();
    em.persist(pojo);
    commitTxn();
    assertEquals(pojo.getVal(), pojo.getVal2() - 1);

    HasSequenceOnNonPkFields pojo2 = new HasSequenceOnNonPkFields();
    pojo2.setId("yar");
    beginTxn();
    em.persist(pojo2);
    commitTxn();
    assertEquals(pojo.getVal2(), pojo2.getVal() - 1);
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__", kind + "_SEQUENCE__",
                                    kind + "_SEQUENCE__", kind + "_SEQUENCE__"), sequenceNames);
    assertEquals(Utils.newArrayList(1L, 1L, 1L, 1L), sequenceBatchSizes);
  }

  private String getKind(Class<?> cls) {
    return cls.getName().substring(cls.getName().lastIndexOf(".") + 1);
  }

}
