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
import com.google.appengine.api.datastore.KeyRange;

import org.datanucleus.test.SequenceExamplesJDO.HasSequence;
import org.datanucleus.test.SequenceExamplesJDO.HasSequenceOnNonPkFields;
import org.datanucleus.test.SequenceExamplesJDO.HasSequenceWithNoSequenceName;
import org.datanucleus.test.SequenceExamplesJDO.HasSequenceWithSequenceGenerator;
import org.datanucleus.test.SequenceExamplesJDO.HasSequenceWithUnencodedStringPk;

import java.util.List;

import javax.jdo.datastore.Sequence;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOSequenceTest extends JDOTestCase {

  private final List<String> sequenceNames = Utils.newArrayList();
  private final List<Long> sequenceBatchSizes = Utils.newArrayList();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    DatastoreServiceFactoryInternal.setDatastoreService(
        new BaseDatastoreServiceDelegate(DatastoreServiceFactoryInternal.getDatastoreService()) {
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
    sequenceBatchSizes.clear();
    super.tearDown();
  }


  public void testSimpleInsert() throws EntityNotFoundException {
    String kind = getKind(HasSequence.class);
    HasSequence pojo = new HasSequence();
    pojo.setVal("yar1");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals("yar1", e.getProperty("val"));

    HasSequence pojo2 = new HasSequence();
    pojo2.setVal("yar2");
    beginTxn();
    pm.makePersistent(pojo2);
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
    pm.makePersistent(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals("yar1", e.getProperty("val"));
    assertEquals(Utils.newArrayList("that"), sequenceNames);
    assertEquals(Utils.newArrayList(12L), sequenceBatchSizes);
  }

  public void testInsertWithSequenceGenerator_NoSequenceName() throws EntityNotFoundException {
    String kind = getKind(HasSequenceWithNoSequenceName.class);
    HasSequenceWithNoSequenceName pojo = new HasSequenceWithNoSequenceName();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__"), sequenceNames);
    assertEquals(Utils.newArrayList(12L), sequenceBatchSizes);
  }
  
  public void testDirectSequenceAccess() {
    KeyRange range = ldth.ds.allocateIds("that", 1);
    HasSequenceWithSequenceGenerator pojo = new HasSequenceWithSequenceGenerator();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    assertEquals(range.getEnd().getId(), pojo.getId() - 1);
    Sequence seq = pm.getSequence("yar1");
    assertEquals(pojo.getId() + 12, seq.nextValue());
    assertEquals(pojo.getId() + 13, seq.nextValue());
    assertEquals(Utils.newArrayList("that", "that"), sequenceNames);
    assertEquals(Utils.newArrayList(12L, 12L), sequenceBatchSizes);
    sequenceNames.clear();
    sequenceBatchSizes.clear();
    // getting a sequence always gets you a fresh batch
    seq = pm.getSequence("yar1");
    assertEquals(pojo.getId() + 24, seq.nextValue());
    assertEquals(pojo.getId() + 25, seq.nextValue());
    assertEquals(Utils.newArrayList("that"), sequenceNames);
    assertEquals(Utils.newArrayList(12L), sequenceBatchSizes);
  }

  public void testSequenceWithUnencodedStringPk() throws EntityNotFoundException {
    String kind = getKind(HasSequenceWithUnencodedStringPk.class);
    HasSequenceWithUnencodedStringPk pojo = new HasSequenceWithUnencodedStringPk();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));

    HasSequenceWithUnencodedStringPk pojo2 = new HasSequenceWithUnencodedStringPk();
    beginTxn();
    pm.makePersistent(pojo2);
    commitTxn();
    ldth.ds.get(KeyFactory.createKey(kind, pojo2.getId()));
    assertEquals(Long.parseLong(pojo.getId()), Long.parseLong(pojo2.getId()) - 1);
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__", kind + "_SEQUENCE__"), sequenceNames);
    assertEquals(Utils.newArrayList(1L, 1L), sequenceBatchSizes);
  }

  public void testSequenceOnNonPkFields() throws EntityNotFoundException {
    String kind = getKind(HasSequenceOnNonPkFields.class);
    HasSequenceOnNonPkFields pojo = new HasSequenceOnNonPkFields();
    pojo.setId("yar");
    beginTxn();
    pm.makePersistent(pojo);
    assertEquals(pojo.getVal1(), pojo.getVal2() - 1);
    commitTxn();

    HasSequenceOnNonPkFields pojo2 = new HasSequenceOnNonPkFields();
    pojo2.setId("yar");
    beginTxn();
    pm.makePersistent(pojo2);
    assertEquals(pojo.getVal2(), pojo2.getVal1() - 1);
    commitTxn();
    assertEquals(Utils.newArrayList(kind + "_SEQUENCE__", kind + "_SEQUENCE__",
                                    kind + "_SEQUENCE__", kind + "_SEQUENCE__"), sequenceNames);
    assertEquals(Utils.newArrayList(1L, 1L, 1L, 1L), sequenceBatchSizes);
  }

  private String getKind(Class<?> cls) {
    return cls.getName().substring(cls.getName().lastIndexOf(".") + 1);
  }

}