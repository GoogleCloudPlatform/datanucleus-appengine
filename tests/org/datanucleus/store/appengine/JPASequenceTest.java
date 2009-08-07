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

import org.datanucleus.test.SequenceExamplesJPA.HasSequence;
import org.datanucleus.test.SequenceExamplesJPA.HasSequenceWithNoSequenceName;
import org.datanucleus.test.SequenceExamplesJPA.HasSequenceWithSequenceGenerator;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPASequenceTest extends JPATestCase {

  public void testSimpleInsert() throws EntityNotFoundException {
    String kind = getKind(HasSequence.class);
    KeyRange keyRange = ldth.ds.allocateIds(kind, 5);
    HasSequence pojo = new HasSequence();
    pojo.setVal("yar1");
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals("yar1", e.getProperty("val"));
    assertEquals(keyRange.getEnd().getId(), pojo.getId() - 1);

    HasSequence pojo2 = new HasSequence();
    pojo2.setVal("yar2");
    beginTxn();
    em.persist(pojo2);
    commitTxn();
    e = ldth.ds.get(KeyFactory.createKey(kind, pojo2.getId()));
    assertEquals("yar2", e.getProperty("val"));

    assertEquals(pojo.getId().longValue(), pojo2.getId() - 1);
  }

  public void testInsertWithSequenceGenerator() throws EntityNotFoundException {
    String kind = getKind(HasSequenceWithSequenceGenerator.class);
    KeyRange keyRange = ldth.ds.allocateIds("that", 5);
    HasSequenceWithSequenceGenerator pojo = new HasSequenceWithSequenceGenerator();
    pojo.setVal("yar1");
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals("yar1", e.getProperty("val"));
    assertEquals(keyRange.getEnd().getId(), pojo.getId() - 1);
    keyRange = ldth.ds.allocateIds("that", 1);
    assertEquals(pojo.getId() + 12, keyRange.getStart().getId());
  }

  public void testInsertWithSequenceGenerator_NoSequenceName() throws EntityNotFoundException {
    String kind = getKind(HasSequenceWithNoSequenceName.class);
    KeyRange keyRange = ldth.ds.allocateIds(kind, 5);
    HasSequenceWithNoSequenceName pojo = new HasSequenceWithNoSequenceName();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(kind, pojo.getId()));
    assertEquals(keyRange.getEnd().getId(), pojo.getId() - 1);
    keyRange = ldth.ds.allocateIds(kind, 1);
    assertEquals(pojo.getId() + 12, keyRange.getStart().getId());
  }

  private String getKind(Class<?> cls) {
    return cls.getName().substring(cls.getName().lastIndexOf(".") + 1);
  }

}