/*
 * /**********************************************************************
 * Copyright (c) 2009 Google Inc.
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
 * **********************************************************************/
package com.google.appengine.datanucleus.jpa;

import java.util.Set;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.jpa.EmbeddableJPA;
import com.google.appengine.datanucleus.test.jpa.HasEmbeddableCollection;
import com.google.appengine.datanucleus.test.jpa.HasEmbeddedJPA;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAEmbeddedTest extends JPATestCase {

  public void testEmbedded() throws EntityNotFoundException {
    HasEmbeddedJPA pojo = new HasEmbeddedJPA();
    EmbeddableJPA embedded = new EmbeddableJPA();
    embedded.setEmbeddedString("yar");
    embedded.setMultiVal(Utils.newArrayList("m1", "m2"));
    pojo.setEmbeddable(embedded);
    embedded = new EmbeddableJPA();
    embedded.setEmbeddedString("yar2");
    embedded.setMultiVal(Utils.newArrayList("m3", "m4"));
    pojo.setEmbeddable2(embedded);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    assertEquals(1, countForClass(HasEmbeddedJPA.class));
    Entity e = ds.get(KeyFactory.createKey(pojo.getClass().getSimpleName(), pojo.getId()));
    assertEquals("yar", e.getProperty("embeddedString"));
    assertEquals(Utils.newArrayList("m1", "m2"), e.getProperty("multiVal"));
    assertEquals("yar2", e.getProperty("EMBEDDEDSTRING"));
    assertEquals(Utils.newArrayList("m3", "m4"), e.getProperty("MULTIVAL"));
    beginTxn();
    pojo = em.find(HasEmbeddedJPA.class, pojo.getId());
    assertNotNull(pojo.getEmbeddable());
    assertEquals("yar", pojo.getEmbeddable().getEmbeddedString());
    assertEquals(Utils.newArrayList("m1", "m2"), pojo.getEmbeddable().getMultiVal());
    assertEquals("yar2", pojo.getEmbeddable2().getEmbeddedString());
    assertEquals(Utils.newArrayList("m3", "m4"), pojo.getEmbeddable2().getMultiVal());
    commitTxn();
  }

  public void testEmbeddedWithKeyPk_NullEmbedded() {
    HasEmbeddedJPA pojo = new HasEmbeddedJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.clear();

    beginTxn();
    pojo = em.find(HasEmbeddedJPA.class, pojo.getId());
    assertNull(pojo.getEmbeddable());
    assertNull(pojo.getEmbeddable2());
    commitTxn();
  }

  public void testEmbeddedWithKeyPk_AddEmbeddedToExistingParent() {
    HasEmbeddedJPA pojo = new HasEmbeddedJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();

    EmbeddableJPA embeddable = new EmbeddableJPA();
    embeddable.setEmbeddedString("yar");
    EmbeddableJPA embeddable2 = new EmbeddableJPA();
    embeddable2.setEmbeddedString("yar2");
    beginTxn();
    pojo.setEmbeddable(embeddable);
    pojo.setEmbeddable2(embeddable2);
    pojo = em.find(HasEmbeddedJPA.class, pojo.getId()); // Finds it in L1 cache from first txn (but with fields set now)
    assertNotNull(pojo.getEmbeddable());
    assertNotNull(pojo.getEmbeddable().getEmbeddedString());
    assertNotNull(pojo.getEmbeddable2());
    assertNotNull(pojo.getEmbeddable2().getEmbeddedString());
    commitTxn(); // Flushes the changes to embeddable fields

    beginTxn();
    pojo = em.find(HasEmbeddedJPA.class, pojo.getId());
    assertNotNull(pojo.getEmbeddable());
    assertEquals("yar", pojo.getEmbeddable().getEmbeddedString());
    assertNotNull(pojo.getEmbeddable2());
    assertEquals("yar2", pojo.getEmbeddable2().getEmbeddedString());
    commitTxn();
  }

  public void testEmbeddedCollectionOfEmbeddables() {
    HasEmbeddableCollection pojo = new HasEmbeddableCollection();
    EmbeddableJPA emb1 = new EmbeddableJPA();
    emb1.setEmbeddedString("The String1");
    pojo.getTheSet().add(emb1);
    EmbeddableJPA emb2 = new EmbeddableJPA();
    emb2.setEmbeddedString("The String2");
    pojo.getTheSet().add(emb2);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    Long pk = pojo.getId();
    em.close();

    em = emf.createEntityManager();
    beginTxn();
    HasEmbeddableCollection thePojo = em.find(HasEmbeddableCollection.class, pk);
    assertNotNull(thePojo);
    Set<EmbeddableJPA> theEmbColl = thePojo.getTheSet();
    assertNotNull(thePojo);
    assertEquals("size of embedded collection is wrong", 2, theEmbColl.size());
    commitTxn();
    em.close();
  }
}