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

package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.EmbeddableJPA;
import org.datanucleus.test.HasEmbeddedJPA;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAEmbeddedTest extends JPATestCase {

  public void testEmbedded() throws EntityNotFoundException {
    HasEmbeddedJPA pojo = new HasEmbeddedJPA();
    EmbeddableJPA embedded = new EmbeddableJPA();
    embedded.setEmbeddedString("yar");
    pojo.setEmbeddable(embedded);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    assertEquals(1, countForClass(HasEmbeddedJPA.class));
    Entity e = ldth.ds.get(KeyFactory.createKey(pojo.getClass().getSimpleName(), pojo.getId()));
    assertEquals("yar", e.getProperty("embeddedString"));
    beginTxn();
    pojo = em.find(HasEmbeddedJPA.class, pojo.getId());
    assertNotNull(pojo.getEmbeddable());
    assertEquals("yar", pojo.getEmbeddable().getEmbeddedString());
    commitTxn();
  }

  public void testEmbeddedWithKeyPk_NullEmbedded() {
    HasEmbeddedJPA pojo = new HasEmbeddedJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();

    beginTxn();
    pojo = em.find(HasEmbeddedJPA.class, pojo.getId());
    assertNotNull(pojo.getEmbeddable());
    commitTxn();
  }

  public void testEmbeddedWithKeyPk_AddEmbeddedToExistingParent() {
    HasEmbeddedJPA pojo = new HasEmbeddedJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();

    EmbeddableJPA embeddable = new EmbeddableJPA();
    embeddable.setEmbeddedString("yar");
    beginTxn();
    pojo.setEmbeddable(embeddable);
    pojo = em.find(HasEmbeddedJPA.class, pojo.getId());
    assertNotNull(pojo.getEmbeddable());
    assertNull(pojo.getEmbeddable().getEmbeddedString());
    pojo.setEmbeddable(embeddable);
    commitTxn();

    beginTxn();
    pojo = em.find(HasEmbeddedJPA.class, pojo.getId());
    assertNotNull(pojo.getEmbeddable());
    assertEquals("yar", pojo.getEmbeddable().getEmbeddedString());
    commitTxn();
  }
}