/**********************************************************************
Copyright (c) 2012 Google Inc.

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

import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.test.HasManyToManyAJPA;
import com.google.appengine.datanucleus.test.HasManyToManyBJPA;

/**
 * Tests for unowned M-N relations
 */
public class JPAManyToManyTest extends JPATestCase {

  public void testPersistBThenRelation() throws Exception {
    EntityManager em = emf.createEntityManager();
    try {
      // Persist Side B
      HasManyToManyBJPA b = new HasManyToManyBJPA();
      em.persist(b);

      // Create and persist Side A with another Side B, plus the existing Side B
      HasManyToManyAJPA a = new HasManyToManyAJPA();
      HasManyToManyBJPA b2 = new HasManyToManyBJPA();
      b2.getAs().add(a);
      a.getBs().add(b2);
      a.getBs().add(b);
      b.getAs().add(a);
      em.persist(b2);
      em.persist(a);
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in persist", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
    }
    if (emf.getCache() != null) {
      emf.getCache().evictAll();
    }

    em = emf.createEntityManager();
    try {
      Query qa = em.createQuery("SELECT a FROM HasManyToManyAJPA a");
      List<HasManyToManyAJPA> results1 = qa.getResultList();
      assertNotNull(results1);
      assertEquals("Incorrect number of side A", 1, results1.size());
      HasManyToManyAJPA a = results1.iterator().next();
      Set<HasManyToManyBJPA> bs = a.getBs();
      assertNotNull(bs);
      assertEquals("Incorrect number of side B for A", 2, bs.size());

      Query qb = em.createQuery("SELECT b FROM HasManyToManyBJPA b");
      List<HasManyToManyBJPA> results2 = qb.getResultList();
      assertNotNull(results2);
      assertEquals("Incorrect number of side B", 2, results2.size());
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in retrieval/checking", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
    }
  }

  public void testPersistAThenRelation() throws Exception {
    EntityManager em = emf.createEntityManager();
    try {
      // Persist Side A
      HasManyToManyAJPA a = new HasManyToManyAJPA();
      em.persist(a);

      // Create and persist Side B with Side A, plus another Side B
      HasManyToManyBJPA b = new HasManyToManyBJPA();
      b.getAs().add(a);
      a.getBs().add(b);
      em.persist(b);

      HasManyToManyBJPA b2 = new HasManyToManyBJPA();
      b2.getAs().add(a);
      a.getBs().add(b2);
      em.persist(b2);
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in persist", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
    }
    if (emf.getCache() != null) {
      emf.getCache().evictAll();
    }

    em = emf.createEntityManager();
    try {
      Query qa = em.createQuery("SELECT a FROM HasManyToManyAJPA a");
      List<HasManyToManyAJPA> results1 = qa.getResultList();
      assertNotNull(results1);
      assertEquals("Incorrect number of side A", 1, results1.size());
      HasManyToManyAJPA a = results1.iterator().next();
      Set<HasManyToManyBJPA> bs = a.getBs();
      assertNotNull(bs);
      assertEquals("Incorrect number of side B for A", 2, bs.size());

      Query qb = em.createQuery("SELECT b FROM HasManyToManyBJPA b");
      List<HasManyToManyBJPA> results2 = qb.getResultList();
      assertNotNull(results2);
      assertEquals("Incorrect number of side B", 2, results2.size());
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in retrieval/checking", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
    }
  }

  public void testPersistInOneStep() throws Exception {
    EntityManager em = emf.createEntityManager();
    try {
      // Persist Side A
      HasManyToManyBJPA b = new HasManyToManyBJPA();
      HasManyToManyAJPA a = new HasManyToManyAJPA();
      HasManyToManyBJPA b2 = new HasManyToManyBJPA();
      b2.getAs().add(a);
      a.getBs().add(b2);
      a.getBs().add(b);
      b.getAs().add(a);
      em.persist(a);
      em.persist(a);
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in persist", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
    }
    if (emf.getCache() != null) {
      emf.getCache().evictAll();
    }

    em = emf.createEntityManager();
    try {
      Query qa = em.createQuery("SELECT a FROM HasManyToManyAJPA a");
      List<HasManyToManyAJPA> results1 = qa.getResultList();
      assertNotNull(results1);
      assertEquals("Incorrect number of side A", 1, results1.size());
      HasManyToManyAJPA a = results1.iterator().next();
      Set<HasManyToManyBJPA> bs = a.getBs();
      assertNotNull(bs);
      assertEquals("Incorrect number of side B for A", 2, bs.size());

      Query qb = em.createQuery("SELECT b FROM HasManyToManyBJPA b");
      List<HasManyToManyBJPA> results2 = qb.getResultList();
      assertNotNull(results2);
      assertEquals("Incorrect number of side B", 2, results2.size());
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in retrieval/checking", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
    }
  }
}
