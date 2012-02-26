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
package com.google.appengine.datanucleus.jdo;

import java.util.List;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.test.UnownedJDOManyToManySideA;
import com.google.appengine.datanucleus.test.UnownedJDOManyToManySideB;

/**
 * Tests for unowned M-N relations
 */
public class JDOUnownedManyToManyTest extends JDOTestCase {

  public void testPersist() throws Exception {
    PersistenceManager pm = pmf.getPersistenceManager();
    try {
      // Persist Side A
      UnownedJDOManyToManySideB b = new UnownedJDOManyToManySideB();
      pm.makePersistent(b);

      // Create and persist Side B with another Side A, plus the existing Side A
      UnownedJDOManyToManySideA a = new UnownedJDOManyToManySideA();
      UnownedJDOManyToManySideB b2 = new UnownedJDOManyToManySideB();
      b2.getAs().add(a);
      a.getBs().add(b2);
      a.getBs().add(b);
      b.getAs().add(a);
      pm.makePersistent(b2);
      pm.makePersistent(a);
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in persist", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (pm.currentTransaction().isActive()) {
        pm.currentTransaction().rollback();
      }
      pm.close();
    }
    pmf.getDataStoreCache().evictAll();

    pm = pmf.getPersistenceManager();
    try {
      Query qa = pm.newQuery(UnownedJDOManyToManySideA.class);
      List<UnownedJDOManyToManySideA> results1 = (List<UnownedJDOManyToManySideA>) qa.execute();
      assertNotNull(results1);
      assertEquals("Incorrect number of side A", 1, results1.size());
      UnownedJDOManyToManySideA a = results1.iterator().next();
      Set<UnownedJDOManyToManySideB> bs = a.getBs();
      assertNotNull(bs);
      assertEquals("Incorrect number of side B for A", 2, bs.size());

      Query qb = pm.newQuery(UnownedJDOManyToManySideB.class);
      List<UnownedJDOManyToManySideB> results2 = (List<UnownedJDOManyToManySideB>) qb.execute();
      assertNotNull(results2);
      assertEquals("Incorrect number of side B", 2, results2.size());
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in retrieval/checking", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (pm.currentTransaction().isActive()) {
        pm.currentTransaction().rollback();
      }
      pm.close();
    }
  }
}
