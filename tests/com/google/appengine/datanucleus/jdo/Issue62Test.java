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

import javax.jdo.JDOException;

import com.google.appengine.datanucleus.test.Issue62Child;
import com.google.appengine.datanucleus.test.Issue62Parent;

public class Issue62Test extends JDOTestCase {

  public void testMultipleOneToOne() {
    Issue62Parent parent;
    Issue62Child child;
    {
      child = new Issue62Child();
      parent = new Issue62Parent(child);
      pm.makePersistent(parent);
      pm.close();
    }

    // Create a second parent/child and next update the first child with the second parent
    Issue62Parent parent2;
    {
      pm = pmf.getPersistenceManager();
      parent2 = new Issue62Parent(new Issue62Child());
      pm.makePersistent(parent2);
      pm.close();

      try {
        pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();
        Issue62Child childToChangeParent = pm.getObjectById(Issue62Child.class, child.getId());
        assertNotNull(childToChangeParent);
        childToChangeParent.setParent(parent2);
        parent2.setChild(childToChangeParent);
        pm.currentTransaction().commit();
        fail("Didn't throw exception on attempt to change parent");
      } catch (JDOException jdoe) {
        // Expected
      }
      finally {
        if (pm.currentTransaction().isActive()) {
          pm.currentTransaction().rollback();
        }
        pm.close();
      }
    }

    // check the update
    {
      pm = pmf.getPersistenceManager();
      Issue62Child changedParent = pm.getObjectById(Issue62Child.class, child.getId());
      assertEquals(parent.getId(), changedParent.getParent().getId()); // <-- the update doen't take effect? why?
      pm.close();
    }
  }
}