package com.google.appengine.datanucleus.jdo;

import javax.jdo.*;

import com.google.appengine.datanucleus.test.jdo.Issue290Parent;
import com.google.appengine.datanucleus.test.jdo.Issue290Child;

public class Issue290Test extends JDOTestCase {

  public void testDeleteDependent() {
    Object parentId = null;
    Object child1Id = null;
    {
      Issue290Parent parent = new Issue290Parent("First Parent");
      Issue290Child child1 = new Issue290Child("Child 1");
      parent.getChildren().add(child1);
      Issue290Child child2 = new Issue290Child("Child 2");
      parent.getChildren().add(child2);
      pm.makePersistent(parent);
      parentId = pm.getObjectId(parent);
      child1Id = pm.getObjectId(child1);
      pm.close();
      pmf.getDataStoreCache().evictAll();
    }

    {
      PersistenceManager pm = pmf.getPersistenceManager();
      Issue290Parent parent = (Issue290Parent)pm.getObjectById(parentId);
      assertEquals("Number of children persisted was wrong", 2, parent.getChildren().size());

      Issue290Child child1 = (Issue290Child)pm.getObjectById(child1Id);

      // Remove from collection. Since not dependent, should leave in datastore
      parent.getChildren().remove(child1);
      assertFalse(JDOHelper.isDeleted(child1));
      pm.close();
    }

    {
      PersistenceManager pm = pmf.getPersistenceManager();
      Issue290Parent parent = (Issue290Parent)pm.getObjectById(parentId);
      assertEquals("Number of children persisted was wrong", 1, parent.getChildren().size());

      try {
        pm.getObjectById(child1Id);
      } catch (JDOObjectNotFoundException onfe) {
        fail("Element was deleted but should be in datastore still after remove");
      }
    }
  }
}
