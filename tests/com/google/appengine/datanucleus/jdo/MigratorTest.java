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

import java.util.Collection;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.identity.ObjectIdentity;

import com.google.appengine.datanucleus.Inner;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.Migrator;
import com.google.appengine.datanucleus.test.jdo.MigratorOneToManyChild;
import com.google.appengine.datanucleus.test.jdo.MigratorOneToManyParent;
import com.google.appengine.datanucleus.test.jdo.MigratorOneToOneChild;
import com.google.appengine.datanucleus.test.jdo.MigratorOneToOneParent;

/**
 * Some simple tests for the Migrator that was written to migrate data from the StorageVersion
 * "PARENTS_DO_NOT_REFER_TO_CHILDREN" across to "READ_OWNED_CHILD_KEYS_FROM_PARENTS".
 */
@Inner("Solve the problem of lazy loading of classes; e.g. no SuperclassTableInheritanceJDO#ChildToParentWithoutDiscriminator")
public class MigratorTest extends JDOTestCase {

  public void testMigrateOneToOneUni() {
    // Persist Parent+Child (1-1 relation) to old storage version
    PersistenceManagerFactory oldPMF = JDOHelper.getPersistenceManagerFactory("originalStorageVersion");
    PersistenceManager oldPM = oldPMF.getPersistenceManager();
    MigratorOneToOneParent p = new MigratorOneToOneParent();
    p.setName("First Parent");
    MigratorOneToOneChild c = new MigratorOneToOneChild();
    c.setName("Child 1");
    p.setChild(c);
    beginTxn();
    oldPM.makePersistent(p);
    commitTxn();
    ObjectIdentity pId = (ObjectIdentity)oldPM.getObjectId(p);
    ObjectIdentity cId = (ObjectIdentity)oldPM.getObjectId(c);
    Key pIdKey = (Key)pId.getKey();
    Key cIdKey = (Key)cId.getKey();
    oldPM.close();

    // Migrate the data
    Migrator migrator = new Migrator(((JDOPersistenceManagerFactory)oldPMF).getNucleusContext());
    try {
      // Migrate the parent
      Entity pEntity = ds.get(pIdKey);
      boolean changed = migrator.migrate(pEntity, MigratorOneToOneParent.class);
      assertTrue("Parent entity should have been changed but wasnt", changed);
      if (changed) {
        ds.put(pEntity);
      }

      // Migrate the child
      Entity cEntity = ds.get(cIdKey);
      changed = migrator.migrate(cEntity, MigratorOneToOneChild.class);
      assertFalse("Child entity shouldnt have been changed but was", changed);
      if (changed) {
        ds.put(cEntity);
      }

      assertTrue("Child keys property not added to parent", pEntity.hasProperty("child_id_OID"));
      Object propValue = pEntity.getProperty("child_id_OID");
      assertNotNull(propValue);
      assertEquals("Child key property value is not the same as the child key", cIdKey, propValue);
    } catch (EntityNotFoundException enfe) {
      fail("Some entity was not found");
    }

    oldPMF.close();
  }

  public void testMigrateOneToManyUni() {
    // Persist Parent+Child (1-N relation) to old storage version
    PersistenceManagerFactory oldPMF = JDOHelper.getPersistenceManagerFactory("originalStorageVersion");
    PersistenceManager oldPM = oldPMF.getPersistenceManager();
    MigratorOneToManyParent p = new MigratorOneToManyParent();
    p.setName("First Parent");
    MigratorOneToManyChild c = new MigratorOneToManyChild();
    c.setName("Child 1");
    p.addChild(c);
    beginTxn();
    oldPM.makePersistent(p);
    commitTxn();
    ObjectIdentity pId = (ObjectIdentity)oldPM.getObjectId(p);
    ObjectIdentity cId = (ObjectIdentity)oldPM.getObjectId(c);
    Key pIdKey = (Key)pId.getKey();
    Key cIdKey = (Key)cId.getKey();
    oldPM.close();

    // Migrate the data
    Migrator migrator = new Migrator(((JDOPersistenceManagerFactory)oldPMF).getNucleusContext());
    try {
      // Migrate the parent
      Entity pEntity = ds.get(pIdKey);
      boolean changed = migrator.migrate(pEntity, MigratorOneToManyParent.class);
      assertTrue("Parent entity should have been changed but wasnt", changed);
      if (changed) {
        ds.put(pEntity);
      }

      // Migrate the child
      Entity cEntity = ds.get(cIdKey);
      changed = migrator.migrate(cEntity, MigratorOneToManyChild.class);
      assertFalse("Child entity shouldnt have been changed but was", changed);
      if (changed) {
        ds.put(cEntity);
      }

      assertTrue("Child keys property not added to parent", pEntity.hasProperty("children"));
      Object propValue = pEntity.getProperty("children");
      assertNotNull(propValue);
      assertTrue("Child keys property in parent should be Collection!", propValue instanceof Collection);
      Collection<Key> childKeysProp = (Collection<Key>)propValue;
      assertEquals("Number of child keys stored is incorrect", 1, childKeysProp.size());
      assertEquals("Child key property value is not the same as the child key", cIdKey, childKeysProp.iterator().next());
    } catch (EntityNotFoundException enfe) {
      fail("Some entity was not found");
    }

    oldPMF.close();
  }
}
