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

import org.datanucleus.StateManager;
import org.datanucleus.jdo.JDOPersistenceManager;
import org.easymock.EasyMock;

import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public class BatchManagerTest extends JDOTestCase {

  public void testLegalWorkflow_NoOps() {
    BatchManager bm = new BatchManager();
    assertFalse(bm.isBatchOperation());
    bm.startBatchOperation();
    assertTrue(bm.isBatchOperation());
    bm.finishBatchOperation(null);
    assertFalse(bm.isBatchOperation());
  }

  private StateManager newStateManagerMock() {
    return EasyMock.createNiceMock(StateManager.class);
  }

  public void testLegalWorkflow() throws IllegalAccessException, NoSuchFieldException {
    BatchManager bm = new BatchManager();
    assertFalse(bm.isBatchOperation());
    bm.startBatchOperation();
    assertTrue(bm.isBatchOperation());
    final StateManager sm1 = newStateManagerMock();
    final StateManager sm2 = newStateManagerMock();
    bm.addInsertion(sm1);
    bm.addInsertion(sm2);
    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    DatastoreManager dm = new DatastoreManager(
            jpm.getObjectManager().getClassLoaderResolver(), jpm.getObjectManager().getOMFContext());
    bm.finishBatchOperation(new DatastorePersistenceHandler(dm) {
      @Override
      void insertObjects(List<StateManager> sms) {
        assertEquals(Utils.newArrayList(sm1, sm2), sms);
      }
    });
    assertFalse(bm.isBatchOperation());
  }

  public void testLegalWorkflowWithException() throws IllegalAccessException, NoSuchFieldException {
    BatchManager bm = new BatchManager();
    assertFalse(bm.isBatchOperation());
    bm.startBatchOperation();
    assertTrue(bm.isBatchOperation());
    final StateManager sm1 = newStateManagerMock();
    final StateManager sm2 = newStateManagerMock();
    bm.addInsertion(sm1);
    bm.addInsertion(sm2);
    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    DatastoreManager dm = new DatastoreManager(
            jpm.getObjectManager().getClassLoaderResolver(), jpm.getObjectManager().getOMFContext());
    try {
      bm.finishBatchOperation(new DatastorePersistenceHandler(dm) {
        @Override
        void insertObjects(List<StateManager> sms) {
          assertEquals(Utils.newArrayList(sm1, sm2), sms);
          throw new RuntimeException("yar");
        }
      });
      fail("expected rte");
    } catch (RuntimeException rte) {
      // good
    }
    assertFalse(bm.isBatchOperation());
  }
}
