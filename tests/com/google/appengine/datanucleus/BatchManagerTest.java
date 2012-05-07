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
package com.google.appengine.datanucleus;

import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.store.ObjectProvider;
import org.easymock.EasyMock;

import com.google.appengine.datanucleus.jdo.JDOTestCase;

import java.util.HashMap;
import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public class BatchManagerTest extends JDOTestCase {

  private static class MyBatchManager extends BatchManager<Object> {

    String getOperation() {
      return "test";
    }

    void processBatchState(DatastorePersistenceHandler handler, List<Object> batchStateList) {
    }
  }

  public void testLegalWorkflow_NoOps() {
    BatchManager bm = new MyBatchManager();
    assertFalse(bm.batchOperationInProgress());
    bm.start();
    assertTrue(bm.batchOperationInProgress());
    bm.finish(null);
    assertFalse(bm.batchOperationInProgress());
  }

  private ObjectProvider newStateManagerMock() {
    return EasyMock.createNiceMock(ObjectProvider.class);
  }

  public void testLegalWorkflow() throws IllegalAccessException, NoSuchFieldException {
    final List<?> providedBatchStateList = Utils.newArrayList();
    BatchManager<Object> bm = new MyBatchManager() {
      @Override
      void processBatchState(DatastorePersistenceHandler handler, List batchStateList) {
        providedBatchStateList.addAll(batchStateList);
      }
    };
    assertFalse(bm.batchOperationInProgress());
    bm.start();
    assertTrue(bm.batchOperationInProgress());
    final ObjectProvider sm1 = newStateManagerMock();
    final ObjectProvider sm2 = newStateManagerMock();
    bm.add(sm1);
    bm.add(sm2);
    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    DatastoreManager dm = new DatastoreManager(
            jpm.getExecutionContext().getClassLoaderResolver(), jpm.getExecutionContext().getNucleusContext(), 
            new HashMap<String, Object>());
    bm.finish(new DatastorePersistenceHandler(dm));
    assertEquals(Utils.newArrayList(sm1, sm2), providedBatchStateList);
    assertFalse(bm.batchOperationInProgress());
  }

  public void testLegalWorkflowWithException() throws IllegalAccessException, NoSuchFieldException {
    BatchManager<Object> bm = new MyBatchManager() {
      @Override
      void processBatchState(DatastorePersistenceHandler handler, List batchStateList) {
        throw new RuntimeException("yar");
      }
    };
    assertFalse(bm.batchOperationInProgress());
    bm.start();
    assertTrue(bm.batchOperationInProgress());
    final ObjectProvider sm1 = newStateManagerMock();
    final ObjectProvider sm2 = newStateManagerMock();
    bm.add(sm1);
    bm.add(sm2);
    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    DatastoreManager dm = new DatastoreManager(
            jpm.getExecutionContext().getClassLoaderResolver(), jpm.getExecutionContext().getNucleusContext(), 
            new HashMap<String, Object>());
    try {
      bm.finish(new DatastorePersistenceHandler(dm));
      fail("expected rte");
    } catch (RuntimeException rte) {
      // good
    }
    assertFalse(bm.batchOperationInProgress());
  }
}
