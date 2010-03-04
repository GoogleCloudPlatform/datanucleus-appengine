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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @author Max Ross <maxr@google.com>
 */
abstract class JDOBatchTestCase extends JDOTestCase {

  BatchRecorder batchRecorder;

  static abstract class BatchRecorder implements InvocationHandler {

    private final DatastoreService delegate;
    int batchOps = 0;

    public BatchRecorder(DatastoreServiceConfig config) {
      this.delegate = DatastoreServiceFactoryInternal.getDatastoreService(config);
    }

    public Object invoke(Object o, Method method, Object[] objects) throws Throwable {
      if (isBatchMethod(method)) {
        batchOps++;
      }
      try {
        return method.invoke(delegate, objects);
      } catch (InvocationTargetException ite) {
        throw ite.getTargetException();
      }
    }

    abstract boolean isBatchMethod(Method m);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    batchRecorder = newBatchRecorder();
    DatastoreService recordBatchPuts = (DatastoreService)
        Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] {DatastoreService.class}, batchRecorder);
    DatastoreServiceFactoryInternal.setDatastoreService(recordBatchPuts);
  }

  abstract BatchRecorder newBatchRecorder();

}
