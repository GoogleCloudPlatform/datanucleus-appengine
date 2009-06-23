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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Utility for disabling datastore writes.  Useful for proving
 * that operations that shouldn't write to the datastore aren't
 * actually writing to the datastore.
 *
 * If you call
 * {@link #installNoWritesDatastoreService()} make sure you call
 * {@link #uninstallNoWritesDatastoreService()} in a finally block.
 *
 * @author Max Ross <maxr@google.com>
 */
public final class WriteBlocker {

  private static DatastoreService ORIGINAL_DATASTORE_SERVICE;

  private static final class Handler implements InvocationHandler {
    private final DatastoreService delegate;

    private Handler(DatastoreService delegate) throws NoSuchMethodException {
      this.delegate = delegate;
    }

    public Object invoke(Object o, Method method, Object[] objects) throws Throwable {
      if (method.getName().equals("put") || method.getName().equals("delete")) {
        throw new RuntimeException("Detected a write: " + method);
      }
      return method.invoke(delegate, objects);
    }
  }

  public static void installNoWritesDatastoreService()
      throws NoSuchMethodException {
    ORIGINAL_DATASTORE_SERVICE = DatastoreServiceFactoryInternal.getDatastoreService();
    Handler handler = new Handler(ORIGINAL_DATASTORE_SERVICE);
    DatastoreService ds = (DatastoreService) Proxy.newProxyInstance(
        WriteBlocker.class.getClassLoader(),
        new Class[] {DatastoreService.class}, handler);
    DatastoreServiceFactoryInternal.setDatastoreService(ds);
  }

  public static void uninstallNoWritesDatastoreService() {
    DatastoreServiceFactoryInternal.setDatastoreService(ORIGINAL_DATASTORE_SERVICE);
  }
}

