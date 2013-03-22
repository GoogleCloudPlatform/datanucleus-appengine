/*
 * Copyright (C) 2010 Google Inc
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
 */
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * If you call
 * {@link #install} make sure you call
 * {@link #uninstall} in a finally block.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
public final class DatastoreServiceInterceptor {
  private static DatastoreService ORIGINAL_DATASTORE_SERVICE;

  private static final class Handler implements InvocationHandler {
    private final DatastoreService delegate;
    private final Policy policy;

    private Handler(DatastoreService delegate, Policy policy) {
      this.delegate = delegate;
      this.policy = policy;
    }

    public Object invoke(Object o, Method method, Object[] params) throws Throwable {
      policy.intercept(o, method, params);
      try {
        method.setAccessible(true);
        return method.invoke(delegate, params);
      } catch (InvocationTargetException ite) {
        // Always throw the real cause.
        throw ite.getTargetException();
      }
    }
  }

  public static void install(DatastoreManager storeManager, Policy policy) {
    DatastoreServiceConfig config = storeManager.getDefaultDatastoreServiceConfigForReads();
    ORIGINAL_DATASTORE_SERVICE = DatastoreServiceFactoryInternal.getDatastoreService(config);
    Handler handler = new Handler(ORIGINAL_DATASTORE_SERVICE, policy);
    DatastoreService ds = (DatastoreService) Proxy.newProxyInstance(
        DatastoreServiceInterceptor.class.getClassLoader(),
        new Class[] {DatastoreService.class}, handler);
    DatastoreServiceFactoryInternal.setDatastoreService(ds);
  }

  public static void uninstall() {
    DatastoreServiceFactoryInternal.setDatastoreService(ORIGINAL_DATASTORE_SERVICE);
  }

  public interface Policy {
    void intercept(Object o, Method method, Object[] params);
  }
}
