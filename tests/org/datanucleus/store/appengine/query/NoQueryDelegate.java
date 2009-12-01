/*
 * Copyright (C) 2009 Max Ross.
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
package org.datanucleus.store.appengine.query;

import com.google.appengine.api.datastore.dev.LocalDatastoreService;
import com.google.apphosting.api.ApiProxy;

import java.util.concurrent.Future;

/**
 * A delegate that throws a {@link RuntimeException} whenever RunQuery is
 * invoked on the datastore service.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
public class NoQueryDelegate implements ApiProxy.Delegate {
  private final ApiProxy.Delegate original = ApiProxy.getDelegate();
  public byte[] makeSyncCall(ApiProxy.Environment environment, String pkg, String method, byte[] bytes)
      throws ApiProxy.ApiProxyException {
    if (pkg.equals(LocalDatastoreService.PACKAGE) && method.equals("RunQuery")) {
      throw new RuntimeException("boom");
    }
    return original.makeSyncCall(environment, pkg, method, bytes);
  }

  public Future makeAsyncCall(ApiProxy.Environment environment, String pkg, String method, byte[] bytes,
                              ApiProxy.ApiConfig apiConfig) {
    if (pkg.equals(LocalDatastoreService.PACKAGE) && method.equals("RunQuery")) {
      throw new RuntimeException("boom");
    }
    return original.makeAsyncCall(environment, pkg, method, bytes, apiConfig);
  }

  public void log(ApiProxy.Environment environment, ApiProxy.LogRecord logRecord) {
    original.log(environment, logRecord);
  }

  public NoQueryDelegate install() {
    ApiProxy.setDelegate(this);
    return this;
  }

  public void uninstall() {
    ApiProxy.setDelegate(original);
  }
}

