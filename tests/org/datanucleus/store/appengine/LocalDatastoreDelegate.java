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

import com.google.appengine.api.datastore.dev.LocalDatastoreService;
import com.google.appengine.tools.development.ApiProxyLocalImpl;
import com.google.apphosting.api.ApiProxy;

import java.io.File;

/**
 * {@link DatastoreDelegate} implementation that integrates with the stub
 * datastore that ships with the sdk.
 *
 * @author Max Ross <maxr@google.com>
 */
class LocalDatastoreDelegate implements DatastoreDelegate {

  private ApiProxyLocalImpl localProxy;

  public void setUp() throws Exception {
    localProxy = new ApiProxyLocalImpl(new File(".")) {};
    // run completely in-memory
    localProxy.setProperty(LocalDatastoreService.NO_STORAGE_PROPERTY, Boolean.TRUE.toString());
    // don't expire queries - makes debugging easier
    localProxy.setProperty(LocalDatastoreService.MAX_QUERY_LIFETIME_PROPERTY,
        Integer.toString(Integer.MAX_VALUE));
    // don't expire txns - makes debugging easier
    localProxy.setProperty(LocalDatastoreService.MAX_TRANSACTION_LIFETIME_PROPERTY,
        Integer.toString(Integer.MAX_VALUE));
  }

  public void tearDown() throws Exception {
    localProxy.stop();
  }

  public byte[] makeSyncCall(ApiProxy.Environment environment, String packageName,
      String methodName, byte[] request) throws ApiProxy.ApiProxyException {
    return localProxy.makeSyncCall(environment, packageName, methodName, request);
  }

  public void log(ApiProxy.Environment environment, ApiProxy.LogRecord logRecord) {
    localProxy.log(environment, logRecord);
  }
}
