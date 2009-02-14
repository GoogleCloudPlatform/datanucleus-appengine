// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.tools.development.ApiProxyLocalImpl;
import com.google.appengine.api.datastore.dev.LocalDatastoreService;
import com.google.apphosting.api.ApiProxy;

/**
 * {@link DatastoreDelegate} implementation that integrates with the stub
 * datastore that ships with the sdk.
 *
 * @author Max Ross <maxr@google.com>
 */
class LocalDatastoreDelegate implements DatastoreDelegate {

  private ApiProxyLocalImpl localProxy;

  public void setUp() throws Exception {
    localProxy = new ApiProxyLocalImpl();
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
