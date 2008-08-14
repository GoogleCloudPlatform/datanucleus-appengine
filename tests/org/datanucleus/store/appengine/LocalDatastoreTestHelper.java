// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.DatastoreServiceFactory;
import com.google.apphosting.tools.development.ApiProxyLocalImpl;

import java.io.File;

/**
 * A test helper that sets up a datastore service that can be used in tests.
 *
 * @author Max Ross <maxr@google.com>
 */
public class LocalDatastoreTestHelper {
  
  public DatastoreService ds;
  public void setUp() {
    File f = new File("local_db.bin");
    f.delete();
    ds = DatastoreServiceFactory.getDatastoreService();
    ApiProxy.setDelegate(new ApiProxyLocalImpl());
    ApiProxy.setEnvironmentForCurrentThread(new ApiProxy.Environment() {
      public String getAppId() {
        return "test";
      }

      public String getVersionId() {
        return "1.0";
      }

      public String getEmail() {
        throw new UnsupportedOperationException();
      }

      public boolean isLoggedIn() {
        throw new UnsupportedOperationException();
      }

      public boolean isAdmin() {
        throw new UnsupportedOperationException();
      }

      public String getAuthDomain() {
        throw new UnsupportedOperationException();
      }
    });

  }

  public void tearDown() {
    ApiProxy.clearEnvironmentForCurrentThread();
  }
}
