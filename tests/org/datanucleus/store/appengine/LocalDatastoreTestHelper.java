// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.DatastoreServiceFactory;
import com.google.apphosting.api.DatastoreConfig;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.AppEngineWebXml;
import com.google.apphosting.tools.development.ApiProxyLocalImpl;

/**
 * A test helper that sets up a datastore service that can be used in tests.
 *
 * @author Max Ross <maxr@google.com>
 */
public class LocalDatastoreTestHelper {
  
  public DatastoreService ds;
  public void setUp() {
    ds = DatastoreServiceFactory.getDatastoreService(DatastoreConfig.DEFAULT);
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

      public AppEngineWebXml getAppEngineWebXml() {
        return new AppEngineWebXml() {
          public DatastoreConfig getDefaultDatastoreConfig() {
            return DatastoreConfig.DEFAULT;
          }
        };
      }
    });

  }

  public void tearDown() {
    ApiProxy.clearEnvironmentForCurrentThread();
  }
}
