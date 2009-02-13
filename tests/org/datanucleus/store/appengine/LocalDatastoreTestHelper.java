// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.DatastoreConfig;
import com.google.appengine.api.datastore.ImplicitTransactionManagementPolicy;
import com.google.appengine.api.datastore.dev.LocalDatastoreService;
import com.google.appengine.tools.development.ApiProxyLocalImpl;
import com.google.appengine.tools.development.LocalRpcService;
import com.google.apphosting.api.ApiProxy;

import java.io.File;

/**
 * A test helper that sets up a datastore service that can be used in tests.
 *
 * @author Max Ross <maxr@google.com>
 */
public class LocalDatastoreTestHelper {
  
  public DatastoreService ds;

  private static final ApiProxy.Environment ENV = new ApiProxy.Environment() {
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
  };

  public void setUp() {
    ApiProxyLocalImpl delegate = new ApiProxyLocalImpl();
    // run completely in-memory
    delegate.setProperty(LocalDatastoreService.NO_STORAGE_PROPERTY, Boolean.TRUE.toString());
    // don't expire queries - makes debugging easier
    delegate.setProperty(LocalDatastoreService.MAX_QUERY_LIFETIME_PROPERTY,
        Integer.toString(Integer.MAX_VALUE));
    // don't expire txns - makes debugging easier
    delegate.setProperty(LocalDatastoreService.MAX_TRANSACTION_LIFETIME_PROPERTY,
        Integer.toString(Integer.MAX_VALUE));
    ds = DatastoreServiceFactory.getDatastoreService();
    ApiProxy.setDelegate(delegate);
    ApiProxy.setEnvironmentForCurrentThread(ENV);
  }

  public void tearDown(boolean exceptionIfActiveTxn) {
    Transaction txn = ds.getCurrentTransaction(null);
    try {
      if (txn != null) {
        try {
          txn.rollback();
        } finally {
          if (exceptionIfActiveTxn) {
            throw new IllegalStateException("Datastore service still has an active txn.  Please "
                + "rollback or commit all txns before test completes.");
          }
        }
      }
    } finally {
      ApiProxy.clearEnvironmentForCurrentThread();
      ApiProxyLocalImpl delegate = (ApiProxyLocalImpl) ApiProxy.getDelegate();
      delegate.stop();
    }
  }
}
