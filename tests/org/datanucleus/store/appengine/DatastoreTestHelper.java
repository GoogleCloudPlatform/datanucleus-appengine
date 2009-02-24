// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.apphosting.api.ApiProxy;

/**
 * A test helper that sets up a datastore service that can be used in tests.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreTestHelper {

  public DatastoreService ds;

  public static final ApiProxy.Environment ENV = new ApiProxy.Environment() {
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

    public String getRequestNamespace() {
      throw new UnsupportedOperationException();
    }

    public String getDefaultNamespace() {
      throw new UnsupportedOperationException();
    }

    public void setDefaultNamespace(String s) {
      throw new UnsupportedOperationException();
    }
  };

  private final DatastoreDelegate delegate = getDatastoreDelegate();

  private static final String DATASTORE_DELEGATE_PROP = "orm.DatastoreDelegate";

  public void setUp() throws Exception {
    delegate.setUp();
    ApiProxy.setDelegate(delegate);
    ApiProxy.setEnvironmentForCurrentThread(ENV);
    ds = DatastoreServiceFactory.getDatastoreService();
  }

  public void tearDown(boolean exceptionIfActiveTxn) throws Exception {
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
      delegate.tearDown();
    }
  }

  private DatastoreDelegate getDatastoreDelegate() {
    String helperClass =
        System.getProperty(DATASTORE_DELEGATE_PROP, LocalDatastoreDelegate.class.getName());
    try {
      return (DatastoreDelegate) Class.forName(helperClass).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
