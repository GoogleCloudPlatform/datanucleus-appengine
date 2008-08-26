// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.DatastoreServiceFactory;

/**
 * {@link DatastoreServiceFactory} doesn't provide an easy way to configure
 * a specific instance to be returned, and that makes unit testing of classes
 * that need a {@link DatastoreService} difficult.  Typically we'd use Guice
 * to inject instances of {@link DatastoreService} into the various plugin
 * classes that need them, but we don't want to force our users to have a
 * runtime dependency on Guice.  Instead, we're kicking it old-school -
 * our own factory with a static setter that lets you "install" the instance
 * you want returned.  If you never call the setter,
 * {@link #getDatastoreService} will return the result of
 * {@link DatastoreServiceFactory#getDatastoreService()}.
 *
 * If you find yourself calling {@link #setDatastoreService} from production
 * code you're probably doing something wrong.  It should really only be used
 * for tests.
 *
 * @author Max Ross <maxr@google.com>
 */
public final class DatastoreServiceFactoryInternal {

  private DatastoreServiceFactoryInternal() {}

  private static DatastoreService datastoreServiceToReturn = null;

  public static DatastoreService getDatastoreService() {
    if (datastoreServiceToReturn != null) {
      return datastoreServiceToReturn;
    }
    return DatastoreServiceFactory.getDatastoreService();
  }

  public static void setDatastoreService(DatastoreService ds) {
    datastoreServiceToReturn = ds;
  }
}
