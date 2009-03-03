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
import com.google.appengine.api.datastore.DatastoreServiceFactory;

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

  /**
   * @return If a {@link DatastoreService} to return has been explicitly provided by a
   * call to
   * {@link #setDatastoreService(DatastoreService)},
   * the explicitly provided instance.  Otherwise a {@link DatastoreService}
   * constructed by calling.
   * {@link DatastoreServiceFactory#getDatastoreService()}
   * .
   */
  public static DatastoreService getDatastoreService() {
    if (datastoreServiceToReturn != null) {
      return datastoreServiceToReturn;
    }
    // Wrap the service in an impl that properly translates the runtime
    // exceptions thrown by the datastore api
    return new RuntimeExceptionWrappingDatastoreService(
        DatastoreServiceFactory.getDatastoreService());
  }

  /**
   * Provides a specific {@link DatastoreService} instance that will be the
   * return value of all calls to {@link #getDatastoreService()}.  If
   * {@code null} is provided, subsequent calls to {@link #getDatastoreService()}
   * will return to its default behavior.
   *
   * @param ds The {@link DatastoreService} to be returned by all calls to
   * {@link #getDatastoreService()}.  Can be null.
   */
  public static void setDatastoreService(DatastoreService ds) {
    datastoreServiceToReturn = ds;
  }
}
