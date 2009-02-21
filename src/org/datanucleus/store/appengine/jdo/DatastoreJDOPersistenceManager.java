// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.jdo;

import com.google.appengine.api.datastore.Key;

import org.datanucleus.jdo.JDOPersistenceManager;
import org.datanucleus.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.store.appengine.EntityUtils;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreJDOPersistenceManager extends JDOPersistenceManager {

  public DatastoreJDOPersistenceManager(JDOPersistenceManagerFactory apmf, String userName, String password) {
    super(apmf, userName, password);
  }

  /**
   * We override this method to make it easier for users to lookup objects
   * when they only have the name or the id component of the {@link Key}.
   * Given a {@link Class} and the nname or id a user can always construct a
   * {@link Key}, but that introduces app-engine specific dependencies into
   * the code and we're trying to let users keep things portable.  Furthermore,
   * if the primary key field on the object is a {@link String}, the user,
   * after constructing the {@link Key}, needs to call another app-engine
   * specific method to convert the {@link Key} into a {@link String}.  So
   * in addition to writing code that isn't portable, users have to write a lot
   * of code.  This override determines if the provided key is just an id or
   * name, and if so it constructs an appropriate {@link Key} on the user's
   * behalf and then calls the parent's implementation of this method.
   */
  @Override
  public Object getObjectById(Class cls, Object key) {
    key = EntityUtils.idOrNameToInternalKey(getObjectManager(), cls, key);
    return super.getObjectById(cls, key);
  }
}
