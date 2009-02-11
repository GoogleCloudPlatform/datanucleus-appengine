// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.jdo.JDOPersistenceManager;
import org.datanucleus.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.mapped.MappedStoreManager;

import javax.jdo.identity.StringIdentity;

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
  public Object getObjectById(Class cls, Object key) {
    if (key instanceof Integer || key instanceof Long) {
      // We only support pks of type Key and String so we know the user is
      // giving us the id component of a Key.
      AbstractClassMetaData cmd = getObjectManager().getMetaDataManager().getMetaDataForClass(
          cls, getObjectManager().getClassLoaderResolver());
      MappedStoreManager storeMgr = (MappedStoreManager) getObjectManager().getStoreManager();
      String kind = EntityUtils.determineKind(cmd, storeMgr.getIdentifierFactory());
      Key idKey = KeyFactory.createKey(kind, ((Number) key).longValue());
      key = overriddenKeyToKeyOrString(cmd, idKey);
    } else if (key instanceof String) {
      // We support pks of type String so it's not immediately clear whether
      // the user is giving us a serialized Key or just the name component of
      // the Key.  Try converting the provided value into a Key.  If we're
      // successful, we know the user gave us a serialized Key.  If we're not,
      // treat the value as the name component of a Key.
      try {
        KeyFactory.stringToKey((String) key);
      } catch (IllegalArgumentException iae) {
        // convert it to a named key
        AbstractClassMetaData cmd = getObjectManager().getMetaDataManager().getMetaDataForClass(
            cls, getObjectManager().getClassLoaderResolver());
        MappedStoreManager storeMgr = (MappedStoreManager) getObjectManager().getStoreManager();
        String kind = EntityUtils.determineKind(cmd, storeMgr.getIdentifierFactory());
        Key namedKey = KeyFactory.createKey(kind, (String) key);
        key = overriddenKeyToKeyOrString(cmd, namedKey);
      }
    }
    return super.getObjectById(cls, key);
  }

  private Object overriddenKeyToKeyOrString(AbstractClassMetaData cmd, Key overriddenKey) {
    return cmd.getObjectidClass().equals(StringIdentity.class.getName()) ?
           KeyFactory.keyToString(overriddenKey) : overriddenKey;
  }
}
