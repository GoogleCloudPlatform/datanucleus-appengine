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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NoPersistenceInformationException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.VersionMetaData;

import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;

/**
 * Utility methods for determining entity property names and kinds.
 *
 * @author Max Ross <maxr@google.com>
 */
public final class EntityUtils {
  /**
   * Method to return the property name to use for storing the specified member.
   * @param idFactory IdentifierFactory
   * @param ammd Metadata for the field/property
   * @return The property name to use in the datastore
   */
  public static String getPropertyName(IdentifierFactory idFactory, AbstractMemberMetaData ammd) {
    AbstractClassMetaData acmd = ammd.getAbstractClassMetaData();
    VersionMetaData vermd = acmd.getVersionMetaDataForClass();
    if (acmd.isVersioned() && ammd.getName().equals(vermd.getFieldName())) {
      return getVersionPropertyName(idFactory, acmd.getVersionMetaData());
    }

    // If a column name was explicitly provided, use that as the property name.
    if (ammd.getColumn() != null) {
      return ammd.getColumn();
    }

    // If we're dealing with embeddables, the column name override will show up as part of the column meta data.
    if (ammd.getColumnMetaData() != null && ammd.getColumnMetaData().length > 0) {
      if (ammd.getColumnMetaData().length != 1) {
        throw new NucleusUserException("Field " + ammd.getFullFieldName() +
            " has been specified with more than 1 column! This is unsupported with GAE/J");
      }
      return ammd.getColumnMetaData()[0].getName();
    }

    // Use the IdentifierFactory to convert from the name of the field into a property name.
    return idFactory.newDatastoreFieldIdentifier(ammd.getName()).getIdentifierName();
  }

  /**
   * Accessor for the property name to use for the version.
   * @param idFactory Identifier factory
   * @param vmd Version metadata
   * @return The property name
   */
  public static String getVersionPropertyName(
      IdentifierFactory idFactory, VersionMetaData vmd) {
    ColumnMetaData[] columnMetaData = vmd.getColumnMetaData();
    if (columnMetaData == null || columnMetaData.length == 0) {
      return idFactory.newVersionFieldIdentifier().getIdentifierName();
    }
    if (columnMetaData.length != 1) {
      throw new IllegalArgumentException("Please specify 0 or 1 column name for the version property.");
    }
    return columnMetaData[0].getName();
  }

  /**
   * Accessor for the property name to use for the discriminator.
   * @param idFactory Identifier factory
   * @param dismd Discriminator metadata
   * @return The property name
   */
  public static String getDiscriminatorPropertyName(
      IdentifierFactory idFactory, DiscriminatorMetaData dismd) {
    ColumnMetaData columnMetaData = dismd.getColumnMetaData();
    if (columnMetaData == null) {
      return idFactory.newDiscriminatorFieldIdentifier().getIdentifierName();
    }
    return columnMetaData.getName();
  }

  /**
   * Method to set a property in the supplied entity, and uses the provided metadata component to
   * decide if it is indexed or not.
   * @param entity The entity
   * @param md Metadata component
   * @param propertyName Name of the property to use in the entity
   * @param value The value to set
   */
  public static void setEntityProperty(Entity entity, MetaData md, String propertyName, Object value) {
    boolean unindexed = false;
    String val = md.getValueForExtension(DatastoreManager.UNINDEXED_PROPERTY);
    if (val != null && val.equalsIgnoreCase("true")) {
      unindexed = true;
    } else if (md instanceof VersionMetaData && ((VersionMetaData)md).getFieldName() != null) {
      // Version : Check against the metadata of the field
      VersionMetaData vmd = (VersionMetaData)md;
      AbstractMemberMetaData vermmd = ((AbstractClassMetaData)vmd.getParent()).getMetaDataForMember(vmd.getFieldName());
      val = vermmd.getValueForExtension(DatastoreManager.UNINDEXED_PROPERTY);
      unindexed = (val != null && val.equalsIgnoreCase("true"));
    }

    if (unindexed) {
      entity.setUnindexedProperty(propertyName, value);
    } else {
      entity.setProperty(propertyName, value);
    }
  }

  public static String determineKind(AbstractClassMetaData acmd, ExecutionContext ec) {
    MappedStoreManager storeMgr = (MappedStoreManager) ec.getStoreManager();
    return determineKind(acmd, storeMgr, ec.getClassLoaderResolver());
  }

  public static String determineKind(AbstractClassMetaData acmd, MappedStoreManager storeMgr, ClassLoaderResolver clr) {
    DatastoreClass table = storeMgr.getDatastoreClass(acmd.getFullClassName(), clr);
    if (table == null) {
      // We've seen this when there is a class with the superclass inheritance
      // strategy that does not have a parent
      throw new NoPersistenceInformationException(acmd.getFullClassName());
    }
    return table.getIdentifier().getIdentifierName();
  }

  /**
   * @see DatastoreIdentityKeyTranslator for an explanation of how this is useful.
   *
   * Supported translations:
   * <ul>
   * <li>When the pk field is a Long you can give us a Long, an encoded key string, or a Key.</li>
   * <li>When the pk field is an unencoded String you can give us an unencoded String, an encoded String, or a Key.</li>
   * <li>When the pk field is an encoded String you can give us an unencoded String, an encoded String, or a Key.</li>
   * </ul>
   */
  public static Object idToInternalKey(ExecutionContext ec, Class<?> cls, Object val, boolean allowSubclasses) {
    AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(
        cls, ec.getClassLoaderResolver());
    String kind = determineKind(cmd, ec);
    AbstractMemberMetaData pkMemberMetaData = getPKMemberMetaData(ec, cls);
    return idToInternalKey(kind, pkMemberMetaData, cls, val, ec, allowSubclasses);
  }

  // broken out for testing
  static Object idToInternalKey(
      String kind, AbstractMemberMetaData pkMemberMetaData, Class<?> cls, Object val,
      ExecutionContext ec, boolean allowSubclasses) {
    Object result = null;
    Class<?> pkType = pkMemberMetaData.getType();
    if (val instanceof String) {
      result = stringToInternalKey(kind, pkType, pkMemberMetaData, cls, val);
    } else if (val instanceof Long || val instanceof Integer) {
      result = intOrLongToInternalKey(kind, pkType, pkMemberMetaData, cls, val);
    } else if (val instanceof Key) {
      result = keyToInternalKey(kind, pkType, pkMemberMetaData, cls, (Key) val, ec, allowSubclasses);
    }
    if (result == null && val != null) {
      // missed a case somewhere
      throw new NucleusFatalUserException(
          "Received a request to find an object of type " + cls.getName() + " identified by "
          + val + ".  This is not a valid representation of a primary key for an instance of "
          + cls.getName() + ".");
    }
    return result;
  }

  static Key getPkAsKey(Object pk, AbstractClassMetaData acmd, ExecutionContext ec) {
    if (pk == null) {
      throw new IllegalStateException(
          "Primary key for object of type " + acmd.getName() + " is null.");
    } else if (pk instanceof Key) {
      return (Key) pk;
    } else if (pk instanceof String) {
      if (DatastoreManager.hasEncodedPKField(acmd)) {
        return KeyFactory.stringToKey((String) pk);
      } else {
        String kind = EntityUtils.determineKind(acmd, ec);
        return KeyFactory.createKey(kind, (String) pk);
      }
    } else if (pk instanceof Long) {
      String kind = EntityUtils.determineKind(acmd, ec);
      return KeyFactory.createKey(kind, (Long) pk);
    } else {
      throw new IllegalStateException(
          "Primary key for object of type " + acmd.getName()
              + " is of unexpected type " + pk.getClass().getName()
              + " (must be String, Long, or " + Key.class.getName() + ")");
    }
  }

  private static boolean keyKindIsValid(String kind, AbstractMemberMetaData pkMemberMetaData,
                                        Class<?> cls, Key key, ExecutionContext ec, boolean allowSubclasses) {

    if (key.getKind().equals(kind)) {
      return true;
    }

    if (!allowSubclasses) {
      return false;
    }

    MetaDataManager mdm = ec.getMetaDataManager();
    // see if the key kind is a subclass of the requested kind
    String[] subclasses = mdm.getSubclassesForClass(cls.getName(), true);
    if (subclasses != null) {
      for (String subclass : subclasses) {
        AbstractClassMetaData subAcmd = mdm.getMetaDataForClass(subclass, ec.getClassLoaderResolver());
        if (key.getKind().equals(determineKind(subAcmd, ec))) {
          return true;
        }
      }
    }
    return false;
  }

  // TODO(maxr): This method is generally useful.  Consider making it public
  // and refactoring the error messages so that they aren't specific to
  // object lookups.
  private static Object keyToInternalKey(String kind, Class<?> pkType,
                                         AbstractMemberMetaData pkMemberMetaData, Class<?> cls,
                                         Key key, ExecutionContext ec, boolean allowSubclasses) {
    Object result = null;
    if (!keyKindIsValid(kind, pkMemberMetaData, cls, key, ec, allowSubclasses)) {
      throw new NucleusFatalUserException(
          "Received a request to find an object of kind " + kind + " but the provided "
          + "identifier is a Key for kind " + key.getKind());
    }
    if (!key.isComplete()) {
      throw new NucleusFatalUserException(
          "Received a request to find an object of kind " + kind + " but the provided "
          + "identifier is is an incomplete Key");
    }
    if (pkType.equals(String.class)) {
      if (pkMemberMetaData.hasExtension(DatastoreManager.ENCODED_PK)) {
        result = KeyFactory.keyToString(key);
      } else {
        if (key.getParent() != null) {
          // By definition, classes with unencoded string pks
          // do not have parents.  Since this key has a parent
          // this isn't valid input.
          throw new NucleusFatalUserException(
              "Received a request to find an object of type " + cls.getName() + ".  The primary "
              + "key for this type is an unencoded String, which means instances of this type "
              + "never have parents.  However, the Key that was provided as an argument has a "
              + "parent.");
        }
        result = key.getName();
      }
    } else if (pkType.equals(Long.class)) {
      if (key.getParent() != null) {
        // By definition, classes with unencoded string pks
        // do not have parents.  Since this key has a parent
        // this isn't valid input.
        throw new NucleusFatalUserException(
            "Received a request to find an object of type " + cls.getName() + ".  The primary "
            + "key for this type is a Long, which means instances of this type "
            + "never have parents.  However, the Key that was provided as an argument has a "
            + "parent.");
      }
      if (key.getName() != null) {
        throw new NucleusFatalUserException(
            "Received a request to find an object of type " + cls.getName() + ".  The primary "
            + "key for this type is a Long.  However, the encoded string "
            + "representation of the Key that was provided as an argument has its name field "
            + "set, not its id.  This makes it an invalid key for this class.");
      }
      result = key.getId();
    } else if (pkType.equals(Key.class)) {
      result = key;
    }
    return result;
  }

  private static Object intOrLongToInternalKey(
      String kind, Class<?> pkType, AbstractMemberMetaData pkMemberMetaData, Class<?> cls, Object val) {
    Object result = null;
    Key keyWithId = KeyFactory.createKey(kind, ((Number) val).longValue());
    if (pkType.equals(String.class)) {
      if (pkMemberMetaData.hasExtension(DatastoreManager.ENCODED_PK)) {
        result = KeyFactory.keyToString(keyWithId);
      } else {
        throw new NucleusFatalUserException(
            "Received a request to find an object of type " + cls.getName() + ".  The primary "
            + "key for this type is an unencoded String.  However, the provided value is of type "
            + val.getClass().getName() + ".");
      }
    } else if (pkType.equals(Long.class)) {
      result = keyWithId.getId();
    } else if (pkType.equals(Key.class)) {
      result = keyWithId;
    }
    return result;
  }

  private static Object stringToInternalKey(
      String kind, Class<?> pkType, AbstractMemberMetaData pkMemberMetaData, Class<?> cls, Object val) {
    Key decodedKey;
    Object result = null;
    try {
      decodedKey = KeyFactory.stringToKey((String) val);
      if (!decodedKey.isComplete()) {
        throw new NucleusFatalUserException(
            "Received a request to find an object of kind " + kind + " but the provided "
            + "identifier is the String representation of an incomplete Key for kind "
            + decodedKey.getKind());
      }
    } catch (IllegalArgumentException iae) {
      if (pkType.equals(Long.class)) {
        // We were given an unencoded String and the pk type is Long.
        // There's no way that can be valid
        throw new NucleusFatalUserException(
            "Received a request to find an object of type " + cls.getName() + " identified by the String "
            + val + ", but the primary key of " + cls.getName() + " is of type Long.");
      }
      // this is ok, it just means we were only given the name
      decodedKey = KeyFactory.createKey(kind, (String) val);
    }
    if (!decodedKey.getKind().equals(kind)) {
      throw new NucleusFatalUserException(
          "Received a request to find an object of kind " + kind + " but the provided "
          + "identifier is the String representation of a Key for kind "
          + decodedKey.getKind());
    }
    if (pkType.equals(String.class)) {
      if (pkMemberMetaData.hasExtension(DatastoreManager.ENCODED_PK)) {
        // Need to make sure we pass on an encoded pk
        result = KeyFactory.keyToString(decodedKey);
      } else {
        if (decodedKey.getParent() != null) {
          throw new NucleusFatalUserException(
              "Received a request to find an object of type " + cls.getName() + ".  The primary "
              + "key for this type is an unencoded String, which means instances of this type "
              + "never have parents.  However, the encoded string representation of the Key that "
              + "was provided as an argument has a parent.");
        }
        // Pk is an unencoded string so need to pass on just the name
        // component.  However, we need to make sure the provided key actually
        // contains a name component.
        if (decodedKey.getName() == null) {
          throw new NucleusFatalUserException(
              "Received a request to find an object of type " + cls.getName() + ".  The primary "
              + "key for this type is an unencoded String.  However, the encoded string "
              + "representation of the Key that was provided as an argument has its id field "
              + "set, not its name.  This makes it an invalid key for this class.");
        }
        result = decodedKey.getName();
      }
    } else if (pkType.equals(Long.class)) {
      if (decodedKey.getParent() != null) {
        throw new NucleusFatalUserException(
            "Received a request to find an object of type " + cls.getName() + ".  The primary "
            + "key for this type is a Long, which means instances of this type "
            + "never have parents.  However, the encoded string representation of the Key that "
            + "was provided as an argument has a parent.");
      }

      if (decodedKey.getName() != null) {
        throw new NucleusFatalUserException(
            "Received a request to find an object of type " + cls.getName() + " identified by the "
            + "encoded String representation of "
            + decodedKey + ", but the primary key of " + cls.getName() + " is of type Long and the "
            + "encoded String has its name component set.  It must have its id component set "
            + "instead in order to be legal.");
      }
      // pk is a long so just pass on the id component
      result = decodedKey.getId();
    } else if (pkType.equals(Key.class)) {
      result = decodedKey;
    }
    return result;
  }

  private static AbstractMemberMetaData getPKMemberMetaData(ExecutionContext ec, Class<?> cls) {
    AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(
            cls, ec.getClassLoaderResolver());
    return cmd.getMetaDataForManagedMemberAtAbsolutePosition(cmd.getPKMemberPositions()[0]);
  }

  /**
   * Get the active transaction.  Depending on the connection factory
   * associated with the store manager, this may establish a transaction if one
   * is not currently active.  This method will return null if the connection
   * factory associated with the store manager is nontransactional
   */
  public static DatastoreTransaction getCurrentTransaction(ExecutionContext ec) {
    ManagedConnection mconn = ec.getStoreManager().getConnection(ec);
    return ((EmulatedXAResource) mconn.getXAResource()).getCurrentTransaction();
  }

  public static Key getPrimaryKeyAsKey(ApiAdapter apiAdapter, ObjectProvider op) {
    Object primaryKey = apiAdapter.getTargetKeyForSingleFieldIdentity(op.getInternalObjectId());

    String kind =
        EntityUtils.determineKind(op.getClassMetaData(), op.getExecutionContext());
    if (primaryKey instanceof Key) {
      return (Key) primaryKey;
    } else if (primaryKey instanceof Long) {
      return KeyFactory.createKey(kind, (Long) primaryKey);
    }
    try {
      return KeyFactory.stringToKey((String) primaryKey);
    } catch (IllegalArgumentException iae) {
      return KeyFactory.createKey(kind, (String) primaryKey);
    }
  }
}
