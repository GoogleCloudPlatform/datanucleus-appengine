// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.ObjectManager;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.store.appengine.jdo.DatastoreJDOPersistenceManager;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;

/**
 * Utility methods for determining entity property names and kinds.
 *
 * @author Max Ross <maxr@google.com>
 */
public final class EntityUtils {

  public static String getPropertyName(
      IdentifierFactory idFactory, AbstractMemberMetaData ammd) {
    // TODO(maxr): See if there is a better way than field name comparison to
    // determine if this is a version field
    AbstractClassMetaData acmd = ammd.getAbstractClassMetaData();
    if (acmd.hasVersionStrategy() &&
        ammd.getName().equals(acmd.getVersionMetaData().getFieldName())) {
      return getVersionPropertyName(idFactory, acmd.getVersionMetaData());
    }

    // If a column name was explicitly provided, use that as the property name.
    if (ammd.getColumn() != null) {
      return ammd.getColumn();
    }

    // If we're dealing with embeddables, the column name override
    // will show up as part of the column meta data.
    if (ammd.getColumnMetaData() != null && ammd.getColumnMetaData().length > 0) {
      if (ammd.getColumnMetaData().length != 1) {
        // TODO(maxr) throw something more appropriate
        throw new UnsupportedOperationException();
      }
      return ammd.getColumnMetaData()[0].getName();
    }
    // Use the IdentifierFactory to convert from the name of the field into
    // a property name.
    return idFactory.newDatastoreFieldIdentifier(ammd.getName()).getIdentifierName();
  }

  public static Object getVersionFromEntity(
      IdentifierFactory idFactory, VersionMetaData vmd, Entity entity) {
    return entity.getProperty(getVersionPropertyName(idFactory, vmd));
  }

  public static String getVersionPropertyName(
      IdentifierFactory idFactory, VersionMetaData vmd) {
    ColumnMetaData[] columnMetaData = vmd.getColumnMetaData();
    if (columnMetaData == null || columnMetaData.length == 0) {
      return idFactory.newVersionFieldIdentifier().getIdentifierName();
    }
    if (columnMetaData.length != 1) {
      throw new IllegalArgumentException(
          "Please specify 0 or 1 column name for the version property.");
    }
    return columnMetaData[0].getName();
  }

  public static String determineKind(AbstractClassMetaData acmd, ObjectManager om) {
    MappedStoreManager storeMgr = (MappedStoreManager) om.getStoreManager();
    return determineKind(acmd, storeMgr.getIdentifierFactory());
  }

  public static String determineKind(AbstractClassMetaData acmd, IdentifierFactory idFactory) {
    if (acmd.getTable() != null) {
      // User specified a table name as part of the mapping so use that as the
      // kind.
      return acmd.getTable();
    }
    // No table name provided so use the identifier factory to convert the
    // class name into the kind.
    return idFactory.newDatastoreContainerIdentifier(acmd).getIdentifierName();
  }

  /**
   * @see DatastoreJDOPersistenceManager#getObjectById(Class, Object) for
   * an expalnation of how this is useful.
   *
   * Supported translations:
   * When the pk field is a Long you can give us a Long or an encoded key
   * string.
   *
   * When the pk field is an unencoded String you can give us an unencoded
   * or an encoded String.
   *
   * When the pk field is an encoded String you can give us an unencoded
   * or an encoded String.
   */
  public static Object idOrNameToInternalKey(ObjectManager om, Class<?> cls, final Object val) {
    Object result = null;
    AbstractClassMetaData cmd = om.getMetaDataManager().getMetaDataForClass(
        cls, om.getClassLoaderResolver());
    DatastoreManager storeMgr = (DatastoreManager) om.getStoreManager();
    String kind = determineKind(cmd, storeMgr.getIdentifierFactory());
    AbstractMemberMetaData pkMemberMetaData = getPKMemberMetaData(om, cls);
    Class<?> pkType = pkMemberMetaData.getType();
    if (val instanceof String) {
      result = stringToInternalKey(kind, pkType, pkMemberMetaData, cls, val);
    } else if (val instanceof Long || val instanceof Integer) {
      result = intOrLongToInternalKey(kind, pkType, pkMemberMetaData, cls, val);
    } else if (val instanceof Key) {
      result = keyToInternalKey(kind, pkType, pkMemberMetaData, cls, val);
    }
    if (result == null && val != null) {
      // missed a case somewhere
      throw new NucleusUserException(
          "Received a request to find an object of type " + cls.getName() + " identified by "
          + val + ".  This is not a valid representation of a primary key for an instance of "
          + cls.getName() + ".");
    }
    return result;
  }

  private static Object keyToInternalKey(String kind, Class<?> pkType,
                                         AbstractMemberMetaData pkMemberMetaData, Class<?> cls,
                                         Object val) {
    Object result = null;
    Key key = (Key) val;
    if (!key.getKind().equals(kind)) {
      throw new NucleusUserException(
          "Received a request to find an object of kind " + kind + " but the provided "
          + "identifier is a Key for kind " + key.getKind());
    }
    if (!key.isComplete()) {
      throw new NucleusUserException(
          "Received a request to find an object of kind " + kind + " but the provided "
          + "identifier is is an incomplete Key");
    }
    if (pkType.equals(String.class)) {
      if (pkMemberMetaData.hasExtension("encoded-pk")) {
        result = KeyFactory.keyToString(key);
      } else {
        if (key.getParent() != null) {
          // By definition, classes with unencoded string pks
          // do not have parents.  Since this key has a parent
          // this isn't valid input.
          throw new NucleusUserException(
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
        throw new NucleusUserException(
            "Received a request to find an object of type " + cls.getName() + ".  The primary "
            + "key for this type is a Long, which means instances of this type "
            + "never have parents.  However, the Key that was provided as an argument has a "
            + "parent.");
      }
      result = key.getId();
    } else if (pkType.equals(Key.class)) {
      result = val;
    }
    return result;
  }

  private static Object intOrLongToInternalKey(String kind, Class<?> pkType,
                                       AbstractMemberMetaData pkMemberMetaData, Class<?> cls,
                                       Object val) {
    Object result = null;
    Key keyWithId = KeyFactory.createKey(kind, ((Number) val).longValue());
    if (pkType.equals(String.class)) {
      if (pkMemberMetaData.hasExtension("encoded-pk")) {
        result = KeyFactory.keyToString(keyWithId);
      } else {
        throw new NucleusUserException(
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
        throw new NucleusUserException(
            "Received a request to find an object of kind " + kind + " but the provided "
            + "identifier is the String representation of an incomplete Key for kind "
            + decodedKey.getKind());
      }
    } catch (IllegalArgumentException iae) {
      // this is ok, it just means we were only given the name
      decodedKey = KeyFactory.createKey(kind, (String) val);
    }
    if (!decodedKey.getKind().equals(kind)) {
      throw new NucleusUserException(
          "Received a request to find an object of kind " + kind + " but the provided "
          + "identifier is the String representation of a Key for kind "
          + decodedKey.getKind());
    }
    if (pkType.equals(String.class)) {
      if (pkMemberMetaData.hasExtension("encoded-pk")) {
        // Need to make sure we pass on an encoded pk
        result = KeyFactory.keyToString(decodedKey);
      } else {
        if (decodedKey.getParent() != null) {
          throw new NucleusUserException(
              "Received a request to find an object of type " + cls.getName() + ".  The primary "
              + "key for this type is an unencoded String, which means instances of this type "
              + "never have parents.  However, the encoded string representation of the Key that "
              + "was provided as an argument has a parent.");
        }
        // Pk is an unencoded string so need to pass on just the name
        // component.  However, we need to make sure the provided key actually
        // contains a name component.
        if (decodedKey.getName() == null) {
          throw new NucleusUserException(
              "Received a request to find an object of type " + cls.getName() + ".  The primary "
              + "key for this type is an unencoded String.  However, the encoded string "
              + "representation of the Key that was provided as an argument has its id field "
              + "set, not its name.  This makes it an invalid key for this class.");
        }
        result = decodedKey.getName();
      }
    } else if (pkType.equals(Long.class)) {
      if (decodedKey.getParent() != null) {
        throw new NucleusUserException(
            "Received a request to find an object of type " + cls.getName() + ".  The primary "
            + "key for this type is a Long, which means instances of this type "
            + "never have parents.  However, the encoded string representation of the Key that "
            + "was provided as an argument has a parent.");
      }
      // pk is a long so just pass on the id component
      result = decodedKey.getId();
    } else if (pkType.equals(Key.class)) {
      result = decodedKey;
    }
    return result;
  }

  private static AbstractMemberMetaData getPKMemberMetaData(ObjectManager om, Class<?> cls) {
    AbstractClassMetaData cmd = om.getMetaDataManager().getMetaDataForClass(
            cls, om.getClassLoaderResolver());
    return cmd.getMetaDataForManagedMemberAtAbsolutePosition(cmd.getPKMemberPositions()[0]);
  }
}
