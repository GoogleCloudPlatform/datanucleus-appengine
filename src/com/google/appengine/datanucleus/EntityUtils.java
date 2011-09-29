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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.datanucleus.mapping.DatastoreTable;
import com.google.appengine.datanucleus.query.QueryEntityPKFetchFieldManager;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.FetchPlan;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NoPersistenceInformationException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.VersionMetaData;

import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

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

    if (ammd.getColumnMetaData() != null && ammd.getColumnMetaData().length > 0 && 
        ammd.getColumnMetaData()[0].getName() != null) {
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
    ColumnMetaData columnMetaData = vmd.getColumnMetaData();
    if (columnMetaData == null) {
      return idFactory.newVersionFieldIdentifier().getIdentifierName();
    }
    return columnMetaData.getName();
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
    AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(cls, ec.getClassLoaderResolver());
    String kind = determineKind(cmd, ec);
    AbstractMemberMetaData pkMemberMetaData =
      cmd.getMetaDataForManagedMemberAtAbsolutePosition(cmd.getPKMemberPositions()[0]);
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
    } else if (val instanceof Long || val instanceof Integer || long.class.isInstance(val)) {
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

  public static Key getKeyForObject(Object pc, ExecutionContext ec) {
    AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(pc.getClass(), ec.getClassLoaderResolver());
    if (cmd.getIdentityType() == IdentityType.DATASTORE) {
      OID oid = (OID)ec.getApiAdapter().getIdForObject(pc);
      if (oid == null) {
        // Not yet persistent, so return null
        return null;
      }
      Object keyValue = oid.getKeyValue();
      return EntityUtils.getPkAsKey(keyValue, cmd, ec);
    } else {
      // TODO Cater for composite identity
      Object internalPk = ec.getApiAdapter().getTargetKeyForSingleFieldIdentity(ec.getApiAdapter().getIdForObject(pc));
      if (internalPk == null) {
        // Not yet persistent, so return null
        return null;
      }
      return EntityUtils.getPkAsKey(internalPk, 
          ec.getMetaDataManager().getMetaDataForClass(pc.getClass(), ec.getClassLoaderResolver()), ec);
    }
  }

  public static Key getPkAsKey(ObjectProvider op) {
    if (op.getClassMetaData().getIdentityType() == IdentityType.DATASTORE) {
      OID oid = (OID)op.getInternalObjectId();
      Object keyValue = oid.getKeyValue();
      return EntityUtils.getPkAsKey(keyValue, op.getClassMetaData(), op.getExecutionContext());
    } else {
      // TODO Support composite PK
      Object pk = op.getExecutionContext().getApiAdapter().getTargetKeyForSingleFieldIdentity(op.getInternalObjectId());
      if (pk == null) {
        throw new IllegalStateException("Primary key for object of type " + op.getClassMetaData().getName() + " is null.");
      }
      return EntityUtils.getPkAsKey(pk, op.getClassMetaData(), op.getExecutionContext());
    }
  }

  static Key getPkAsKey(Object pk, AbstractClassMetaData acmd, ExecutionContext ec) {
    if (pk == null) {
      throw new IllegalStateException(
          "Primary key for object of type " + acmd.getName() + " is null.");
    } else if (pk instanceof Key) {
      return (Key) pk;
    } else if (pk instanceof String) {
      if (MetaDataUtils.hasEncodedPKField(acmd)) {
        return KeyFactory.stringToKey((String) pk);
      } else {
        String kind = EntityUtils.determineKind(acmd, ec);
        return KeyFactory.createKey(kind, (String) pk);
      }
    } else if (pk instanceof Long || long.class.isInstance(pk)) {
      String kind = EntityUtils.determineKind(acmd, ec);
      return KeyFactory.createKey(kind, (Long) pk);
    } else {
      throw new IllegalStateException(
          "Primary key for object of type " + acmd.getName()
              + " is of unexpected type " + pk.getClass().getName()
              + " (must be String, Long, long, or " + Key.class.getName() + ")");
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
  // and refactoring the error messages so that they aren't specific to object lookups.
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
    } else if (pkType.equals(Long.class) || pkType.equals(long.class)) {
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
    } else if (pkType.equals(Long.class) || pkType.equals(long.class)) {
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
      if (pkType.equals(Long.class) || pkType.equals(long.class)) {
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
    } else if (pkType.equals(Long.class) || pkType.equals(long.class)) {
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

  public static Key getPrimaryKeyAsKey(Object pk, ExecutionContext ec, AbstractClassMetaData cmd) {
    String kind = EntityUtils.determineKind(cmd, ec);
    if (pk instanceof Key) {
      return (Key) pk;
    } else if (long.class.isInstance(pk) || pk instanceof Long) {
      return KeyFactory.createKey(kind, (Long) pk);
    }
    try {
      return KeyFactory.stringToKey((String) pk);
    } catch (IllegalArgumentException iae) {
      return KeyFactory.createKey(kind, (String) pk);
    }
  }

  public static Key getPrimaryKeyAsKey(ApiAdapter apiAdapter, ObjectProvider op) {
    Object primaryKey = apiAdapter.getTargetKeyForSingleFieldIdentity(op.getInternalObjectId());
    return getPrimaryKeyAsKey(primaryKey, op.getExecutionContext(), op.getClassMetaData());
  }

  private static final Field PROPERTY_MAP_FIELD;
  static {
    try {
      PROPERTY_MAP_FIELD = Entity.class.getDeclaredField("propertyMap");
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    PROPERTY_MAP_FIELD.setAccessible(true);
  }

  // TODO(maxr) Get rid of this once we have a formal way of figuring out which properties are indexed and which are unindexed.
  private static Map<String, Object> getPropertyMap(Entity entity) {
    try {
      return (Map<String, Object>) PROPERTY_MAP_FIELD.get(entity);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  // TODO(maxr) Rewrite once Entity.setPropertiesFrom() is available.
  static public void copyProperties(Entity src, Entity dest) {
    for (Map.Entry<String, Object> entry : getPropertyMap(src).entrySet()) {
      // barf
      if (entry.getValue() != null &&
          entry.getValue().getClass().getName().equals("com.google.appengine.api.datastore.Entity$UnindexedValue")) {
        dest.setUnindexedProperty(entry.getKey(), src.getProperty(entry.getKey()));
      } else {
        dest.setProperty(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Method to check whether the child is having its parent switched from the specified parent.
   * @param child The child object
   * @param parentOP ObjectProvider for the parent
   * @throws ChildWithoutParentException if no parent defined
   * @throws ChildWithWrongParentException if parent is wrong
   */
  public static void checkParentage(Object child, ObjectProvider parentOP) {
    if (child == null) {
      return;
    }

    ExecutionContext ec = parentOP.getExecutionContext();
    ApiAdapter apiAdapter = ec.getApiAdapter();

    ObjectProvider childOP = ec.findObjectProvider(child);
    if (apiAdapter.isNew(child) &&
        (childOP == null || 
         childOP.getAssociatedValue(((DatastoreManager)ec.getStoreManager()).getDatastoreTransaction(ec)) == null)) {
      // This condition is difficult to get right.  An object that has been persisted
      // (and therefore had its primary key already established) may still be considered
      // NEW by the apiAdapter if there is a txn and the txn has not yet committed.
      // In order to determine if an object has been persisted we see if there is
      // a state manager for it.  If there isn't, there's no way it was persisted.
      // If there is, it's still possible that it hasn't been persisted so we check
      // to see if there is an associated Entity. 
      // TODO Just call sm.isFlushedNew(). It's not that hard
      return;
    }
    // Since we only support owned relationships right now, we can assume
    // that this is parent/child and verify that the parent of the childSM
    // is the parent object in this cascade.
    // We know that the child primary key is a Key or an encoded String
    // because we don't support child objects with primary keys of type
    // Long or unencoded String and our metadata validation would have
    // caught it.
    Object childKeyOrString =
      apiAdapter.getTargetKeyForSingleFieldIdentity(apiAdapter.getIdForObject(child));
    if (childKeyOrString == null) {
      // must be a new object or transient
      return;
    }
    Key childKey = childKeyOrString instanceof Key
    ? (Key) childKeyOrString : KeyFactory.stringToKey((String) childKeyOrString);

    Key parentKey = EntityUtils.getPrimaryKeyAsKey(apiAdapter, parentOP);

    if (childKey.getParent() == null) {
      throw new ChildWithoutParentException(parentKey, childKey);
    } else if (!parentKey.equals(childKey.getParent())) {
      throw new ChildWithWrongParentException(parentKey, childKey);
    }
  }

  static class ChildWithoutParentException extends NucleusFatalUserException {
    public ChildWithoutParentException(Key parentKey, Key childKey) {
      super("Detected attempt to establish " + parentKey + " as the "
          + "parent of " + childKey + " but the entity identified by "
          + childKey + " has already been persisted without a parent.  A parent cannot "
          + "be established or changed once an object has been persisted.");
    }
  }

  static class ChildWithWrongParentException extends NucleusFatalUserException {
    public ChildWithWrongParentException(Key parentKey, Key childKey) {
      super("Detected attempt to establish " + parentKey + " as the "
          + "parent of " + childKey + " but the entity identified by "
          + childKey + " is already a child of " + childKey.getParent() + ".  A parent cannot "
          + "be established or changed once an object has been persisted.");
    }
  }

  /**
   * Method to retrieve the Entity with the specified key from the datastore.
   * @param ds DatastoreService to use
   * @param op ObjectProvider that we want to associate this Entity with (if any)
   * @param key The key
   * @return The Entity
   */
  public static Entity getEntityFromDatastore(DatastoreService ds, ObjectProvider op, Key key) {
    DatastoreTransaction txn = 
      ((DatastoreManager)op.getExecutionContext().getStoreManager()).getDatastoreTransaction(op.getExecutionContext());

    if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
      NucleusLogger.DATASTORE_NATIVE.debug("Getting entity of kind " + key.getKind() + " with key " + key);
    }

    Entity entity;
    try {
      if (txn == null) {
        entity = ds.get(key);
      } else {
        entity = ds.get(txn.getInnerTxn(), key);
      }
    } catch (EntityNotFoundException e) {
      throw DatastoreExceptionTranslator.wrapEntityNotFoundException(e, key);
    }

    if (op != null) {
      op.setAssociatedValue(txn, entity);
    }

    return entity;
  }

  /**
   * Method to put the provided entity into the datastore.
   * @param ec ExecutionContext
   * @param entity The entity
   * @return The DatastoreTransaction
   */
  public static DatastoreTransaction putEntityIntoDatastore(ExecutionContext ec, Entity entity) {
    return putEntitiesIntoDatastore(ec, Collections.singletonList(entity));
  }

  /**
   * Method to put the provided entities into the datastore.
   * @param ec ExecutionContext
   * @param entities The entities
   * @return The DatastoreTransaction
   */
  public static DatastoreTransaction putEntitiesIntoDatastore(ExecutionContext ec, List<Entity> entities) {
    DatastoreTransaction txn = ((DatastoreManager)ec.getStoreManager()).getDatastoreTransaction(ec);
    DatastoreService ds = ((DatastoreManager)ec.getStoreManager()).getDatastoreService(ec);
    List<Entity> putMe = Utils.newArrayList();
    for (Entity entity : entities) {
      if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
        StringBuffer str = new StringBuffer();
        for (Map.Entry<String, Object> entry : entity.getProperties().entrySet()) {
          str.append(entry.getKey() + "[" + entry.getValue() + "]");
          str.append(", ");
        }
        NucleusLogger.DATASTORE_NATIVE.debug("Putting entity of kind " + entity.getKind() + 
            " with key " + entity.getKey() + " as {" + str.toString() + "}");
      }
      if (txn == null) {
        putMe.add(entity);
      } else {
        if (txn.getDeletedKeys().contains(entity.getKey())) {
          // entity was already deleted - just skip it
          // I'm a bit worried about swallowing user errors but we'll
          // see what bubbles up when we launch.  In theory we could
          // keep the entity that the user was deleting along with the key
          // and check to see if they changed anything between the
          // delete and the put.
        } else {
          Entity previouslyPut = txn.getPutEntities().get(entity.getKey());
          // It's ok to put if we haven't put this entity before or we have
          // and something has changed.  The reason we want to reput if something has
          // changed is that this will generate a datastore error, and we want users
          // to get this error because it means they have done something wrong.

          // TODO(maxr) Throw this exception ourselves with lots of good error detail.
          if (previouslyPut == null || !previouslyPut.getProperties().equals(entity.getProperties())) {
            putMe.add(entity);
          }
        }
      }
    }
    if (!putMe.isEmpty()) {
      if (txn == null) {
        if (putMe.size() == 1) {
          ds.put(putMe.get(0));
        } else {
          ds.put(putMe);
        }
      } else {
        Transaction innerTxn = txn.getInnerTxn();
        if (putMe.size() == 1) {
          ds.put(innerTxn, putMe.get(0));
        } else {
          ds.put(innerTxn, putMe);
        }
        txn.addPutEntities(putMe);
      }
    }
    return txn;
  }

  /**
   * Method to actually perform the deletion of Entity(s) from the datastore.
   * @param ec ExecutionContext
   * @param keys Keys to delete
   */
  public static void deleteEntitiesFromDatastore(ExecutionContext ec, List<Key> keys) {
    DatastoreService ds = ((DatastoreManager)ec.getStoreManager()).getDatastoreService(ec);

    if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
      NucleusLogger.DATASTORE_NATIVE.debug("Deleting entities with keys " + StringUtils.collectionToString(keys));
    }

    DatastoreTransaction txn = ((DatastoreManager)ec.getStoreManager()).getDatastoreTransaction(ec);
    if (txn == null) {
      if (keys.size() == 1) {
        ds.delete(keys.get(0));
      } else {
        ds.delete(keys);
      }
    } else {
      Transaction innerTxn = txn.getInnerTxn();
      if (keys.size() == 1) {
        ds.delete(innerTxn, keys.get(0));
      } else {
        ds.delete(innerTxn, keys);
      }
    }
  }

  /**
   * Convenience method to return an Entity with the same properties as the input Entity but with the
   * specified parent.
   * @param parentKey Key for the parent
   * @param originalEntity The original Entity
   * @return The new Entity
   */
  public static Entity recreateEntityWithParent(Key parentKey, Entity originalEntity) {
    Entity entity = null;
    if (originalEntity.getKey().getName() != null) {
      entity = new Entity(originalEntity.getKind(), originalEntity.getKey().getName(), parentKey);
    } else {
      entity = new Entity(originalEntity.getKind(), parentKey);
    }
    EntityUtils.copyProperties(originalEntity, entity);
    return entity;
  }

  /**
   * Convenience method to find the parent key for the supplied entity and its ObjectProvider about to be
   * persisted. If a datastore entity doesn't have a parent there are 3 places we can look for one.
   * 1) It's possible that a pojo in the cascade chain registered itself as the parent.
   * 2) It's possible that the pojo has an external foreign key mapping to the object that owns it, 
   *    in which case we can use the key of that field as the parent.
   * 3) If part of the attachment process we can consult the ExecutionContext for the owner of this object 
   *    (that caused the attach of this object).
   * @return The parent key
   */
  public static Key getParentKey(Entity entity, ObjectProvider op) {
    Key parentKey = entity.getParent();
    if (parentKey != null) {
      return parentKey;
    }

    ExecutionContext ec = op.getExecutionContext();

    // Mechanism 1, from the registry
    parentKey = KeyRegistry.getKeyRegistry(ec).getParentKeyForOwnedObject(op.getObject());

    if (parentKey == null) {
      // Mechanism 2, from the parent end of a bidir relation where this is a child
      DatastoreTable table =
        ((DatastoreManager)ec.getStoreManager()).getDatastoreClass(op.getClassMetaData().getFullClassName(),
          ec.getClassLoaderResolver());
      AbstractMemberMetaData parentField = table.getParentMappingMemberMetaData();
      if (parentField != null) {
        Object parent = op.provideField(parentField.getAbsoluteFieldNumber());
        parentKey = parent == null ? null : EntityUtils.getKeyForObject(parent, ec);
      }
    }

    if (parentKey == null) {
      // Mechanism 3, use attach parent info from ExecutionContext
      ObjectProvider ownerOP = op.getExecutionContext().getObjectProviderOfOwnerForAttachingObject(op.getObject());
      if (ownerOP != null) {
        Object parentPojo = ownerOP.getObject();
        ObjectProvider mergeOP = ec.findObjectProvider(parentPojo);
        if (mergeOP != null) {
          parentKey = EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), mergeOP);
        }
      }
    }

    return parentKey;
  }

  /**
   * Method to return the key of the provided child object.
   * @param childObj The persistable object from which to extract a key.
   * @param ec ExecutionContext
   * @param parentEntity The Entity of the parent (if owned).
   * @return The key of the object. Returns {@code null} if the object is being deleted 
   *    or the object does not yet have a key.
   */
  public static Key extractChildKey(Object childObj, ExecutionContext ec, Entity parentEntity) {
    if (childObj == null) {
      return null;
    }

    if (ec.getApiAdapter().isDetached(childObj)) {
      // Child is detached, so return its id
      Object valueID = ec.getApiAdapter().getIdForObject(childObj);
      Object valuePK = ec.getApiAdapter().getTargetKeyForSingleFieldIdentity(valueID);
      Key key = EntityUtils.getPrimaryKeyAsKey(valuePK, ec,
          ec.getMetaDataManager().getMetaDataForClass(childObj.getClass(), ec.getClassLoaderResolver()));
      return key;
    } else if (ec.getApiAdapter().isDeleted(childObj)) {
      // Child is deleted, so return null
      return null;
    }

    ObjectProvider childOP = ec.findObjectProvider(childObj);
    if (childOP == null) {
      // Not yet persistent
      return null;
    }

    Key key = null;
    if (childOP.getClassMetaData().getIdentityType() == IdentityType.DATASTORE) {
      OID oid = (OID)childOP.getInternalObjectId();
      if (oid == null) {
        // Not yet persistent, so return null
        return null;
      }
      Object keyValue = oid.getKeyValue();
      key = EntityUtils.getPkAsKey(keyValue, childOP.getClassMetaData(), ec);
    } else {
      // TODO Cater for composite identity
      Object primaryKey = ec.getApiAdapter().getTargetKeyForSingleFieldIdentity(childOP.getInternalObjectId());
      if (primaryKey == null) {
        // Not yet persistent, so return null
        return null;
      }

      key = EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), childOP);
    }

    if (key == null) {
      throw new NullPointerException("Could not extract a key from " + childObj);
    }

    if (parentEntity != null) {
      // We only support owned relationships so this key should be a child of the entity we are persisting.
      if (key.getParent() == null) {
        // TODO This is caused by persisting from non-owner side of a relation when owned and GAE having a very basic
        // restriction of not allowing persistence of such things due to its silly parent-key rationale.
        throw new ChildWithoutParentException(parentEntity.getKey(), key);
      } else if (!key.getParent().equals(parentEntity.getKey())) {
        throw new ChildWithWrongParentException(parentEntity.getKey(), key);
      }
    }
    return key;
  }

  /**
   * Converts the provided Entity to a pojo.
   * @param entity The entity to convert
   * @param acmd The meta data for the pojo class
   * @param clr The classloader resolver
   * @param om The object manager
   * @param ignoreCache Whether or not the cache should be ignored when the PM/EM attempts to find the pojo
   * @param fetchPlan the fetch plan to use
   * @return The pojo that corresponds to the provided entity.
   */
  public static Object entityToPojo(final Entity entity, final AbstractClassMetaData acmd,
      final ClassLoaderResolver clr, ExecutionContext ec, boolean ignoreCache, final FetchPlan fetchPlan) {
    final DatastoreManager storeMgr = (DatastoreManager) ec.getStoreManager();
    DatastoreTable table = storeMgr.getDatastoreClass(acmd.getFullClassName(), ec.getClassLoaderResolver());
    storeMgr.validateMetaDataForClass(acmd);

    FieldValues fv = null;
    if (fetchPlan != null) {
      // candidate select : load all fetch plan fields from the Entity

      // Make sure this class is managed in the FetchPlan
      fetchPlan.manageFetchPlanForClass(acmd);

      final int[] fieldsToFetch = fetchPlan.getFetchPlanForClass(acmd).getMemberNumbers();
      fv = new FieldValues() {
        public void fetchFields(ObjectProvider op) {
          op.replaceFields(fieldsToFetch, new FetchFieldManager(op, entity));
        }
        public void fetchNonLoadedFields(ObjectProvider op) {
          op.replaceNonLoadedFields(fieldsToFetch, new FetchFieldManager(op, entity));
        }
        public FetchPlan getFetchPlanForLoading() {
          return fetchPlan;
        }
      };
    } else {
      // projection select : load PK fields only here, and all later
      fv = new FieldValues() {
        public void fetchFields(ObjectProvider op) {
          op.replaceFields(acmd.getPKMemberPositions(), new FetchFieldManager(op, entity));
        }
        public void fetchNonLoadedFields(ObjectProvider op) {
          op.replaceNonLoadedFields(acmd.getPKMemberPositions(), new FetchFieldManager(op, entity));
        }
        public FetchPlan getFetchPlanForLoading() {
          return null;
        }
      };
    }

    Object id = null;
    Class cls = getClassFromDiscriminator(entity, acmd, table, clr, ec);
    if (acmd.getIdentityType() == IdentityType.DATASTORE) {
      // TODO Implement support for datastore id
      throw new NucleusException("Dont currently support use of datastore-identity");
    } else {
      FieldManager fm = new QueryEntityPKFetchFieldManager(acmd, entity);
      id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, acmd, cls, true, fm);
    }

    Object pojo = ec.findObject(id, fv, cls, ignoreCache);
    ObjectProvider op = ec.findObjectProvider(pojo);

    // TODO(maxr): Seems like we should be able to refactor the handler
    // so that we can do a fetch without having to hide the entity in the state manager.
    op.setAssociatedValue(((DatastoreManager)ec.getStoreManager()).getDatastoreTransaction(ec), entity);

    if (fetchPlan == null) {
      // Projection, so load everything
      // TODO Remove this. It prevents postLoad calls being made, but do we care since its a projection?
      storeMgr.getPersistenceHandler().fetchObject(op, acmd.getAllMemberPositions());
    }

    return pojo;
  }

  /**
   * Method to return the class that this Entity is an instance of. Uses a discriminator property in the
   * Entity if present, otherwise returns the candidate type
   * @param entity The Entity to get values from
   * @param acmd Metadata for the candidate
   * @param table Table we're retrieving information from
   * @param clr ClassLoader resolver
   * @param ec ExecutionContext
   * @return The class of this Entity
   */
  private static Class<?> getClassFromDiscriminator(Entity entity, AbstractClassMetaData acmd,
      DatastoreTable table, ClassLoaderResolver clr, ExecutionContext ec) {
    if (!acmd.hasDiscriminatorStrategy()) {
      return clr.classForName(acmd.getFullClassName());
    }

    String discrimPropertyName = EntityUtils.getDiscriminatorPropertyName(
        table.getStoreManager().getIdentifierFactory(), acmd.getDiscriminatorMetaDataRoot());
    Object discrimValue = entity.getProperty(discrimPropertyName);

    if (discrimValue == null) {
      throw new NucleusUserException("Discriminator of this entity is null: " + entity);
    }
    String rowClassName = null;

    if (acmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME) {
      rowClassName = (String) discrimValue;
    } else if (acmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP) {
      // Check the main class type for the table
      Object discrimMDValue = acmd.getDiscriminatorValue();
      if (discrimMDValue.equals(discrimValue)) {
        rowClassName = acmd.getFullClassName();
      } else {
        // Go through all possible subclasses to find one with this value
        for (Object o : ec.getStoreManager().getSubClassesForClass(acmd.getFullClassName(), true, clr)) {
          String className = (String) o;
          AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(className, clr);
          discrimMDValue = cmd.getDiscriminatorValue();
          if (discrimValue.equals(discrimMDValue)) {
            rowClassName = className;
            break;
          }
        }
      }
    }

    if (rowClassName == null) {
      throw new NucleusUserException("Cannot get the class for entity " + entity + "\n" +
          "This can happen if the meta data for the subclasses of " + acmd.getFullClassName() +
          " is not yet loaded! You may want to consider using the datanucleus autostart mechanism" +
          " to tell datanucleus about these classes.");
    }

    return clr.classForName(rowClassName);
  }
}