package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.state.StateManagerFactory;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.fieldmanager.SingleValueFieldManager;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.EmbeddedPCMapping;
import org.datanucleus.store.mapped.mapping.InterfaceMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.PersistenceCapableMapping;
import org.datanucleus.store.mapped.mapping.SerialisedPCMapping;
import org.datanucleus.store.mapped.mapping.SerialisedReferenceMapping;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.spi.JDOImplHelper;

/**
 * FieldManager for converting app engine datastore entities into POJOs and
 * vice-versa.
 *
 * Most of the complexity in this class is due to the fact that the datastore
 * automatically promotes certain types:
 * It promotes short/Short, int/Integer, and byte/Byte to long.
 * It also promotes float/Float to double.
 *
 * Also, the datastore does not support char/Character.  We've made the decision
 * to promote this to long as well.
 *
 * We let the datastore handle the conversion when mapping pojos to datastore
 * {@link Entity Entities} but we handle the conversion ourselves when mapping
 * datastore {@link Entity Entities} to pojos.  For symmetry's sake we could
 * do all the pojo to datastore conversions in our code, but then the
 * conversions would be in two places (our code and the datastore service).
 * We'd rather have a bit of asymmetry and only have the logic exist in one
 * place.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreFieldManager implements FieldManager {

  // Needed for relation management in datanucleus.
  private static final int[] NOT_USED = {0};

  private static final String ILLEGAL_NULL_ASSIGNMENT_ERROR_FORMAT =
      "Datastore entity with kind %s and key %s has a null property named %s.  This property is "
          + "mapped to %s, which cannot accept null values.";

  // Thread local map is used to pass messages between parent and child
  // during cascades.  We're explicit about using an IdentityHashMap here
  // because we really really (really) want reference equality, not object
  // equality.  Field is not private to facilitate testing.
  static final ThreadLocal<IdentityHashMap<Object, Key>> PARENT_KEY_MAP =
      new ThreadLocal<IdentityHashMap<Object, Key>>() {
    @Override
    protected IdentityHashMap<Object, Key> initialValue() {
      return Maps.newIdentityHashMap();
    }
  };

  // Stack used to maintain the current field state manager to use.  We push on
  // to this stack as we encounter embedded classes and then pop when we're
  // done.
  private final Deque<FieldManagerState> fieldManagerStateStack =
      new ArrayDeque<FieldManagerState>();

  // true if we instantiated the entity ourselves.
  private final boolean createdWithoutEntity;

  private final MappedStoreManager storeManager;

  // Not final because we will reallocate if we hit an ancestor pk field
  // and the key of the current value does not have a parent, or if the pk
  // gets set.
  private Entity datastoreEntity;

  // Events that know how to store relations.
  private final List<StoreRelationEvent> storeRelationEvents = Lists.newArrayList();

  // We'll assign this if we have an ancestor member and we store a value
  // into it.
  private AbstractMemberMetaData ancestorMemberMetaData;

  private DatastoreFieldManager(StateManager sm, boolean createdWithoutEntity,
      MappedStoreManager storeManager, Entity datastoreEntity) {
    // We start with an ammdProvider that just gets member meta data from the class meta data.
    AbstractMemberMetaDataProvider ammdProvider = new AbstractMemberMetaDataProvider() {
      public AbstractMemberMetaData get(int fieldNumber) {
        return getClassMetaData().getMetaDataForManagedMemberAtPosition(fieldNumber);
      }
    };
    this.fieldManagerStateStack.push(new FieldManagerState(sm, ammdProvider));
    this.createdWithoutEntity = createdWithoutEntity;
    this.storeManager = storeManager;
    this.datastoreEntity = datastoreEntity;

    // Sanity check
    String expectedKind = EntityUtils.determineKind(getClassMetaData(), getIdentifierFactory());
    if (!expectedKind.equals(datastoreEntity.getKind())) {
      throw new NucleusException(
          "StateManager is for <" + expectedKind + "> but key is for <" + datastoreEntity.getKind()
              + ">.  This is almost certainly a bug in App Engine ORM.");
    }
  }

  /**
   * Creates a DatastoreFieldManager using the given StateManager and Entity.
   * Only use this overload when you have been provided with an Entity object
   * that already has a well-formed Key.  This will be the case when the entity
   * has been returned by the datastore (get or query), or after the entity has
   * been put into the datastore.
   */
  public DatastoreFieldManager(StateManager stateManager, MappedStoreManager storeManager,
      Entity datastoreEntity) {
    this(stateManager, false, storeManager, datastoreEntity);
  }

  public DatastoreFieldManager(StateManager stateManager, String kind,
      MappedStoreManager storeManager) {
    this(stateManager, true, storeManager, new Entity(kind));
  }

  public String fetchStringField(int fieldNumber) {
    // TODO(maxr): validate that pks are a valid type at time of enhancement.
    // TODO(maxr): validate that a class only has a single ancestor key.
    if (isPK(fieldNumber)) {
      // If this is pk field, transform the Key into its String representation.
      return KeyFactory.encodeKey(datastoreEntity.getKey());
    } else if (isAncestorPK(fieldNumber)) {
      Key parentKey = datastoreEntity.getKey().getParent();
      if (parentKey == null) {
        return null;
      }
      return KeyFactory.encodeKey(parentKey);
    }
    return (String) fetchObjectField(fieldNumber);
  }

  public short fetchShortField(int fieldNumber) {
    return (Short) fetchObjectField(fieldNumber);
  }

  private boolean fieldIsOfTypeKey(int fieldNumber) {
    // Key is final so we don't need to worry about checking for subclasses.
    return getMetaData(fieldNumber).getType().equals(Key.class);
  }

  private RuntimeException exceptionForUnexpectedKeyType(String fieldType, int fieldNumber) {
    return new IllegalStateException(
        fieldType + " for type " + getClassMetaData().getName()
            + " is of unexpected type " + getMetaData(fieldNumber).getType().getName()
            + " (must be String or " + Key.class.getName() + ")");
  }

  public Object fetchObjectField(int fieldNumber) {
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    if (ammd.getEmbeddedMetaData() != null) {
      return fetchEmbeddedField(ammd);
    } else if (ammd.getRelationType(getClassLoaderResolver()) != Relation.NONE) {
      return fetchRelationField(ammd);
    }

    Object value = datastoreEntity.getProperty(getPropertyName(fieldNumber));
    if (isPK(fieldNumber)) {
      if (fieldIsOfTypeKey(fieldNumber)) {
        // If this is a pk field, transform the Key into its String
        // representation.
        return datastoreEntity.getKey();
      }
      throw exceptionForUnexpectedKeyType("Primary key", fieldNumber);
    } else if (isAncestorPK(fieldNumber)) {
      if (fieldIsOfTypeKey(fieldNumber)) {
        return datastoreEntity.getKey().getParent();
      }
      throw exceptionForUnexpectedKeyType("Ancestor key", fieldNumber);
    } else {
      if (value != null) {
        // Datanucleus invokes this method for the object versions
        // of primitive types.  We need to make sure we convert
        // appropriately.
        value = TypeConversionUtils.datastoreValueToPojoValue(value, getMetaData(fieldNumber));
      }
      return value;
    }
  }

  private Object fetchRelationField(AbstractMemberMetaData ammd) {
    DatastoreClass dc = storeManager.getDatastoreClass(
        ammd.getAbstractClassMetaData().getFullClassName(), getClassLoaderResolver());
    JavaTypeMapping mapping = dc.getFieldMappingInDatastoreClass(ammd);
    // Based on ResultSetGetter
    Object value;
    if (mapping instanceof EmbeddedPCMapping ||
        mapping instanceof SerialisedPCMapping ||
        mapping instanceof SerialisedReferenceMapping) {
      value = mapping.getObject(
          getObjectManager(),
          datastoreEntity,
          NOT_USED,
          getStateManager(),
          ammd.getAbsoluteFieldNumber());
    } else {
      // Extract the related key from the entity
      String propName = EntityUtils.getPropertyName(getIdentifierFactory(), ammd);
      Key relatedKey = (Key) datastoreEntity.getProperty(propName);
      value = mapping.getObject(getObjectManager(), relatedKey, NOT_USED);
    }
    // Return the field value (as a wrapper if wrappable)
    return getStateManager().wrapSCOField(ammd.getAbsoluteFieldNumber(), value, false, false, false);
  }

  private AbstractMemberMetaDataProvider getEmbeddedAbstractMemberMetaDataProvider(
      AbstractMemberMetaData ammd) {
    final EmbeddedMetaData emd = ammd.getEmbeddedMetaData();
    // This implementation gets the meta data from the embedded meta data.
    // This is needed to ensure we see column overrides that are specific to
    // a specific embedded field.
    return new AbstractMemberMetaDataProvider() {
      public AbstractMemberMetaData get(int fieldNumber) {
        return emd.getMemberMetaData()[fieldNumber];
      }
    };
  }

  private StateManager getEmbeddedStateManager(AbstractMemberMetaData ammd, Object value) {
    if (value == null) {
      // Not positive this is the right approach, but when we read the values
      // of an embedded field out of the datastore we have no way of knowing
      // if the field should be null or it should contain an instance of the
      // embeddable whose members are all initialized to their default values
      // (the result of calling the default ctor).  Also, we can't risk
      // storing 'null' for every field of the embedded class because some of
      // the members might be base types and therefore non-nullable.  Writing
      // nulls to the datastore for these fields would cause NPEs when we read
      // the object back out.  Seems like the only safe thing to do here is
      // instantiate a fresh instance of the embeddable class using the default
      // constructor and then persist that.
      value = JDOImplHelper.getInstance().newInstance(
          ammd.getType(), (javax.jdo.spi.StateManager) getStateManager());
    }
    ObjectManager objMgr = getObjectManager();
    StateManager embeddedStateMgr = objMgr.findStateManager(value);
    if (embeddedStateMgr == null) {
      embeddedStateMgr = StateManagerFactory.newStateManagerForEmbedded(objMgr, value, false);
      embeddedStateMgr.addEmbeddedOwner(getStateManager(), ammd.getAbsoluteFieldNumber());
      embeddedStateMgr.setPcObjectType(StateManager.EMBEDDED_PC);
    }
    return embeddedStateMgr;
  }

  private Object fetchEmbeddedField(AbstractMemberMetaData ammd) {
    StateManager embeddedStateMgr = getEmbeddedStateManager(ammd, null);
    AbstractMemberMetaDataProvider ammdProvider = getEmbeddedAbstractMemberMetaDataProvider(ammd);
    fieldManagerStateStack.push(new FieldManagerState(embeddedStateMgr, ammdProvider));
    AbstractClassMetaData acmd = embeddedStateMgr.getClassMetaData();
    embeddedStateMgr.replaceFields(acmd.getAllMemberPositions(), this);
    fieldManagerStateStack.removeFirst();
    return embeddedStateMgr.getObject();
  }

  /**
   * Ensures that the given value is not null.  Throws
   * {@link NullPointerException} with a helpful error message if it is.
   */
  private Object checkAssignmentToNotNullField(Object val, int fieldNumber) {
    if (val != null) {
      // not null so no problem
      return val;
    }
    // Put together a really helpful error message
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    String propertyName = getPropertyName(fieldNumber);
    final String msg = String.format(ILLEGAL_NULL_ASSIGNMENT_ERROR_FORMAT,
        datastoreEntity.getKind(), KeyFactory.encodeKey(datastoreEntity.getKey()), propertyName,
        ammd.getFullFieldName());
    throw new NullPointerException(msg);
  }

  public long fetchLongField(int fieldNumber) {
    return (Long) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public int fetchIntField(int fieldNumber) {
    return (Integer) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public float fetchFloatField(int fieldNumber) {
    return (Float) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public double fetchDoubleField(int fieldNumber) {
    return (Double) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public char fetchCharField(int fieldNumber) {
    return (Character) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public byte fetchByteField(int fieldNumber) {
    return (Byte) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public boolean fetchBooleanField(int fieldNumber) {
    return (Boolean) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public void storeStringField(int fieldNumber, String value) {
    if (isPK(fieldNumber)) {
      storeStringPK(fieldNumber, value);
    } else if (isAncestorPK(fieldNumber)) {
      if (datastoreEntity.getParent() == null) {
        storeStringAncestorPK(value);
      }
    } else {
      storeObjectField(fieldNumber, value);
    }
  }

  /**
   * If the datastore entity doesn't have a parent and some other
   * pojo in the cascade chain registered itself as the parent for
   * this pojo, recreate the datastore entity with the parent pojo's
   * key as the ancestor.
   *
   * @return The key that was assigned, if any.
   */
  Object establishEntityGroup() {
    Key parentKey = PARENT_KEY_MAP.get().get(getStateManager().getObject());
    if (parentKey != null && datastoreEntity.getParent() == null) {
      recreateEntityWithAncestor(KeyFactory.encodeKey(parentKey));
      if (ancestorMemberMetaData != null) {
        return ancestorMemberMetaData.getType().equals(Key.class)
            ? parentKey : KeyFactory.encodeKey(parentKey);
      }
    }
    return null;
  }

  private void storeStringAncestorPK(String value) {
    if (value != null) {
      if (!createdWithoutEntity) {
        // Shouldn't even happen.
        throw new IllegalStateException("You can only rely on this class to properly handle "
            + "ancestor pks if you instantiated the class without providing a datastore "
            + "entity to the constructor.");
      }

      // If this field is labeled as an ancestor PK we need to recreate the Entity, passing
      // the value of this field as an arg to the Entity constructor and then moving all
      // properties on the old entity to the new entity.
      recreateEntityWithAncestor(value);
    } else {
      // Null ancestor.  Ancestor is defined on a per-instance basis so
      // annotating a field as an ancestor is not necessarily a commitment
      // to always having an ancestor.  Null ancestor is fine.
    }
  }

  private void recreateEntityWithAncestor(String value) {
    Entity old = datastoreEntity;
    if (old.getKey().getName() != null) {
      datastoreEntity =
          new Entity(old.getKind(), old.getKey().getName(), KeyFactory.decodeKey(value));
    } else {
      datastoreEntity = new Entity(old.getKind(), KeyFactory.decodeKey(value));
    }
    copyProperties(old, datastoreEntity);
  }

  private void storeStringPK(int fieldNumber, String value) {
    if (value != null) {
      // If the value of the PK has changed we assume that the user has
      // provided a named key.  Since named keys need to be provided when the
      // entity is created, we create a new entity and copy over any
      // properties that have already been set.

      // TODO(maxr): Find out if it is in violation of the spec
      // to throw an exception when someone updates the PK of a POJO.
      // We would prefer to throw an exception in this case.
      Entity old = datastoreEntity;
      if (old.getParent() != null) {
        datastoreEntity = new Entity(old.getKind(), value, old.getParent());
      } else {
        datastoreEntity = new Entity(old.getKind(), value);
      }
      copyProperties(old, datastoreEntity);
    } else if (getMetaData(fieldNumber).getColumn() != null) {
      // The pk doesn't get stored as a property so the fact that the user is
      // trying to customize the name of the property is concerning.  We
      // could log here, but that's going to annoy the heck out of people
      // who are porting existing code.  Instead we should....
      // TODO(maxr): Log a warning at startup or add a reasonably threadsafe way
      // to just log the warning once.
    }
  }

  private static void copyProperties(Entity src, Entity dest) {
    for (Map.Entry<String, Object> entry : src.getProperties().entrySet()) {
      dest.setProperty(entry.getKey(), entry.getValue());
    }
  }

  private boolean isAncestorPK(int fieldNumber) {
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    boolean result = ammd.hasExtension("ancestor-pk");
    if (result) {
      // ew, side effect
      ancestorMemberMetaData = ammd;
    }
    return result;
  }

  public void storeShortField(int fieldNumber, short value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeObjectField(int fieldNumber, Object value) {
    if (isPK(fieldNumber)) {
      if (fieldIsOfTypeKey(fieldNumber)) {
        storeKeyPK(fieldNumber, (Key) value);
      } else {
        throw exceptionForUnexpectedKeyType("Primary key", fieldNumber);
      }
    } else if (isAncestorPK(fieldNumber)) {
      if (datastoreEntity.getParent() == null) {
        if (fieldIsOfTypeKey(fieldNumber)) {
          storeAncestorKeyPK((Key) value);
        } else {
          throw exceptionForUnexpectedKeyType("Ancestor primary key", fieldNumber);
        }
      }
    } else {
      AbstractMemberMetaData ammd = getMetaData(fieldNumber);
      if (value != null ) {
        if (value.getClass().isArray()) {
          if (TypeConversionUtils.pojoPropertyIsByteArray(getMetaData(fieldNumber))) {
            value = TypeConversionUtils.convertByteArrayToBlob(value);
          } else {
            // Translate all arrays to lists before storing.
            value = TypeConversionUtils.convertPojoArrayToDatastoreList(value);
          }
        } else if (TypeConversionUtils.pojoPropertyIsCharacterCollection(ammd)) {
          // Datastore doesn't support Character so translate into
          // a list of Longs.  All other Collections can pass straight
          // through.
          value = Lists.transform((List<Character>) value, TypeConversionUtils.CHARACTER_TO_LONG);
        } else if (value instanceof Character) {
          // Datastore doesn't support Character so translate into a Long.
          value = TypeConversionUtils.CHARACTER_TO_LONG.apply((Character) value);
        }
      }
      ClassLoaderResolver clr = getClassLoaderResolver();
      if (ammd.getEmbeddedMetaData() != null) {
        storeEmbeddedField(ammd, value);
      } else if (ammd.getRelationType(clr) != Relation.NONE) {
        storeRelationField(ammd, value);
      } else {
        datastoreEntity.setProperty(getPropertyName(fieldNumber), value);
      }
    }
  }

  /**
   * Applies all the relation events that have been built up.
   * @return {@code true} if any of the events modified the parent
   * in such a way that the parent needs to be repersisted, {@link false}
   * otherwise.
   */
  boolean storeRelations() {
    if (storeRelationEvents.isEmpty()) {
      return false;
    }
    Map<Object, Key> parentKeyMap = PARENT_KEY_MAP.get();
    Map<Object, Key> original = null;
    try {
      if (datastoreEntity.getKey() != null) {
        original = makeKeyAvailableToRelatedObjects(parentKeyMap, original);
      }
      for (StoreRelationEvent event : storeRelationEvents) {
        event.apply();
      }
    } finally {
      if (original != null) {
        // This is an easy place to leak memory so make sure we restore
        // the map to its original state.  The map lives in a threadlocal
        // so we don't need to worry about concurrent access.
        parentKeyMap.clear();
        parentKeyMap.putAll(original);
      }
    }
    storeRelationEvents.clear();
    // TODO(maxr) Detect changes that modify the parent.
    return true;
  }

  /**
   * In order to automatically assign children to the same entity groups as
   * their parents, we need to make sure the Key of the parent entity is
   * available to the children when their corresponding entities are created.
   * We do this by looping over all the dependent fields of the parent, and for
   * each dependent field we extract the value of that field from the parent.
   * We then add that field value as the key in an indentity map, associating
   * it with the Key of the parent entity.  Anytime we insert a new object
   * we need to consult this map to see if a parent Key has been mapped to it,
   * and if it has we use that Key as the parent key for the new Entity we
   * are creating.
   *
   * TODO(maxr): Refactor relation-related code into a separate class - it's
   * starting to get crowded in here.
   */
  private Map<Object, Key> makeKeyAvailableToRelatedObjects(
      Map<Object, Key> parentKeyMap, Map<Object, Key> original) {
    DatastoreTable dt = (DatastoreTable) storeManager.getDatastoreClass(
        getClassMetaData().getFullClassName(), getClassLoaderResolver());
    SingleValueFieldManager sfv = new SingleValueFieldManager();
    for (AbstractMemberMetaData dependent : dt.getDependentMemberMetaData()) {
      getStateManager().provideFields(new int[]{dependent.getAbsoluteFieldNumber()}, sfv);
      Object dependentValue = sfv.fetchObjectField(dependent.getAbsoluteFieldNumber());
      if (dependentValue != null) {
        if (original == null) {
          // try to avoid making a copy of this map unless we really have to
          original = Maps.newHashMap(parentKeyMap);
        }
        PARENT_KEY_MAP.get().put(dependentValue, datastoreEntity.getKey());
      }
    }
    return original;
  }

  private void storeRelationField(final AbstractMemberMetaData ammd, final Object value) {
    StoreRelationEvent event = new StoreRelationEvent() {
      public void apply() {
        DatastoreClass dc = storeManager.getDatastoreClass(
            ammd.getAbstractClassMetaData().getFullClassName(), getClassLoaderResolver());
        // Based on ParameterSetter
        JavaTypeMapping mapping = dc.getFieldMappingInDatastoreClass(ammd);
        if (mapping instanceof EmbeddedPCMapping ||
            mapping instanceof SerialisedPCMapping ||
            mapping instanceof SerialisedReferenceMapping ||
            mapping instanceof PersistenceCapableMapping ||
            mapping instanceof InterfaceMapping) {
          mapping.setObject(
              getObjectManager(),
              datastoreEntity,
              NOT_USED,
              value,
              getStateManager(),
              ammd.getAbsoluteFieldNumber());
        } else {
          mapping.setObject(getObjectManager(), datastoreEntity, NOT_USED, value);
        }
        // Make sure the field is wrapped where appropriate
        getStateManager().wrapSCOField(ammd.getAbsoluteFieldNumber(), value, false, true, true);
      }
    };

    // If the related object can't exist without the parent it should be part
    // of the parent's entity group.  In order to be part of the parent's
    // entity group we need the parent's key.  In order to get the parent's key
    // we must save the parent before we save the child.  In order to avoid
    // saving the child until after we've saved the parent we register an event
    // that we will apply later.
    if (ammd.isDependent()) {
      storeRelationEvents.add(event);
    } else {
      // not dependent so just apply right away
      event.apply();
    }
  }

  ClassLoaderResolver getClassLoaderResolver() {
    return getObjectManager().getClassLoaderResolver();
  }

  private StateManager getStateManager() {
    return fieldManagerStateStack.peekFirst().stateManager;
  }

  private ObjectManager getObjectManager() {
    return getStateManager().getObjectManager();
  }

  private void storeEmbeddedField(AbstractMemberMetaData ammd, Object value) {
    StateManager embeddedStateMgr = getEmbeddedStateManager(ammd, value);
    AbstractMemberMetaDataProvider ammdProvider = getEmbeddedAbstractMemberMetaDataProvider(ammd);
    fieldManagerStateStack.push(new FieldManagerState(embeddedStateMgr, ammdProvider));
    AbstractClassMetaData acmd = embeddedStateMgr.getClassMetaData();
    embeddedStateMgr.provideFields(acmd.getAllMemberPositions(), this);
    fieldManagerStateStack.removeFirst();
  }

  private void storeKeyPK(int fieldNumber, Key key) {
    if (key != null) {
      storeStringPK(fieldNumber, key.getName());
    } else {
      storeStringPK(fieldNumber, null);
    }
  }

  private void storeAncestorKeyPK(Key key) {
    if (key != null) {
      storeStringAncestorPK(KeyFactory.encodeKey(key));
    } else {
      storeStringAncestorPK(null);
    }
  }

  public void storeLongField(int fieldNumber, long value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeIntField(int fieldNumber, int value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeFloatField(int fieldNumber, float value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeDoubleField(int fieldNumber, double value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeCharField(int fieldNumber, char value) {
    storeLongField(fieldNumber, (long) value);
  }

  public void storeByteField(int fieldNumber, byte value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeBooleanField(int fieldNumber, boolean value) {
    storeObjectField(fieldNumber, value);
  }

  private boolean isPK(int fieldNumber) {
    int[] pkPositions = getClassMetaData().getPKMemberPositions();
    // Assumes that if we have a pk we only have a single field pk
    return pkPositions != null && pkPositions[0] == fieldNumber;
  }

  private String getPropertyName(int fieldNumber) {
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    return EntityUtils.getPropertyName(getIdentifierFactory(), ammd);
  }

  private AbstractMemberMetaData getMetaData(int fieldNumber) {
    return fieldManagerStateStack.peekFirst().abstractMemberMetaDataProvider.get(fieldNumber);
  }

  AbstractClassMetaData getClassMetaData() {
    return getStateManager().getClassMetaData();
  }

  Entity getEntity() {
    return datastoreEntity;
  }

  IdentifierFactory getIdentifierFactory() {
    return ((MappedStoreManager) getObjectManager().getStoreManager()).getIdentifierFactory();
  }

  AbstractMemberMetaData getAncestorMemberMetaData() {
    return ancestorMemberMetaData;
  }

  /**
   * Translates field numbers into {@link AbstractMemberMetaData}.
   */
  private interface AbstractMemberMetaDataProvider {
    AbstractMemberMetaData get(int fieldNumber);
  }

  private static final class FieldManagerState {
    private final StateManager stateManager;
    private final AbstractMemberMetaDataProvider abstractMemberMetaDataProvider;

    private FieldManagerState(StateManager stateManager,
        AbstractMemberMetaDataProvider abstractMemberMetaDataProvider) {
      this.stateManager = stateManager;
      this.abstractMemberMetaDataProvider = abstractMemberMetaDataProvider;
    }
  }

  /**
   * Supports a mechanism for delaying the storage of a relation.
   */
  private interface StoreRelationEvent {
    void apply();
  }

}