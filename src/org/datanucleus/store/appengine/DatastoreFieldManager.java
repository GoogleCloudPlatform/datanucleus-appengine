package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.common.collect.Lists;

import org.datanucleus.StateManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;

import java.util.List;
import java.util.Map;

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
 * conversions would be in two places (our code and the datastore service),
 * but we'd rather have a bit of asymmetry and only have the logic exist in one
 * place.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreFieldManager implements FieldManager {

  private final StateManager sm;
  private final boolean createdWithoutEntity;
  // Not final because we will reallocate if we hit an ancestor pk field
  // and the key of the current value does not have a parent, or if the pk
  // gets set.
  private Entity datastoreEntity;

  private DatastoreFieldManager(StateManager sm, boolean createdWithoutEntity, Entity datastoreEntity) {
    this.sm = sm;
    this.createdWithoutEntity = createdWithoutEntity;
    this.datastoreEntity = datastoreEntity;
  }

  /**
   * Creates a DatastoreFieldManager using the given StateManager and Entity.
   * Only use this overload when you have been provided with an Entity object
   * that already has a well-formed Key.  This will be the case when the entity
   * has been returned by the datastore (get or query), or after the entity has
   * been put into the datastore.
   */
  public DatastoreFieldManager(StateManager sm, Entity datastoreEntity) {
    this(sm, false, datastoreEntity);
  }

  public DatastoreFieldManager(StateManager sm, String kind) {
    this(sm, true, new Entity(kind));
  }

  public String fetchStringField(int fieldNumber) {
    // We assume pks are of type String.
    // TODO(maxr): validate that pks are of type String at time of enhancement.
    if (isPK(fieldNumber)) {
      // If this is pk field, transform the Key into its String representation.
      return KeyFactory.encodeKey(datastoreEntity.getKey());
    } else if (isAncestorPK(fieldNumber)) {
      return KeyFactory.encodeKey(datastoreEntity.getKey().getParent());
    }
    return (String) fetchObjectField(fieldNumber);
  }

  public short fetchShortField(int fieldNumber) {
    return (Short) fetchObjectField(fieldNumber);
  }

  public Object fetchObjectField(int fieldNumber) {
    Object value = datastoreEntity.getProperty(getFieldName(fieldNumber));
    if (value != null) {
      // Datanucleus invokes this method for the object versions
      // of primitive types.  We need to make sure we convert
      // appropriately.
      value = TypeConversionUtils.datastoreValueToPojoValue(value, getMetaData(fieldNumber));
    }
    return value;
  }

  public long fetchLongField(int fieldNumber) {
    return (Long) fetchObjectField(fieldNumber);
  }

  public int fetchIntField(int fieldNumber) {
    return (Integer) fetchObjectField(fieldNumber);
  }

  public float fetchFloatField(int fieldNumber) {
    return (Float) fetchObjectField(fieldNumber);
  }

  public double fetchDoubleField(int fieldNumber) {
    return (Double) fetchObjectField(fieldNumber);
  }

  public char fetchCharField(int fieldNumber) {
    return (Character) fetchObjectField(fieldNumber);
  }

  public byte fetchByteField(int fieldNumber) {
    return (Byte) fetchObjectField(fieldNumber);
  }

  public boolean fetchBooleanField(int fieldNumber) {
    return (Boolean) fetchObjectField(fieldNumber);
  }

  public void storeStringField(int fieldNumber, String value) {
    // We assume pks are of type String.
    if (isPK(fieldNumber)) {
      if (value != null) {
        // If the value of the PK has changed we assume that the user has
        // provided a named key.  Since named keys need to provided when the
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
    } else if (isAncestorPK(fieldNumber) && datastoreEntity.getParent() == null) {
      if (value == null) {
        throw new IllegalArgumentException("Cannot have a null ancestor PK.");
      }

      if (!createdWithoutEntity) {
        // Shouldn't even happen.
        throw new IllegalStateException("You can only rely on this class to properly handle "
            + "ancestor pks if you instantiated the class without providing a datastore "
            + "entity to the constructor.");
      }

      // If this field is labeled as an ancestor PK we need to recreate the Entity, passing
      // the value of this field as an arg to the Entity constructor and then moving all
      // properties on the old entity to the new entity.
      Entity old = datastoreEntity;
      if (old.getKey().getName() != null) {
        datastoreEntity =
            new Entity(old.getKind(), old.getKey().getName(), KeyFactory.decodeKey(value));
      } else {
        datastoreEntity = new Entity(old.getKind(), KeyFactory.decodeKey(value));
      }
      copyProperties(old, datastoreEntity);
    } else {
      storeObjectField(fieldNumber, value);
    }
  }

  private static void copyProperties(Entity src, Entity dest) {
    for (Map.Entry<String, Object> entry : src.getProperties().entrySet()) {
      dest.setProperty(entry.getKey(), entry.getValue());
    }
  }

  private boolean isAncestorPK(int fieldNumber) {
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    return ammd.hasExtension("ancestor-pk");
  }

  public void storeShortField(int fieldNumber, short value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeObjectField(int fieldNumber, Object value) {
    if (value != null ) {
      if (value.getClass().isArray()) {
        // TODO(maxr): Convert byte[] and Byte[] to BLOB
        // Translate all arrays to lists before storing.
        value = TypeConversionUtils.convertPojoArrayToDatastoreList(value);
      } else if (TypeConversionUtils.pojoPropertyIsCharacterCollection(getMetaData(fieldNumber))) {
        // Datastore doesn't support Character so translate into
        // a list of Longs.  All other Collections can pass straight
        // through.
        value = Lists.transform((List<Character>) value, TypeConversionUtils.CHARACTER_TO_LONG);
      } else if (value instanceof Character) {
        // Datastore doesn't support Character so translate into a Long.
        value = TypeConversionUtils.CHARACTER_TO_LONG.apply((Character) value);
      }
    }
    datastoreEntity.setProperty(getFieldName(fieldNumber), value);
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
    // Assumes we only have a single field pk
    return getClassMetaData().getPKMemberPositions()[0] == fieldNumber;
  }

  private String getFieldName(int fieldNumber) {
    AbstractClassMetaData acmd = getClassMetaData();
    AbstractMemberMetaData ammd = acmd.getMetaDataForMemberAtRelativePosition(fieldNumber);
    // If a column name was explicitly provided, use that as the property name.
    if (ammd.getColumn() != null) {
      return ammd.getColumn();
    }
    // Use the IdentifierFactory to convert from the name of the field into
    // a property name.  Be careful, if the field is a version field
    // we need to invoke a different method on the id factory.
    IdentifierFactory idFactory = getIdentifierFactory();
    // TODO(maxr): See if there is a better way than field name comparison to
    // determine if this is a version field
    if (acmd.hasVersionStrategy() &&
        ammd.getName().equals(acmd.getVersionMetaData().getFieldName())) {
      return idFactory.newVersionFieldIdentifier().getIdentifier();
    }
    return idFactory.newDatastoreFieldIdentifier(ammd.getName()).getIdentifier();
  }

  private AbstractMemberMetaData getMetaData(int fieldNumber) {
    return getClassMetaData().getMetaDataForManagedMemberAtPosition(fieldNumber);
  }

  AbstractClassMetaData getClassMetaData() {
    return sm.getClassMetaData();
  }

  Entity getEntity() {
    return datastoreEntity;
  }

  IdentifierFactory getIdentifierFactory() {
    return ((MappedStoreManager)sm.getObjectManager().getStoreManager()).getIdentifierFactory();
  }
}