package org.datanucleus.store.appengine;

import org.datanucleus.StateManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Key;

/**
 * FieldManager for converting app engine datastore entities into POJOs and
 * vice-versa.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreFieldManager implements FieldManager {

  private final StateManager sm;
  private final Entity datastoreEntity;

  public DatastoreFieldManager(StateManager sm, Entity datastoreEntity) {
    this.sm = sm;
    this.datastoreEntity = datastoreEntity;
  }

  public String fetchStringField(int fieldNumber) {
    // We assume pks are of type String.
    // TODO(maxr): validate that pks are of type String at time of enhancement.
    if (isPK(fieldNumber)) {
      // If this is pk field, transform the Key into its String representation.
      return KeyFactory.encodeKey(datastoreEntity.getKey());
    }
    return (String) fetchObjectField(fieldNumber);
  }

  public short fetchShortField(int fieldNumber) {
    // the datastore stores shorts as longs
    return (short) fetchLongField(fieldNumber);
  }

  public Object fetchObjectField(int fieldNumber) {
    return datastoreEntity.getProperty(getFieldName(fieldNumber));
  }


  public long fetchLongField(int fieldNumber) {
    return (Long) fetchObjectField(fieldNumber);
  }

  public int fetchIntField(int fieldNumber) {
    // the datastore stores ints as longs
    return (int) fetchLongField(fieldNumber);
  }

  public float fetchFloatField(int fieldNumber) {
    // the datastore stores floats as doubles
    return (float) fetchDoubleField(fieldNumber);
  }

  public double fetchDoubleField(int fieldNumber) {
    return (Double) fetchObjectField(fieldNumber);
  }

  public char fetchCharField(int fieldNumber) {
    // the datastore stores chars as longs
    return (char) fetchLongField(fieldNumber);
  }

  public byte fetchByteField(int fieldNumber) {
    // the datastore stores bytes as longs
    return Long.valueOf(fetchLongField(fieldNumber)).byteValue();
  }

  public boolean fetchBooleanField(int fieldNumber) {
    return (Boolean) fetchObjectField(fieldNumber);
  }

  public void storeStringField(int fieldNumber, String value) {
    // We assume pks are of type String.
    if (isPK(fieldNumber)) {
      // If this is pk field, transform the String into its Key representation.
      Key key = KeyFactory.decodeKey(value);
      // TODO(maxr): Unless we can get some guarantees about the order in which
      // these store methods are called we will need to lazily instantiate the entity
//      datastoreEntity.set
    } else {
      storeObjectField(fieldNumber, value);
    }
  }

  public void storeShortField(int fieldNumber, short value) {
    // The datastore stores shorts as longs.
    storeLongField(fieldNumber, value);
  }

  public void storeObjectField(int fieldNumber, Object value) {
    datastoreEntity.setProperty(getFieldName(fieldNumber), value);
  }

  public void storeLongField(int fieldNumber, long value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeIntField(int fieldNumber, int value) {
    // The datastore stores ints as longs.
    storeLongField(fieldNumber, value);
  }

  public void storeFloatField(int fieldNumber, float value) {
    // The datastore stores floats as doubles.
    storeDoubleField(fieldNumber, value);
  }

  public void storeDoubleField(int fieldNumber, double value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeCharField(int fieldNumber, char value) {
    // The datastore stores chars as longs.
    storeLongField(fieldNumber, (long) value);
  }

  public void storeByteField(int fieldNumber, byte value) {
    // The datastore stores bytes as longs.
    storeLongField(fieldNumber, (long) value);
  }

  public void storeBooleanField(int fieldNumber, boolean value) {
    storeObjectField(fieldNumber, value);
  }

  boolean isPK(int fieldNumber) {
    return sm.getClassMetaData().getPKMemberPositions()[0] == fieldNumber;
  }

  String getFieldName(int fieldNumber) {
    return sm.getClassMetaData().getMetaDataForMemberAtRelativePosition(fieldNumber).getName();
  }
}