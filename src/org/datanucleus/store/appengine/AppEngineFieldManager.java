package org.datanucleus.store.appengine;

import org.datanucleus.StateManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.KeyFactory;

/**
 * FieldManager for converting app engine datastore entities into POJOs and
 * vice-versa.
 *
 * @author Max Ross <maxr@google.com>
 */
public class AppEngineFieldManager implements FieldManager {

  private final StateManager sm;
  private final Entity result;

  public AppEngineFieldManager(StateManager sm, Entity result) {
    this.sm = sm;
    this.result = result;
  }

  public String fetchStringField(int fieldNumber) {
    // We assume pks are of type String.
    // TODO(maxr): validate that pks are of type String at time of enhancement.
    if (sm.getClassMetaData().getPKMemberPositions()[0] == fieldNumber) {
      // If this is pk field, transform the Key into its String representation.
      return KeyFactory.encodeKey(result.getKey());
    }
    return (String) result.getProperty(
        sm.getClassMetaData().getMetaDataForMemberAtRelativePosition(fieldNumber).getName());
  }

  public short fetchShortField(int fieldNumber) {
    // TODO Auto-generated method stub
    return 0;
  }

  public Object fetchObjectField(int fieldNumber) {
    return null;
  }


  public long fetchLongField(int fieldNumber) {
    return (Long) result.getProperty(
        sm.getClassMetaData().getMetaDataForMemberAtRelativePosition(fieldNumber).getName());
  }

  public int fetchIntField(int fieldNumber) {
    Object propVal = result.getProperty(
        sm.getClassMetaData().getMetaDataForMemberAtRelativePosition(fieldNumber).getName());
    return propVal == null ? 0 : ((Long) propVal).intValue();
  }

  public float fetchFloatField(int fieldNumber) {
    // TODO Auto-generated method stub
    return 0;
  }

  public double fetchDoubleField(int fieldNumber) {
    // TODO Auto-generated method stub
    return 0;
  }

  public char fetchCharField(int fieldNumber) {
    // TODO Auto-generated method stub
    return 0;
  }

  public byte fetchByteField(int fieldNumber) {
    // TODO Auto-generated method stub
    return 0;
  }

  public boolean fetchBooleanField(int fieldNumber) {
    // TODO Auto-generated method stub
    return false;
  }

  public void storeStringField(int fieldNumber, String value) {
    result.setProperty(
        sm.getClassMetaData().getMetaDataForMemberAtRelativePosition(fieldNumber).getName(), value);
  }

  public void storeShortField(int fieldNumber, short value) {
    // TODO Auto-generated method stub
  }

  public void storeObjectField(int fieldNumber, Object value) {
    // TODO Auto-generated method stub
  }

  public void storeLongField(int fieldNumber, long value) {
    // TODO Auto-generated method stub
  }

  public void storeIntField(int fieldNumber, int value) {
    // TODO Auto-generated method stub
  }

  public void storeFloatField(int fieldNumber, float value) {
    // TODO Auto-generated method stub
  }

  public void storeDoubleField(int fieldNumber, double value) {
    // TODO Auto-generated method stub
  }

  public void storeCharField(int fieldNumber, char value) {
    // TODO Auto-generated method stub
  }

  public void storeByteField(int fieldNumber, byte value) {
    // TODO Auto-generated method stub
  }

  public void storeBooleanField(int fieldNumber, boolean value) {
    // TODO Auto-generated method stub
  }
}