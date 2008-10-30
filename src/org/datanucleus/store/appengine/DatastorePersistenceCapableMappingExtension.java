// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.StatementExpressionIndex;
import org.datanucleus.store.mapped.mapping.PersistenceCapableMapping;

/**
 * Datastore specific extension to {@link PersistenceCapableMapping}.
 * This is used by datanucleus during relation-resolution.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastorePersistenceCapableMappingExtension {

  public boolean fieldIsNull(PersistenceCapableMapping pcm, ObjectManager om, Object relatedKey,
      int[] param) {
    return relatedKey == null;
  }

  public Object getObjectAtIndex(PersistenceCapableMapping pcm, ObjectManager om, Object obj,
      int index) throws Exception {
    Entity entity = (Entity) obj;
    org.datanucleus.store.mapped.mapping.DatastoreMapping mapping = pcm.getDataStoreMapping(index);
    IdentifierFactory idFactory = ((MappedStoreManager) om.getStoreManager()).getIdentifierFactory();
    return entity.getProperty(
        EntityUtils.getPropertyName(idFactory, mapping.getDatastoreField().getFieldMetaData()));
  }

  public FieldManager getFieldManager(PersistenceCapableMapping pcm, StateManager sm, ObjectManager om,
      Object obj,
      StatementExpressionIndex[] statementExpressionIndex) {
    return new KeyOnlyFieldManager((Key) obj);
  }

  /**
   * A {@link FieldManager} implementation that can only be used for managing
   * keys.  Everything else throws {@link UnsupportedOperationException}.
   */
  private static class KeyOnlyFieldManager implements FieldManager {
    private final Key key;

    private KeyOnlyFieldManager(Key key) {
      this.key = key;
    }

    public void storeBooleanField(int fieldNumber, boolean value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeByteField(int fieldNumber, byte value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeCharField(int fieldNumber, char value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeDoubleField(int fieldNumber, double value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeFloatField(int fieldNumber, float value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeIntField(int fieldNumber, int value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeLongField(int fieldNumber, long value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeShortField(int fieldNumber, short value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeStringField(int fieldNumber, String value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeObjectField(int fieldNumber, Object value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public boolean fetchBooleanField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public byte fetchByteField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public char fetchCharField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public double fetchDoubleField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public float fetchFloatField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public int fetchIntField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public long fetchLongField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public short fetchShortField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public String fetchStringField(int fieldNumber) {
      return KeyFactory.encodeKey(key);
    }

    public Object fetchObjectField(int fieldNumber) {
      return key;
    }
  }
}
