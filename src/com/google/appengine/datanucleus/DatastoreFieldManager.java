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
import com.google.appengine.datanucleus.mapping.DatastoreTable;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.state.ObjectProviderFactory;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;

import java.util.LinkedList;

import javax.jdo.spi.JDOImplHelper;

/**
 * FieldManager for converting app engine datastore entities into POJOs and vice-versa.
 *
 * Most of the complexity in this class is due to the fact that the datastore automatically promotes certain types:
 * <ul>
 * <li>Promotes short/Short, int/Integer, and byte/Byte to long.</li>
 * <li>Promotes float/Float to double.</li>
 * <li>the datastore does not support char/Character.  We've made the decision to promote this to long as well.</li>
 * </ul>
 *
 * We handle the conversion in both directions. At one point we let the datastore api do the conversion from 
 * pojos to {@link Entity Entities} but we this proved problematic in the case where we return entities that were
 * cached during insertion to avoid issuing a get().  In this case we then end up trying to construct a pojo from
 * an {@link Entity} whose contents violate the datastore api invariants, and we end up with cast exceptions.
 * So, we do the conversion ourselves, even though this duplicates logic in the ORM and the datastore api.
 *
 * @author Max Ross <maxr@google.com>
 */
public abstract class DatastoreFieldManager extends AbstractFieldManager {

  private final TypeConversionUtils typeConversionUtils;

  /**
   * List of FieldManagerState, with the last one being the root object, and earlier ones being those
   * for (nested) embedded objects.
   */
  protected final LinkedList<FieldManagerState> fieldManagerStateStack =
      new LinkedList<FieldManagerState>();

  /** ExecutionContext for this usage. */
  protected ExecutionContext ec;

  /** Entity being populated/accessed by this FieldManager. */
  protected Entity datastoreEntity;

  /**
   * Constructor.
   * @param op ObjectProvider for the object being handled.
   * @param datastoreEntity Entity to represent this object
   * @param fieldNumbers Field numbers that will be processed (optional, null means all fields).
   */
  protected DatastoreFieldManager(ObjectProvider op, Entity datastoreEntity, int[] fieldNumbers) {
    this.ec = op.getExecutionContext();
    this.datastoreEntity = datastoreEntity;
    this.fieldManagerStateStack.addFirst(new FieldManagerState(op));
    DatastoreManager storeManager = (DatastoreManager) ec.getStoreManager();
    this.typeConversionUtils = storeManager.getTypeConversionUtils();

    // Sanity check
    String expectedKind = EntityUtils.determineKind(op.getClassMetaData(), ec);
    if (!expectedKind.equals(datastoreEntity.getKind())) {
      throw new NucleusException(
          "ObjectProvider is for <" + expectedKind + "> but key is for <" + datastoreEntity.getKind()
              + ">.  One way this can happen is if you attempt to fetch an object of one type using"
              + " a Key of a different type.").setFatal();
    }
  }

  /**
   * Accessor for the datastore entity that is being managed.
   * @return Datastore Entity
   */
  Entity getEntity() {
    return datastoreEntity;
  }

  /**
   * Accessor for the ObjectProvider that is currently being processed.
   * @return ObjectProvider
   */
  ObjectProvider getObjectProvider() {
    return fieldManagerStateStack.getFirst().op;
  }

  /**
   * Accessor for the metadata for the specified member.
   * Allows for overridden metadata for embedded members if we are processing an embedded class
   * @param fieldNumber Absolute position of the member.
   * @return The metadata to use
   */
  AbstractMemberMetaData getMetaData(int fieldNumber) {
    AbstractMemberMetaData mmd = getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
    if (fieldManagerStateStack.getFirst().embmd == null) {
      return mmd;
    }

    // Use overriding EmbeddedMetaData if present for this field, otherwise use from class
    AbstractMemberMetaData[] embmmds = fieldManagerStateStack.getFirst().embmd.getMemberMetaData();
    if (embmmds != null) {
      for (int i=0;i<embmmds.length;i++) {
        if (mmd.getName().equals(embmmds[i].getName())) {
          return embmmds[i];
        }
      }
    }
    return mmd;
  }

  protected boolean isPK(int fieldNumber) {
    if (fieldManagerStateStack.getFirst().embmd != null) {
      // ignore the pk annotations if this object is embedded
      return false;
    }

    // Assumes that if we have a pk we only have a single field pk
    int[] pkPositions = getClassMetaData().getPKMemberPositions();
    return pkPositions != null && pkPositions[0] == fieldNumber;
  }

  AbstractClassMetaData getClassMetaData() {
    return getObjectProvider().getClassMetaData();
  }

  ClassLoaderResolver getClassLoaderResolver() {
    return ec.getClassLoaderResolver();
  }

  DatastoreManager getStoreManager() {
    return (DatastoreManager) ec.getStoreManager();
  }

  DatastoreTable getDatastoreTable() {
    return getStoreManager().getDatastoreClass(getClassMetaData().getFullClassName(), getClassLoaderResolver());
  }

  protected RuntimeException exceptionForUnexpectedKeyType(String fieldType, int fieldNumber) {
    return new IllegalStateException(fieldType + " for type " + getClassMetaData().getName()
            + " is of unexpected type " + getMetaData(fieldNumber).getType().getName()
            + " (must be String, Long, long, or " + Key.class.getName() + ")");
  }

  /**
   * Convenience method to return the ObjectProvider for the specified value.
   * The return will never be null, since we always want to store all properties into the 
   * owning object (for querying).
   * @param ammd Metadata for the member where this object is embedded
   * @param fieldNumber Field number in the owning object where this is embedded
   * @param value The embedded value (or null, maybe when retrieving)
   * @return ObjectProvider to use
   */
  protected ObjectProvider getEmbeddedObjectProvider(Class type, int fieldNumber, Object value) {
    if (value == null) {
      value = JDOImplHelper.getInstance().newInstance(type, 
          (javax.jdo.spi.StateManager)getObjectProvider());
    }

    ObjectProvider embeddedOP = ec.findObjectProvider(value);
    if (embeddedOP == null) {
        embeddedOP = ObjectProviderFactory.newForEmbedded(ec, value, false, getObjectProvider(), fieldNumber);
        embeddedOP.setPcObjectType(ObjectProvider.EMBEDDED_PC);
    }
    return embeddedOP;
  }

  protected String getPropertyNameForMember(AbstractMemberMetaData mmd) {
    String propName = EntityUtils.getPropertyName(getStoreManager().getIdentifierFactory(), mmd);

    if (fieldManagerStateStack.getFirst().index != null) {
      // Embedded Collection uses property name with suffixed index
      return propName + "." + fieldManagerStateStack.getFirst().index;
    }
    return propName;
  }

  /**
   * Just exists so we can override in tests. 
   */
  TypeConversionUtils getConversionUtils() {
    return typeConversionUtils;
  }

  protected static final class FieldManagerState {
    protected final ObjectProvider op;
    protected final EmbeddedMetaData embmd;
    protected final Integer index;

    protected FieldManagerState(ObjectProvider op) {
      this.op = op;
      this.embmd = null;
      index = null;
    }

    protected FieldManagerState(ObjectProvider op, EmbeddedMetaData embmd) {
      this.op = op;
      this.embmd = embmd;
      index = null;
    }

    protected FieldManagerState(ObjectProvider op, EmbeddedMetaData embmd, int pos) {
      this.op = op;
      this.embmd = embmd;
      this.index = pos;
    }
  }
}
