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
import com.google.appengine.datanucleus.mapping.InsertMappingConsumer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NoPersistenceInformationException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;

import org.datanucleus.state.ObjectProviderFactory;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.mapped.mapping.EmbeddedMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingConsumer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

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

  private static final TypeConversionUtils TYPE_CONVERSION_UTILS = new TypeConversionUtils();

  /**
   * List of FieldManagerState, with the last one being the root object, and earlier ones being those
   * for (nested) embedded objects.
   */
  protected final LinkedList<FieldManagerState> fieldManagerStateStack =
      new LinkedList<FieldManagerState>();

  /** ObjectProvider for the primary object here. Use fieldManagerStateStack for any nested embedded objects. */
  protected ObjectProvider op;

  /** Entity being populated by this FieldManager. */
  protected Entity datastoreEntity;

  /**
   * Constructor.
   * @param op ObjectProvider for the object being handled.
   * @param datastoreEntity Entity to represent this object
   * @param fieldNumbers Field numbers that will be processed (optional, null means all fields).
   */
  protected DatastoreFieldManager(ObjectProvider op, Entity datastoreEntity, int[] fieldNumbers) {
    AbstractMemberMetaDataProvider ammdProvider = new AbstractMemberMetaDataProvider() {
      public AbstractMemberMetaData get(int fieldNumber) {
        return getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
      }
    };

    this.op = op;
    this.datastoreEntity = datastoreEntity;

    InsertMappingConsumer mappingConsumer = buildMappingConsumer(op.getClassMetaData(), 
        op.getExecutionContext().getClassLoaderResolver(), fieldNumbers, null);
    this.fieldManagerStateStack.addFirst(new FieldManagerState(op, ammdProvider, mappingConsumer, false));

    // Sanity check
    String expectedKind = EntityUtils.determineKind(getClassMetaData(), op.getExecutionContext());
    if (!expectedKind.equals(datastoreEntity.getKind())) {
      throw new NucleusException(
          "ObjectProvider is for <" + expectedKind + "> but key is for <" + datastoreEntity.getKind()
              + ">.  One way this can happen is if you attempt to fetch an object of one type using"
              + " a Key of a different type.").setFatal();
    }
  }

  Entity getEntity() {
    return datastoreEntity;
  }

  ObjectProvider getObjectProvider() {
    return fieldManagerStateStack.getFirst().op;
  }

  AbstractMemberMetaData getMetaData(int fieldNumber) {
    return fieldManagerStateStack.getFirst().abstractMemberMetaDataProvider.get(fieldNumber);
  }

  ExecutionContext getExecutionContext() {
    return getObjectProvider().getExecutionContext();
  }

  AbstractClassMetaData getClassMetaData() {
    return getObjectProvider().getClassMetaData();
  }

  ClassLoaderResolver getClassLoaderResolver() {
    return getExecutionContext().getClassLoaderResolver();
  }

  DatastoreManager getStoreManager() {
    return (DatastoreManager) op.getExecutionContext().getStoreManager();
  }

  DatastoreTable getDatastoreTable() {
    return getStoreManager().getDatastoreClass(getClassMetaData().getFullClassName(), getClassLoaderResolver());
  }

  protected boolean isPK(int fieldNumber) {
    // ignore the pk annotations if this object is embedded
    if (fieldManagerStateStack.getFirst().isEmbedded) {
      return false;
    }
    int[] pkPositions = getClassMetaData().getPKMemberPositions();
    // Assumes that if we have a pk we only have a single field pk
    return pkPositions != null && pkPositions[0] == fieldNumber;
  }

  /**
   * Constructs a {@link MappingConsumer}.  If an {@link EmbeddedMetaData} is provided that means we need 
   * to construct the consumer in an embedded context.
   * TODO Drop this and use metadata
   */
  protected InsertMappingConsumer buildMappingConsumer(
      AbstractClassMetaData acmd, ClassLoaderResolver clr, int[] fieldNumbers, EmbeddedMetaData emd) {
    DatastoreTable table = getStoreManager().getDatastoreClass(acmd.getFullClassName(), clr);
    if (table == null) {
      // We've seen this when there is a class with the superclass inheritance strategy that does not have a parent
      throw new NoPersistenceInformationException(acmd.getFullClassName());
    }

    // Process all required fields for this case
    // a). INSERT : all fields
    // b). embedded field object : all fields
    // c). UPDATE : specified fields only
    InsertMappingConsumer consumer = new InsertMappingConsumer(acmd);
    if (emd == null) {
      table.provideDatastoreIdMappings(consumer);
      table.providePrimaryKeyMappings(consumer);
    } else {
      // skip pk mappings if embedded
    }
    if (fieldNumbers == null || emd != null) {
      table.provideNonPrimaryKeyMappings(consumer, emd != null);
    } else {
      AbstractMemberMetaData[] fmds = new AbstractMemberMetaData[fieldNumbers.length];
      if (fieldNumbers != null && fieldNumbers.length > 0) {
        for (int i = 0; i < fieldNumbers.length; i++) {
          fmds[i] = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
        }
      }
      table.provideMappingsForMembers(consumer, fmds, false);
    }

    if (emd != null) {
      DatastoreTable parentTable = getStoreManager().getDatastoreClass(getClassMetaData().getFullClassName(), clr);
      AbstractMemberMetaData parentField = (AbstractMemberMetaData) emd.getParent();
      EmbeddedMapping embeddedMapping =
          (EmbeddedMapping) parentTable.getMappingForFullFieldName(parentField.getFullFieldName());

      // Build a map that takes us from full field name to member meta data.
      // The member meta data in this map comes from the embedded mapping,
      // which means it will have subclass fields and column overrides.
      Map<String, AbstractMemberMetaData> embeddedMetaDataByFullFieldName = Utils.newHashMap();
      int numMappings = embeddedMapping.getNumberOfJavaTypeMappings();
      for (int i = 0; i < numMappings; i++) {
        JavaTypeMapping fieldMapping = embeddedMapping.getJavaTypeMapping(i);
        AbstractMemberMetaData ammd = fieldMapping.getMemberMetaData();
        embeddedMetaDataByFullFieldName.put(ammd.getFullFieldName(), ammd);
      }

      // We're going to update the consumer's map so make a copy over which we can safely iterate.
      Map<Integer, AbstractMemberMetaData> map =
          new HashMap<Integer, AbstractMemberMetaData>(consumer.getFieldIndexToMemberMetaData());
      for (Map.Entry<Integer, AbstractMemberMetaData> entry : map.entrySet()) {
        // For each value in the consumer's map, find the corresponding value in
        // the embeddedMetaDataByFullFieldName we just built and install it as
        // a replacement.  This will given us access to subclass fields and
        // column overrides as we fetch/store the embedded field.
        AbstractMemberMetaData replacement = embeddedMetaDataByFullFieldName.get(entry.getValue().getFullFieldName());
        if (replacement == null) {
          throw new RuntimeException(
              "Unable to locate " + entry.getValue().getFullFieldName() + " in embedded meta-data "
              + "map.  This is most likely an App Engine bug.");
        }
        consumer.getFieldIndexToMemberMetaData().put(entry.getKey(), replacement);
      }
    }
    return consumer;
  }

  protected RuntimeException exceptionForUnexpectedKeyType(String fieldType, int fieldNumber) {
    return new IllegalStateException(fieldType + " for type " + getClassMetaData().getName()
            + " is of unexpected type " + getMetaData(fieldNumber).getType().getName()
            + " (must be String, Long, or " + Key.class.getName() + ")");
  }

  protected AbstractMemberMetaDataProvider getEmbeddedAbstractMemberMetaDataProvider(
      final InsertMappingConsumer consumer) {
    // This implementation gets the meta data from the mapping consumer.
    // This is needed to ensure we see column overrides that are specific to
    // a specific embedded field and subclass fields, which aren't included
    // in EmbeddedMetaData for some reason.
    return new AbstractMemberMetaDataProvider() {
      public AbstractMemberMetaData get(int fieldNumber) {
        return consumer.getMemberMetaDataForIndex(fieldNumber);
      }
    };
  }

  protected ObjectProvider getEmbeddedObjectProvider(AbstractMemberMetaData ammd, int fieldNumber, Object value) {
    if (value == null) {
      // We need all fields of an embedded object to have properties in the Entity.
      // This just creates a dummy embedded object
      value = JDOImplHelper.getInstance().newInstance(
          ammd.getType(), (javax.jdo.spi.StateManager) getObjectProvider());
    }

    ObjectProvider embeddedOP = getExecutionContext().findObjectProvider(value);
    if (embeddedOP == null) {
        embeddedOP = ObjectProviderFactory.newForEmbedded(getExecutionContext(), value, false, getObjectProvider(), fieldNumber);
        embeddedOP.setPcObjectType(ObjectProvider.EMBEDDED_PC);
    }
    return embeddedOP;
  }

  /**
   * Just exists so we can override in tests. 
   */
  TypeConversionUtils getConversionUtils() {
    return TYPE_CONVERSION_UTILS;
  }

  /**
   * Translates field numbers into {@link AbstractMemberMetaData}.
   * TODO Drop this nonsense, that's what we have AbstractClassMetaData for
   */
  protected interface AbstractMemberMetaDataProvider {
    AbstractMemberMetaData get(int fieldNumber);
  }

  // TODO When we split embedded usage into own FieldManager this will move into the owning class
  protected static final class FieldManagerState {
    protected final ObjectProvider op;
    protected final AbstractMemberMetaDataProvider abstractMemberMetaDataProvider;
    protected final InsertMappingConsumer mappingConsumer;
    protected final boolean isEmbedded;

    protected FieldManagerState(ObjectProvider op,
        AbstractMemberMetaDataProvider abstractMemberMetaDataProvider,
        InsertMappingConsumer mappingConsumer,
        boolean isEmbedded) {
      this.op = op;
      this.abstractMemberMetaDataProvider = abstractMemberMetaDataProvider;
      this.mappingConsumer = mappingConsumer;
      this.isEmbedded = isEmbedded;
    }
  }
}