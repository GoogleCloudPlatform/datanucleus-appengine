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
package org.datanucleus.store.appengine.valuegenerator;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.KeyRange;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.OMFContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.SequenceMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.appengine.DatastoreManager;
import org.datanucleus.store.appengine.DatastoreServiceFactoryInternal;
import org.datanucleus.store.appengine.EntityUtils;
import org.datanucleus.store.appengine.Utils;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;

import java.util.List;
import java.util.Properties;

/**
 * A database sequence abstraction on top of the
 * {@link DatastoreService#allocateIds} functionality.
 *
 * By default the sequence name is the kind of the entity being persisted.
 * This is a different scheme than the one offered by the datastore in that
 * it does not involve the ancestor chain of the entity being persisted.
 * If you're using sequences to generate ids for a certain child object class
 * it is critical that you do _not_ let the datastore generate ids for that
 * same child object class.  This is because the datastore will assign ids
 * within the id-space identified by the parent key and the child kind,
 * while the sequence will just assign ids within the id-space identified
 * just by the child kind.  These id-spaces overlap, so you have to pick
 * one or the other and stick with it.
 *
 * @author Max Ross <maxr@google.com>
 */
public class SequenceGenerator extends AbstractDatastoreGenerator {

  private static final String SEQUENCE_POSTFIX = "_SEQUENCE__";
  private static final String KEY_CACHE_SIZE_PROPERTY = "key-cache-size";

  // can't be final because we need the storeMgr to derive it, and storeMgr
  // isn't set until setStoreManager is invoked.
  private String sequenceName;

  public SequenceGenerator(String name, Properties props) {
    super(name, props);
  }

  @Override
  public void setStoreManager(StoreManager storeMgr) {
    super.setStoreManager(storeMgr);
    OMFContext omfContext = getOMFContext();
    MetaDataManager mdm = omfContext.getMetaDataManager();
    ClassLoaderResolver clr = omfContext.getClassLoaderResolver(getClass().getClassLoader());
    AbstractClassMetaData acmd = mdm.getMetaDataForClass((String) properties.get("class-name"), clr);
    sequenceName = determineSequenceName(acmd);
    if (sequenceName != null) {
      // Fetch the sequence data
      SequenceMetaData sequenceMetaData = mdm.getMetaDataForSequence(clr, sequenceName);
      if (sequenceMetaData != null) {
        // derive allocation size and sequence name from the sequence meta data
        if (sequenceMetaData.hasExtension(KEY_CACHE_SIZE_PROPERTY)) {
          allocationSize = Integer.parseInt(sequenceMetaData.getValueForExtension(KEY_CACHE_SIZE_PROPERTY));
        } else {
          allocationSize = longToInt(sequenceMetaData.getAllocationSize());
        }
        sequenceName = sequenceMetaData.getDatastoreSequence();
      } else {
        // key cache size is passed in as a prop for JDO when the sequence
        // is used directly (pm.getSequence())
        if (properties.getProperty(KEY_CACHE_SIZE_PROPERTY) != null) {
          allocationSize = Integer.parseInt(properties.getProperty(KEY_CACHE_SIZE_PROPERTY));
        }
      }
    }
    // derive the sequence name from the class meta data
    if (sequenceName == null) {
      sequenceName = deriveSequenceNameFromClassMetaData(acmd);
    }
  }

  private String determineSequenceName(AbstractClassMetaData acmd) {
    String sequenceName = (String) properties.get("sequence-name");
    if (sequenceName != null) {
      return sequenceName;
    }
    String fieldName = (String) properties.get("field-name");
    // Look up the meta-data for the field with the generator
    AbstractMemberMetaData ammd =
        acmd.getMetaDataForMember(fieldName.substring(fieldName.lastIndexOf(".") + 1));
    // For JPA the sequence name is stored as the valueGeneratorName
    return ammd.getSequence() != null ? ammd.getSequence() : ammd.getValueGeneratorName();
  }

  /**
   * Conversion method that blows up if it detects overflow.
   * We know the max batch size supported by the datastore is smaller than
   * {@link Integer#MAX_VALUE} so we want to make sure we don't overflow
   * and end up with a valid value that isn't what the user specified.
   */
  private int longToInt(Long val) {
    if (Long.valueOf(val.intValue()).longValue() != val) {
      throw new NucleusUserException("id batch size is too big: " + val).setFatal();
    }
    return val.intValue();
  }

  private OMFContext getOMFContext() {
    return storeMgr.getOMFContext();
  }

  // Default is the kind with _Sequence__ appended
  private String deriveSequenceNameFromClassMetaData(AbstractClassMetaData acmd) {
    return EntityUtils.determineKind(acmd, ((DatastoreManager) storeMgr).getIdentifierFactory()) +
        SEQUENCE_POSTFIX;
  }

  protected ValueGenerationBlock reserveBlock(long size) {
    if (sequenceName == null) {
      // shouldn't happen
      throw new IllegalStateException("sequence name is null");
    }
    DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService();
    KeyRange range = ds.allocateIds(sequenceName, size);
    // Inefficient, but this is
    List<Long> ids = Utils.newArrayList();
    long current = range.getStart().getId();
    for (int i = 0; i < size; i++) {
      ids.add(current + i);
    }
    return new ValueGenerationBlock(ids);
  }
}
