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
package com.google.appengine.datanucleus.valuegenerator;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.KeyRange;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.SequenceMetaData;
import org.datanucleus.store.StoreManager;

import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.DatastoreServiceFactoryInternal;
import com.google.appengine.datanucleus.EntityUtils;
import com.google.appengine.datanucleus.Utils;

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

  public static final String SEQUENCE_POSTFIX = "_SEQUENCE__";
  private static final String KEY_CACHE_SIZE_PROPERTY = "key-cache-size";

  // TODO(maxr): Get rid of this when the local datastore id allocation behavior
  // mirrors prod
  private static final ThreadLocal<String> SEQUENCE_POSTFIX_APPENDAGE = new ThreadLocal<String>() {
    @Override
    protected String initialValue() {
      return "";
    }
  };

  // can't be final because we need the storeMgr to derive it, and storeMgr
  // isn't set until setStoreManager is invoked.
  private String sequenceName;

  public SequenceGenerator(String name, Properties props) {
    super(name, props);
  }

  @Override
  public void setStoreManager(StoreManager storeMgr) {
    super.setStoreManager(storeMgr);
    NucleusContext omfContext = storeMgr.getNucleusContext();
    MetaDataManager mdm = omfContext.getMetaDataManager();
    ClassLoaderResolver clr = omfContext.getClassLoaderResolver(getClass().getClassLoader());
    AbstractClassMetaData acmd = mdm.getMetaDataForClass((String) properties.get("class-name"), clr);
    if (acmd != null) {
      ((DatastoreManager) storeMgr).validateMetaDataForClass(acmd, clr);
    }

    sequenceName = determineSequenceName(acmd);
    if (sequenceName != null) {
      // Fetch the sequence data
      SequenceMetaData sequenceMetaData = mdm.getMetaDataForSequence(clr, sequenceName);
      if (sequenceMetaData != null) {
        // derive allocation size and sequence name from the sequence meta data
        if (sequenceMetaData.hasExtension(KEY_CACHE_SIZE_PROPERTY)) {
          allocationSize = Integer.parseInt(sequenceMetaData.getValueForExtension(KEY_CACHE_SIZE_PROPERTY));
        } else {
          allocationSize = sequenceMetaData.getAllocationSize();
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

  /**
   * AbstractClassMetaData can be null.
   */
  private String determineSequenceName(AbstractClassMetaData acmd) {
    String sequenceName = (String) properties.get("sequence-name");
    if (sequenceName != null) {
      return sequenceName;
    }
    if (acmd == null) {
      throw new IllegalStateException(
          "Received a null AbstractClassMetaData and properties did not contain a sequence-name attribute.");
    }
    String fieldName = (String) properties.get("field-name");
    // Look up the meta-data for the field with the generator
    AbstractMemberMetaData ammd =
        acmd.getMetaDataForMember(fieldName.substring(fieldName.lastIndexOf(".") + 1));
    // For JPA the sequence name is stored as the valueGeneratorName
    return ammd.getSequence() != null ? ammd.getSequence() : ammd.getValueGeneratorName();
  }

  // Default is the kind with _Sequence__ appended
  private String deriveSequenceNameFromClassMetaData(AbstractClassMetaData acmd) {
    ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(getClass().getClassLoader());
    return EntityUtils.determineKind(acmd, (DatastoreManager) storeMgr, clr) +
        SEQUENCE_POSTFIX + SEQUENCE_POSTFIX_APPENDAGE.get();
  }

  protected ValueGenerationBlock reserveBlock(long size) {
    if (sequenceName == null) {
      // shouldn't happen
      throw new IllegalStateException("sequence name is null");
    }
    DatastoreServiceConfig config = ((DatastoreManager) storeMgr).getDefaultDatastoreServiceConfigForWrites();
    DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService(config);
    KeyRange range = ds.allocateIds(sequenceName, size);
    // Too bad we can't pass an iterable and construct the ids
    // on demand.
    List<Long> ids = Utils.newArrayList();
    long current = range.getStart().getId();
    for (int i = 0; i < size; i++) {
      ids.add(current + i);
    }
    return new ValueGenerationBlock(ids);
  }

  public static void setSequencePostfixAppendage(String appendage) {
    SEQUENCE_POSTFIX_APPENDAGE.set(appendage);
  }

  public static void clearSequencePostfixAppendage() {
    SEQUENCE_POSTFIX_APPENDAGE.remove();
  }
}
