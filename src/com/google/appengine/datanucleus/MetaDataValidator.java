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

import com.google.appengine.api.datastore.Key;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InvalidMetaDataException;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.OrderMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.SequenceMetaData;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import java.util.Map;
import java.util.Set;

/**
 * AppEngine-specific rules validator for Meta Data.
 *
 * @author Max Ross <maxr@google.com>
 */
public class MetaDataValidator {
  protected static final Localiser GAE_LOCALISER = Localiser.getInstance(
        "com.google.appengine.datanucleus.Localisation", DatastoreManager.class.getClassLoader());

  private static final Set<String> ONE_OR_ZERO_EXTENSIONS =
      Utils.newHashSet(
          DatastoreManager.PK_ID,
          DatastoreManager.ENCODED_PK,
          DatastoreManager.PK_NAME,
          DatastoreManager.PARENT_PK);

  private static final Set<String> NOT_PRIMARY_KEY_EXTENSIONS =
      Utils.newHashSet(
          DatastoreManager.PK_ID,
          DatastoreManager.PK_NAME,
          DatastoreManager.PARENT_PK);

  private static final Set<String> REQUIRES_ENCODED_STRING_PK_EXTENSIONS =
      Utils.newHashSet(
          DatastoreManager.PK_ID,
          DatastoreManager.PK_NAME);

  /**
   * Defines the various actions we can take when we encounter ignorable meta-data.
   */
  enum IgnorableMetaDataBehavior {
    NONE, // Do nothing at all.
    WARN, // Log a warning.
    ERROR;// Throw an exception.

    private static IgnorableMetaDataBehavior valueOf(String val, IgnorableMetaDataBehavior returnIfNull) {
      if (val == null) {
        return returnIfNull;
      }
      return valueOf(val);
    }
  }

  /**
   * Config property that determines the action we take when we encounter ignorable meta-data.
   */
  private static final String IGNORABLE_META_DATA_BEHAVIOR_PROPERTY = "datanucleus.appengine.ignorableMetaDataBehavior";

  /**
   * This message is appended to every ignorable meta-data warning so users
   * know they can configure it.
   */
  static final String ADJUST_WARNING_MSG =
      String.format("You can modify this warning by setting the %s property in your config.  "
                    + "A value of %s will silence the warning.  "
                    + "A value of %s will turn the warning into an exception.",
                    IGNORABLE_META_DATA_BEHAVIOR_PROPERTY,
                    IgnorableMetaDataBehavior.NONE,
                    IgnorableMetaDataBehavior.ERROR);

  private static final String ALLOW_MULTIPLE_RELATIONS_OF_SAME_TYPE =
      "datanucleus.appengine.allowMultipleRelationsOfSameType";

  private final DatastoreManager storeMgr;
  private final MetaDataManager metaDataManager;
  private final ClassLoaderResolver clr;

  public MetaDataValidator(DatastoreManager storeMgr, MetaDataManager metaDataManager, ClassLoaderResolver clr) {
    this.storeMgr = storeMgr;
    this.metaDataManager = metaDataManager;
    this.clr = clr;
  }

  /**
   * validate the metadata for the provided class to add GAE/J restrictions.
   * @param acmd Metadata for the class to validate
   */
  public void validate(AbstractClassMetaData acmd) {
    if (acmd.isEmbeddedOnly()) {
      // Nothing to check
      return;
    }

    NucleusLogger.METADATA.info("Performing appengine-specific metadata validation for " + acmd.getFullClassName());

    // validate inheritance
    // TODO Put checks on supported inheritance here

    AbstractMemberMetaData pkMemberMetaData = null;
    Class<?> pkType = null;
    boolean noParentAllowed = false;

    if (acmd.getIdentityType() == IdentityType.DATASTORE) {
      pkType = Key.class;
      ColumnMetaData colmd = acmd.getIdentityMetaData().getColumnMetaData();
      if (colmd != null) {
        if ("varchar".equalsIgnoreCase(colmd.getJdbcType()) || "char".equalsIgnoreCase(colmd.getJdbcType())) {
          pkType = String.class;
        } else  if ("integer".equalsIgnoreCase(colmd.getJdbcType()) || "numeric".equalsIgnoreCase(colmd.getJdbcType())) {
          pkType = Long.class;
        }
      }
      if (pkType == Long.class) {
        noParentAllowed = true;
      }
    } else if (acmd.getIdentityType() == IdentityType.APPLICATION) {
      // Validate primary-key
      int[] pkPositions = acmd.getPKMemberPositions();
      if (pkPositions == null) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.NoPkFields", acmd.getFullClassName());
      }
      if (pkPositions.length != 1) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.CompositePKNotSupported", acmd.getFullClassName());
      }

      // TODO Support composite PKs
      int pkPos = pkPositions[0];
      pkMemberMetaData = acmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPos);

      pkType = pkMemberMetaData.getType();
      if (pkType.equals(Long.class) || pkType.equals(long.class) || 
          pkType.equals(Integer.class) || pkType.equals(int.class)) {
        // Allow Long, long, Integer, int numeric PK types
        noParentAllowed = true;
      } else if (pkType.equals(String.class)) {
        if (!MetaDataUtils.isEncodedPKField(acmd, pkPos)) {
          noParentAllowed = true;
        } else {
          // encoded string pk
          if (hasIdentityStrategy(IdentityStrategy.SEQUENCE, pkMemberMetaData)) {
            throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.SequenceInvalidForEncodedStringPK",
                pkMemberMetaData.getFullFieldName());
          }
        }
      } else if (pkType.equals(Key.class)) {
        if (hasIdentityStrategy(IdentityStrategy.SEQUENCE, pkMemberMetaData)) {
          throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.SequenceInvalidForPKType",
              pkMemberMetaData.getFullFieldName(), Key.class.getName());
        }
      } else {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.InvalidPKTypeForField", 
            pkMemberMetaData.getFullFieldName(), pkType.getName());
      }
    }

    // Validate fields
    Set<String> foundOneOrZeroExtensions = Utils.newHashSet();
    Map<Class<?>, String> nonRepeatableRelationTypes = Utils.newHashMap();

    // the constraints that we check across all fields apply to the entire
    // persistent class hierarchy so we're going to validate every field
    // at every level of the hierarchy.  As an example, this lets us detect
    // multiple one-to-many relationships at different levels of the class hierarchy
    AbstractClassMetaData curCmd = acmd;
    do {
      for (AbstractMemberMetaData ammd : curCmd.getManagedMembers()) {
        validateField(acmd, pkMemberMetaData, noParentAllowed, pkType, foundOneOrZeroExtensions, 
            nonRepeatableRelationTypes, ammd);
      }
      curCmd = curCmd.getSuperAbstractClassMetaData();
    } while (curCmd != null);

    // Look for uniqueness constraints.  Not supported but not necessarily an error
    if (acmd.getUniqueMetaData() != null && acmd.getUniqueMetaData().length > 0) {
      handleIgnorableMapping(acmd, null, "AppEngine.MetaData.UniqueConstraintsNotSupported", 
      "The constraint definition will be ignored.");
    }

    NucleusLogger.METADATA.info("Finished performing appengine-specific metadata validation for " + acmd.getFullClassName());
  }

  private void validateField(AbstractClassMetaData acmd, AbstractMemberMetaData pkMemberMetaData, boolean noParentAllowed,
                             Class<?> pkClass, Set<String> foundOneOrZeroExtensions,
                             Map<Class<?>, String> nonRepeatableRelationTypes, AbstractMemberMetaData ammd) {

    // can only have one field with this extension
    for (String extension : ONE_OR_ZERO_EXTENSIONS) {
      if (ammd.hasExtension(extension)) {
        if (!foundOneOrZeroExtensions.add(extension)) {
          throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.MoreThanOneFieldWithExtension",
              acmd.getFullClassName(), extension);
        }
      }
    }

    if (ammd.hasExtension(DatastoreManager.ENCODED_PK)) {
      if (!ammd.isPrimaryKey() || !ammd.getType().equals(String.class)) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ExtensionForStringPK",
            ammd.getFullFieldName(), DatastoreManager.ENCODED_PK);
      }
    }

    if (ammd.hasExtension(DatastoreManager.PK_NAME)) {
      if (!ammd.getType().equals(String.class)) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ExtensionForStringField",
            ammd.getFullFieldName(), DatastoreManager.PK_NAME);
      }
    }

    if (ammd.hasExtension(DatastoreManager.PK_ID)) {
      if (!ammd.getType().equals(Long.class) && !ammd.getType().equals(long.class)) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ExtensionForLongField",
            ammd.getFullFieldName(), DatastoreManager.PK_ID);
      }
    }

    if (ammd.hasExtension(DatastoreManager.PARENT_PK)) {
      if (noParentAllowed) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.PKAndParentPKInvalid",
            ammd.getFullFieldName(), pkClass.getName());
      }
      if (!ammd.getType().equals(String.class) && !ammd.getType().equals(Key.class)) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ParentPKType",
            ammd.getFullFieldName());
      }
    }

    for (String extension : NOT_PRIMARY_KEY_EXTENSIONS) {
      if (ammd.hasExtension(extension) && ammd.isPrimaryKey()) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.FieldWithExtensionNotPK",
            ammd.getFullFieldName(), extension);
      }
    }

    // pk-name and pk-id only supported in conjunction with an encoded string
    if (pkMemberMetaData != null) {
      for (String extension : REQUIRES_ENCODED_STRING_PK_EXTENSIONS) {
        if (ammd.hasExtension(extension)) {
          if (!pkMemberMetaData.hasExtension(DatastoreManager.ENCODED_PK)) {
            // we've already verified that encoded-pk is on a a String pk field
            // so we don't need to check the type of the pk here.
            throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.FieldWithExtensionForEncodedString",
                ammd.getFullFieldName(), extension);
          }
        }
      }
    }

    if (ammd.hasCollection() && ammd.getCollection().isSerializedElement()) {
      throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.CollectionWithSerializedElementInvalid", 
          ammd.getFullFieldName());
    }
    else if (ammd.hasArray() && ammd.getArray().isSerializedElement()) {
      throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ArrayWithSerializedElementInvalid", 
          ammd.getFullFieldName());
    }


    checkForIllegalChildField(ammd, noParentAllowed);

    if (ammd.getRelationType(clr) != RelationType.NONE) {
      // Look for "eager" relationships.  Not supported but not necessarily an error
      // since we can always fall back to "lazy."
      if (ammd.isDefaultFetchGroup() && !ammd.isEmbedded()) {
          warn(String.format(
              "Meta-data warning for %s.%s: %s  %s  %s",
              acmd.getFullClassName(), ammd.getName(), GAE_LOCALISER.msg("AppEngine.MetaData.JoinsNotSupported", ammd.getFullFieldName()), "The field will be fetched lazily on first access.", ADJUST_WARNING_MSG));
//        handleIgnorableMapping(acmd, ammd, "AppEngine.MetaData.JoinsNotSupported", "The field will be fetched lazily on first access.");
      }

      if (ammd.getRelationType(clr) == RelationType.MANY_TO_MANY_BI && MetaDataUtils.isOwnedRelation(ammd, storeMgr)) {
        // We only support many-to-many for unowned relations
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ManyToManyRelationNotSupported",
            ammd.getFullFieldName());
      }

      RelationType relType = ammd.getRelationType(clr);
      if (ammd.getEmbeddedMetaData() == null &&
          (relType == RelationType.ONE_TO_ONE_UNI || relType == RelationType.ONE_TO_ONE_BI ||
           relType == RelationType.ONE_TO_MANY_UNI || relType == RelationType.ONE_TO_MANY_BI) &&
          !getBooleanConfigProperty(ALLOW_MULTIPLE_RELATIONS_OF_SAME_TYPE) &&
          !storeMgr.storageVersionAtLeast(StorageVersion.READ_OWNED_CHILD_KEYS_FROM_PARENTS)) {
        // Check on multiple relations of the same type for early storage versions
        Class<?> relationClass;
        if (ammd.getCollection() != null) {
          relationClass = clr.classForName(ammd.getCollection().getElementType());
        } else if (ammd.getArray() != null) {
          relationClass = clr.classForName(ammd.getArray().getElementType());
        } else {
          relationClass = clr.classForName(ammd.getTypeName());
        }

        // Add the actual type of the field to the list of types that can't
        // repeat.  If that type was already present, problem.
        for (Class<?> existingRelationClass : nonRepeatableRelationTypes.keySet()) {
          if (existingRelationClass.isAssignableFrom(relationClass) ||
              relationClass.isAssignableFrom(existingRelationClass)) {
            throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ClassWithMultipleFieldsOfType",
                acmd.getFullClassName(), relationClass.getName(), ammd.getName(), nonRepeatableRelationTypes.get(existingRelationClass));
          }
        }
        nonRepeatableRelationTypes.put(relationClass, ammd.getName());
      }
    }

    if (ammd.getValueGeneratorName() != null) {
      SequenceMetaData sequenceMetaData = metaDataManager.getMetaDataForSequence(clr, ammd.getValueGeneratorName());
      if (sequenceMetaData != null && sequenceMetaData.getInitialValue() != 1) {
        handleIgnorableMapping(acmd, ammd, "AppEngine.MetaData.SequenceInitialSizeNotSupported",
            "The first value for this sequence will be 1.");
      }
    }
  }

  private void checkForIllegalChildField(AbstractMemberMetaData ammd, boolean noParentAllowed) {
    if (!MetaDataUtils.isOwnedRelation(ammd, storeMgr)) {
      // The check only applies to owned relations
      return;
    }

    // Figure out if this field is the owning side of a one to one or a one to
    // many.  If it is, look at the mapping of the child class and make sure their
    // pk isn't Long or unencoded String.
    RelationType relationType = ammd.getRelationType(clr);
    if (relationType == RelationType.NONE || ammd.isEmbedded()) {
      return;
    }
    AbstractClassMetaData childAcmd = null;
    if (relationType == RelationType.ONE_TO_MANY_BI || relationType == RelationType.ONE_TO_MANY_UNI) {
      if (ammd.getCollection() != null) {
        childAcmd = ammd.getCollection().getElementClassMetaData(clr, metaDataManager);
      } else if (ammd.getArray() != null) {
        childAcmd = ammd.getArray().getElementClassMetaData(clr, metaDataManager);
      } else {
        // don't know how to verify
        NucleusLogger.METADATA.warn("Unable to validate one-to-many relation " + ammd.getFullFieldName());
      }
      if (ammd.getOrderMetaData() != null) {
        verifyOneToManyOrderBy(ammd, childAcmd);
      }
    } else if (relationType == RelationType.ONE_TO_ONE_BI || relationType == RelationType.ONE_TO_ONE_UNI) {
      childAcmd = metaDataManager.getMetaDataForClass(ammd.getType(), clr);
    }
    if (childAcmd == null) {
      return;
    }

    // Get the type of the primary key of the child
    if (childAcmd.getIdentityType() == IdentityType.DATASTORE) {
      Class pkType = Long.class;
      ColumnMetaData colmd = childAcmd.getIdentityMetaData().getColumnMetaData();
      if (colmd != null && 
          ("varchar".equalsIgnoreCase(colmd.getJdbcType()) || "char".equalsIgnoreCase(colmd.getJdbcType()))) {
        pkType = String.class;
      }
      if (noParentAllowed && pkType.equals(Long.class)) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ChildWithPKTypeInvalid",
            childAcmd.getFullClassName()+".[ID]", pkType.getName(), ammd.getFullFieldName());
      }
    } else {
      int[] pkPositions = childAcmd.getPKMemberPositions();
      if (pkPositions == null) {
        // don't know how to verify
        NucleusLogger.METADATA.warn("Unable to validate relation " + ammd.getFullFieldName());
        return;
      }
      int pkPos = pkPositions[0];
      AbstractMemberMetaData pkMemberMetaData = childAcmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPos);
      Class<?> pkType = pkMemberMetaData.getType();
      if (noParentAllowed && (pkType.equals(Long.class) || pkType.equals(long.class) ||
          (pkType.equals(String.class) && !pkMemberMetaData.hasExtension(DatastoreManager.ENCODED_PK)))) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ChildWithPKTypeInvalid",
            pkMemberMetaData.getFullFieldName(), pkType.getName(), ammd.getFullFieldName());
      }
    }
  }

  private void verifyOneToManyOrderBy(AbstractMemberMetaData ammd, AbstractClassMetaData childAcmd) {
    OrderMetaData omd = ammd.getOrderMetaData();
    OrderMetaData.FieldOrder[] fieldOrders = omd.getFieldOrders();
    if (fieldOrders == null) {
      return;
    }
    for (OrderMetaData.FieldOrder fieldOrder : omd.getFieldOrders()) {
      String propertyName = fieldOrder.getFieldName();
      AbstractMemberMetaData orderField = childAcmd.getMetaDataForMember(propertyName);
      if (orderField.hasExtension(DatastoreManager.PK_ID) ||
          orderField.hasExtension(DatastoreManager.PK_NAME)) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.OrderPartOfPK",
            ammd.getFullFieldName(), propertyName);
      }
    }
  }

  private static boolean hasIdentityStrategy(IdentityStrategy strat, AbstractMemberMetaData ammd) {
    return ammd.getValueStrategy() != null && ammd.getValueStrategy().equals(strat);
  }

  private boolean getBooleanConfigProperty(String configProperty) {
    return metaDataManager.getNucleusContext().getPersistenceConfiguration().getBooleanProperty(configProperty);
  }

  private IgnorableMetaDataBehavior getIgnorableMetaDataBehavior() {
    return IgnorableMetaDataBehavior.valueOf(
        metaDataManager.getNucleusContext().getStoreManager()
            .getStringProperty(IGNORABLE_META_DATA_BEHAVIOR_PROPERTY), IgnorableMetaDataBehavior.WARN);
  }

  void handleIgnorableMapping(AbstractClassMetaData acmd, AbstractMemberMetaData ammd, String localiserKey, String warningOnlyMsg) {
    switch (getIgnorableMetaDataBehavior()) {
      case WARN:
        if (ammd == null) {
          warn(String.format(
              "Meta-data warning for %s: %s  %s  %s",
              acmd.getFullClassName(), GAE_LOCALISER.msg(localiserKey), warningOnlyMsg, ADJUST_WARNING_MSG));          
        } else {
          warn(String.format(
              "Meta-data warning for %s.%s: %s  %s  %s",
              acmd.getFullClassName(), ammd.getName(), GAE_LOCALISER.msg(localiserKey, ammd.getFullFieldName()), warningOnlyMsg, ADJUST_WARNING_MSG));
        }
        break;
      case ERROR:
        if (ammd == null) {
          throw new InvalidMetaDataException(GAE_LOCALISER, localiserKey, acmd.getFullClassName());
        }
        throw new InvalidMetaDataException(GAE_LOCALISER, localiserKey, ammd.getFullFieldName());
      case NONE:
        // Do nothing
    }
  }

  // broken out for testing
  void warn(String msg) {
    NucleusLogger.METADATA.warn(msg);
  }
}
