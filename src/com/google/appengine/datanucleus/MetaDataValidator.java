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
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.InvalidMetaDataException;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.OrderMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.metadata.SequenceMetaData;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import java.util.Map;
import java.util.Set;

/**
 * AppEngine-specific rules validator for Meta Data.
 * Can also be used as a DataNucleus MetaDataListener to listen for when metadata is loaded and validate
 * it at source.
 *
 * @author Max Ross <maxr@google.com>
 */
public class MetaDataValidator implements MetaDataListener {
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
  private static final String
      IGNORABLE_META_DATA_BEHAVIOR_PROPERTY = "datanucleus.appengine.ignorableMetaDataBehavior";

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

  private static final Set<Integer> NON_REPEATABLE_RELATION_TYPES = Utils.newHashSet(
      Relation.ONE_TO_MANY_BI,
      Relation.ONE_TO_MANY_UNI,
      Relation.ONE_TO_ONE_BI,
      Relation.ONE_TO_ONE_UNI
  );

  private static final String ALLOW_MULTIPLE_RELATIONS_OF_SAME_TYPE =
      "datanucleus.appengine.allowMultipleRelationsOfSameType";

  private final MetaDataManager metaDataManager;
  private final ClassLoaderResolver clr;

  public MetaDataValidator(MetaDataManager metaDataManager, ClassLoaderResolver clr) {
    this.metaDataManager = metaDataManager;
    this.clr = clr;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.metadata.MetaDataListener#loaded(org.datanucleus.metadata.AbstractClassMetaData)
   */
  public void loaded(AbstractClassMetaData cmd) {
    validate(cmd);
  }

  /**
   * validate the metadata for the provided class.
   * @param acmd Metadata for the class to validate
   */
  public void validate(AbstractClassMetaData acmd) {
    NucleusLogger.METADATA.info("Performing appengine-specific metadata validation for " + acmd.getFullClassName());
    if (acmd.isEmbeddedOnly()) {
      // Nothing to check
      return;
    }

    // validate inheritance
    // TODO Put checks on supported inheritance here

    // Validate primary-key
    int[] pkPositions = acmd.getPKMemberPositions();
    if (pkPositions.length != 1) {
      throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.CompositePKNotSupported", acmd.getName());
    }
    int pkPos = pkPositions[0];
    AbstractMemberMetaData pkMemberMetaData = acmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPos);

    // TODO Allow long, int, Integer types
    Class<?> pkType = pkMemberMetaData.getType();
    boolean noParentAllowed = false;
    if (pkType.equals(Long.class)) {
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

    // Validate fields
    Set<String> foundOneOrZeroExtensions = Utils.newHashSet();
    Map<Class<?>, String> nonRepeatableRelationTypes = Utils.newHashMap();
    Class<?> pkClass = pkMemberMetaData.getType();

    // the constraints that we check across all fields apply to the entire
    // persistent class hierarchy so we're going to validate every field
    // at every level of the hierarchy.  As an example, this lets us detect
    // multiple one-to-many relationships at different levels of the class hierarchy
    AbstractClassMetaData curCmd = acmd;
    do {
      for (AbstractMemberMetaData ammd : curCmd.getManagedMembers()) {
        validateField(acmd, pkMemberMetaData, noParentAllowed, pkClass, foundOneOrZeroExtensions, nonRepeatableRelationTypes, ammd);
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
      if (!ammd.getType().equals(Long.class)) {
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

    checkForIllegalChildField(ammd, noParentAllowed);

    if (ammd.getRelationType(clr) != Relation.NONE) {
      // Look for "eager" relationships.  Not supported but not necessarily an error
      // since we can always fall back to "lazy."
      if (ammd.isDefaultFetchGroup() && !ammd.isEmbedded()) {
        handleIgnorableMapping(acmd, ammd, "AppEngine.MetaData.JoinsNotSupported", "The field will be fetched lazily on first access.");
      }

      // We don't support many-to-many at all
      if (ammd.getRelationType(clr) == Relation.MANY_TO_MANY_BI) {
        throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ManyToManyRelationNotSupported",
            ammd.getFullFieldName());
      } else if (ammd.getEmbeddedMetaData() == null &&
                 NON_REPEATABLE_RELATION_TYPES.contains(ammd.getRelationType(clr)) &&
                 !getBooleanConfigProperty(ALLOW_MULTIPLE_RELATIONS_OF_SAME_TYPE)) {
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
    // Figure out if this field is the owning side of a one to one or a one to
    // many.  If it is, look at the mapping of the child class and make sure their
    // pk isn't Long or unencoded String.
    int relationType = ammd.getRelationType(clr);
    if (relationType == Relation.NONE || ammd.isEmbedded()) {
      return;
    }
    AbstractClassMetaData childAcmd = null;
    if (relationType == Relation.ONE_TO_MANY_BI || relationType == Relation.ONE_TO_MANY_UNI) {
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
    } else if (relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI) {
      childAcmd = metaDataManager.getMetaDataForClass(ammd.getType(), clr);
    }
    if (childAcmd == null) {
      return;
    }

    // Get the type of the primary key of the child
    int[] pkPositions = childAcmd.getPKMemberPositions();
    if (pkPositions == null) {
      // don't know how to verify
      NucleusLogger.METADATA.warn("Unable to validate relation " + ammd.getFullFieldName());
      return;
    }
    int pkPos = pkPositions[0];
    AbstractMemberMetaData pkMemberMetaData = childAcmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPos);
    Class<?> pkType = pkMemberMetaData.getType();
    if (noParentAllowed && (pkType.equals(Long.class) ||
        (pkType.equals(String.class) && !pkMemberMetaData.hasExtension(DatastoreManager.ENCODED_PK)))) {
      throw new InvalidMetaDataException(GAE_LOCALISER, "AppEngine.MetaData.ChildWithPKTypeInvalid",
          pkMemberMetaData.getFullFieldName(), pkType.getName(), ammd.getFullFieldName());
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
    return metaDataManager.getNucleusContext().getPersistenceConfiguration()
        .getBooleanProperty(configProperty);
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
      // We swallow both null and NONE
    }
  }

  // broken out for testing
  void warn(String msg) {
    NucleusLogger.METADATA.warn(msg);
  }
}
