/**********************************************************************
Copyright (c) 2002 Mike Martin (TJDO) and others. All rights reserved. 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 

Contributors:
2003 Erik Bengtson - Important changes regarding the application identity
                     support when fetching PreparedStatements. Added an
                     inner class that should be moved away.
2004 Erik Bengtson - added commentary and Localisation
2004 Andy Jefferson - added capability to handle Abstract PC fields for
                      both SingleFieldIdentity and AID
2007 Andy Jefferson - implement RelationMappingCallbacks
    ...
**********************************************************************/
package org.datanucleus.store.mapped.mapping;

import java.util.Collection;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.spi.PersistenceCapable;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.exceptions.NotYetFlushedException;
import org.datanucleus.store.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.fieldmanager.SingleValueFieldManager;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.scostore.PersistableRelationStore;
import org.datanucleus.store.types.SCOCollection;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Maps a field storing a persistable object.
 * TODO Move column creation for join table collection/map/array of PC to this class
 */
public class PersistableMapping extends MultiMapping implements MappingCallbacks
{
    //----------------------- convenience fields to improve performance ----------------------// 
    /** ClassMetaData for the represented class. Create a new one on each getObject invoke is expensive **/
    protected AbstractClassMetaData cmd;

    /**
     * Create a new empty PersistableMapping.
     * The caller must call one of the initialize methods to initialize the instance with the
     * DatastoreAdapter and its type.
     */
    public PersistableMapping()
    {
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.JavaTypeMapping#getJavaType()
     */
    @Override
    public Class getJavaType()
    {
        return null;
    }

    /**
     * Initialize this JavaTypeMapping with the given DatastoreAdapter for the given metadata.
     * @param mmd MetaData for the field/property to be mapped (if any)
     * @param container The datastore container storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     */
    public void initialize(AbstractMemberMetaData mmd, DatastoreContainerObject container, 
            ClassLoaderResolver clr)
    {
    	super.initialize(mmd, container, clr);

    	prepareDatastoreMapping(clr);
    }

	/**
     * Method to prepare the PC mapping and add its associated datastore mappings.
     * @param clr The ClassLoaderResolver
     */
    protected void prepareDatastoreMapping(ClassLoaderResolver clr)
    {
        if (roleForMember == FieldRole.ROLE_COLLECTION_ELEMENT)
        {
            // TODO Handle creation of columns in join table for collection of PCs
        }
        else if (roleForMember == FieldRole.ROLE_ARRAY_ELEMENT)
        {
            // TODO Handle creation of columns in join table for array of PCs
        }
        else if (roleForMember == FieldRole.ROLE_MAP_KEY)
        {
            // TODO Handle creation of columns in join table for map of PCs as keys
        }
        else if (roleForMember == FieldRole.ROLE_MAP_VALUE)
        {
            // TODO Handle creation of columns in join table for map of PCs as values
        }
        else
        {
            // Either one end of a 1-1 relation, or the N end of a N-1
            AbstractClassMetaData refCmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            JavaTypeMapping referenceMapping = null;
            if (refCmd == null)
            {
                // User stupidity
                throw new NucleusUserException("You have a field " + mmd.getFullFieldName() + " that has type " + mmd.getTypeName() +
                    " but this type has no known metadata. Your mapping is incorrect");
            }
            if (refCmd.getInheritanceMetaData() != null &&
                refCmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
            {
                // Find the actual tables storing the other end (can be multiple subclasses)
                AbstractClassMetaData[] cmds = storeMgr.getClassesManagingTableForClass(refCmd, clr);
                if (cmds != null && cmds.length > 0)
                {
                    if (cmds.length > 1)
                    {
                        NucleusLogger.PERSISTENCE.warn("Field " + mmd.getFullFieldName() + " represents either a 1-1 relation, " +
                            "or a N-1 relation where the other end uses \"subclass-table\" inheritance strategy and more " +
                        "than 1 subclasses with a table. This is not fully supported");
                    }
                }
                else
                {
                    // No subclasses of the class using "subclasses-table" so no mapping!
                    // TODO Throw an exception ?
                    return;
                }
                // TODO We need a mapping for each of the possible subclass tables
                referenceMapping = storeMgr.getDatastoreClass(cmds[0].getFullClassName(), clr).getIdMapping();
            }
            else
            {
                referenceMapping = storeMgr.getDatastoreClass(mmd.getType().getName(), clr).getIdMapping();
            }

            // Generate a mapping from the columns of the referenced object to this mapping's ColumnMetaData
            CorrespondentColumnsMapper correspondentColumnsMapping = new CorrespondentColumnsMapper(mmd, referenceMapping, true);

            // Find any related field where this is part of a bidirectional relation
            RelationType relationType = mmd.getRelationType(clr);
            boolean createDatastoreMappings = true;
            if (relationType == RelationType.MANY_TO_ONE_BI)
            {
                AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                // TODO Cater for more than 1 related field
                createDatastoreMappings = (relatedMmds[0].getJoinMetaData() == null);
            }
            else if (relationType == RelationType.ONE_TO_ONE_BI)
            {
                // Put the FK at the end without "mapped-by"
                createDatastoreMappings = (mmd.getMappedBy() == null);
            }

            if (relationType == RelationType.MANY_TO_ONE_UNI)
            {
                // create join table
                storeMgr.newJoinDatastoreContainerObject(mmd, clr);
            }
            else
            {
                // Loop through the datastore fields in the referenced class and create a datastore field for each
                for (int i=0; i<referenceMapping.getNumberOfDatastoreMappings(); i++)
                {
                    DatastoreMapping refDatastoreMapping = referenceMapping.getDatastoreMapping(i);
                    JavaTypeMapping mapping = storeMgr.getMappingManager().getMapping(refDatastoreMapping.getJavaTypeMapping().getJavaType());
                    this.addJavaTypeMapping(mapping);

                    // Create physical datastore columns where we require a FK link to the related table.
                    if (createDatastoreMappings)
                    {
                        // Find the Column MetaData that maps to the referenced datastore field
                        ColumnMetaData colmd = correspondentColumnsMapping.getColumnMetaDataByIdentifier(
                            refDatastoreMapping.getDatastoreField().getIdentifier());
                        if (colmd == null)
                        {
                            throw new NucleusUserException(LOCALISER.msg("041038",
                                refDatastoreMapping.getDatastoreField().getIdentifier(), toString())).setFatal();
                        }

                        // Create a Datastore field to equate to the referenced classes datastore field
                        MappingManager mmgr = storeMgr.getMappingManager();
                        DatastoreField col = mmgr.createDatastoreField(mmd, datastoreContainer, mapping, 
                            colmd, refDatastoreMapping.getDatastoreField(), clr);

                        // Add its datastore mapping
                        DatastoreMapping datastoreMapping = mmgr.createDatastoreMapping(mapping, col, refDatastoreMapping.getJavaTypeMapping().getJavaTypeForDatastoreMapping(i));
                        this.addDatastoreMapping(datastoreMapping);
                    }
                    else
                    {
                        mapping.setReferenceMapping(referenceMapping);
                    }
                }
            }
        }
    }

    /**
     * Method to return the value to be stored in the specified datastore index given the overall
     * value for this java type.
     * @param nucleusCtx Context
     * @param index The datastore index
     * @param value The overall value for this java type
     * @return The value for this datastore index
     */
    public Object getValueForDatastoreMapping(NucleusContext nucleusCtx, int index, Object value)
    {
        ExecutionContext ec = nucleusCtx.getApiAdapter().getExecutionContext(value);
        if (cmd == null)
        {
            cmd = nucleusCtx.getMetaDataManager().getMetaDataForClass(getType(), 
                ec != null ? ec.getClassLoaderResolver() : nucleusCtx.getClassLoaderResolver(null));
        }

        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            AbstractMemberMetaData mmd =
                cmd.getMetaDataForManagedMemberAtAbsolutePosition(cmd.getPKMemberPositions()[index]);
            ObjectProvider sm = null;
            if (ec != null)
            {
                sm = ec.findObjectProvider(value);
            }

            if (sm == null)
            {
                // Transient or detached maybe, so use reflection to get PK field values
                if (mmd instanceof FieldMetaData)
                {
                    return ClassUtils.getValueOfFieldByReflection(value, mmd.getName());
                }
                else
                {
                    return ClassUtils.getValueOfMethodByReflection(value,
                        ClassUtils.getJavaBeanGetterName(mmd.getName(), false), (Object[])null);
                }
            }

            if (!mmd.isPrimaryKey())
            {
                // Make sure the field is loaded
                sm.isLoaded(mmd.getAbsoluteFieldNumber());
            }
            FieldManager fm = new SingleValueFieldManager();
            sm.provideFields(new int[] {mmd.getAbsoluteFieldNumber()}, fm);
            return fm.fetchObjectField(mmd.getAbsoluteFieldNumber());
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            OID oid = (OID)nucleusCtx.getApiAdapter().getIdForObject(value);
            return oid != null ? oid.getKeyValue() : null;
        }
        return null;
    }

    /**
	 * Method to set an object in the datastore.
	 * @param ec The ExecutionContext
	 * @param ps The Prepared Statement
	 * @param param The parameter ids in the statement
	 * @param value The value to put in the statement at these ids
	 * @throws NotYetFlushedException
	 */
    public void setObject(ExecutionContext ec, Object ps, int[] param, Object value)
    {
        setObject(ec, ps, param, value, null, -1);
    }

    /**
     * Method to set an object reference (FK) in the datastore.
     * @param ec The ExecutionContext
     * @param ps The Prepared Statement
     * @param param The parameter ids in the statement
     * @param value The value to put in the statement at these ids
     * @param ownerSM StateManager for the owner object
     * @param ownerFieldNumber Field number of this PC object in the owner
     * @throws NotYetFlushedException
     */
    public void setObject(ExecutionContext ec, Object ps, int[] param, Object value, ObjectProvider ownerSM, int ownerFieldNumber)
    {
        if (value == null)
        {
            setObjectAsNull(ec, ps, param);
        }
        else
        {
            setObjectAsValue(ec, ps, param, value, ownerSM, ownerFieldNumber);
        }
    }

    /**
     * Populates the PreparedStatement with a null value for the mappings of this PersistenceCapableMapping.
     * @param ec ExecutionContext
     * @param ps the Prepared Statement
     * @param param The parameter ids in the statement
     */
    private void setObjectAsNull(ExecutionContext ec, Object ps, int[] param)
    {
        // Null out the PC object
        int n=0;
        for (int i=0; i<javaTypeMappings.length; i++)
        {
            JavaTypeMapping mapping = javaTypeMappings[i];
            if (mapping.getNumberOfDatastoreMappings() > 0)
            {
                // Only populate the PreparedStatement for the object if it has any datastore mappings
                int[] posMapping = new int[mapping.getNumberOfDatastoreMappings()];
                for (int j=0; j<posMapping.length; j++)
                {
                    posMapping[j] = param[n++];
                }
                mapping.setObject(ec, ps, posMapping, null);
            }
        }
    }

    /**
     * Check if one of the primary key fields of the PC has value attributed by the datastore
     * @param mdm the {@link MetaDataManager}
     * @param srm the {@link StoreManager}
     * @param clr the {@link ClassLoaderResolver}
     * @return true if one of the primary key fields of the PC has value attributed by the datastore
     */
    private boolean hasDatastoreAttributedPrimaryKeyValues(MetaDataManager mdm, StoreManager srm, ClassLoaderResolver clr)
    {
        boolean hasDatastoreAttributedPrimaryKeyValues = false;
        if (this.mmd != null)
        {
            if (roleForMember != FieldRole.ROLE_ARRAY_ELEMENT &&
                roleForMember != FieldRole.ROLE_COLLECTION_ELEMENT &&
                roleForMember != FieldRole.ROLE_MAP_KEY &&
                roleForMember != FieldRole.ROLE_MAP_VALUE)
            {
                // Object is associated to a field (i.e not a join table)
                AbstractClassMetaData acmd = mdm.getMetaDataForClass(this.mmd.getType(), clr);
                if (acmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    for (int i=0; i<acmd.getPKMemberPositions().length; i++)
                    {
                        IdentityStrategy strategy = 
                            acmd.getMetaDataForManagedMemberAtAbsolutePosition(acmd.getPKMemberPositions()[i]).getValueStrategy();
                        if (strategy != null)
                        {
                            //if strategy is null, then it's user attributed value
                            hasDatastoreAttributedPrimaryKeyValues |= srm.isStrategyDatastoreAttributed(acmd, acmd.getPKMemberPositions()[i]);
                        }
                    }
                }
            }
        }
        return hasDatastoreAttributedPrimaryKeyValues;
    }
    
    /**
     * Method to set an object reference (FK) in the datastore.
     * @param ec The ExecutionContext
     * @param ps The Prepared Statement
     * @param param The parameter ids in the statement
     * @param value The value to put in the statement at these ids
     * @param ownerSM StateManager for the owner object
     * @param ownerFieldNumber Field number of this PC object in the owner
     * @throws NotYetFlushedException Just put "null" in and throw "NotYetFlushedException", 
     *                                to be caught by ParameterSetter and will signal to the PC object being inserted
     *                                that it needs to inform this object when it is inserted.
     */
    private void setObjectAsValue(ExecutionContext ec, Object ps, int[] param, Object value, ObjectProvider ownerSM, 
            int ownerFieldNumber)
    {
        Object id;

        ApiAdapter api = ec.getApiAdapter();
        if (!api.isPersistable(value))
        {
            throw new NucleusException(LOCALISER.msg("041016", value.getClass(), value)).setFatal();
        }

        ObjectProvider valueSM = ec.findObjectProvider(value);

        try
        {
            ClassLoaderResolver clr = ec.getClassLoaderResolver();

            // Check if the field is attributed in the datastore
            boolean hasDatastoreAttributedPrimaryKeyValues = hasDatastoreAttributedPrimaryKeyValues(
                ec.getMetaDataManager(), storeMgr, clr);

            boolean inserted = false;
            if (ownerFieldNumber >= 0)
            {
                // Field mapping : is this field of the related object present in the datastore?
                inserted = storeMgr.isObjectInserted(valueSM, ownerFieldNumber);
            }
            else if (mmd == null)
            {
                // Identity mapping : is the object inserted far enough to be considered of this mapping type?
                inserted = storeMgr.isObjectInserted(valueSM, type);
            }

            if (valueSM != null)
            {
                if (ec.getApiAdapter().isDetached(value) && valueSM.getReferencedPC() != null && ownerSM != null && mmd != null)
                {
                    // Still detached but started attaching so replace the field with what will be the attached
                    // Note that we have "fmd != null" here hence omitting any M-N relations where this is a join table 
                    // mapping
                    ownerSM.replaceFieldMakeDirty(ownerFieldNumber, valueSM.getReferencedPC());
                }

                if (valueSM.isWaitingToBeFlushedToDatastore())
                {
                    // Related object is not yet flushed to the datastore so flush it so we can set the FK
                    valueSM.flush();
                }
            }
            else
            {
                if (ec.getApiAdapter().isDetached(value))
                {
                    // Field value is detached and not yet started attaching, so attach
                    Object attachedValue = ec.persistObjectInternal(value, null, -1, ObjectProvider.PC);
                    if (attachedValue != value && ownerSM != null)
                    {
                        // Replace the field value if using copy-on-attach
                        ownerSM.replaceFieldMakeDirty(ownerFieldNumber, attachedValue);
                    }
                }
            }

            // we can execute this block when
            // 1) the pc has been inserted; OR
            // 2) is not in process of being inserted; OR
            // 3) is being inserted yet is inserted enough to use this mapping; OR
            // 4) the PC PK values are not attributed by the database and this mapping is for a PK field (compound identity)
            // 5) the value is the same object as we are inserting anyway and has its identity set
            if (inserted || !ec.isInserting(value) ||
                (!hasDatastoreAttributedPrimaryKeyValues && (this.mmd != null && this.mmd.isPrimaryKey())) ||
                (!hasDatastoreAttributedPrimaryKeyValues && ownerSM == valueSM && api.getIdForObject(value) != null))
            {
                // The PC is either already inserted, or inserted down to the level we need, or not inserted at all,
                // or the field is a PK and identity not attributed by the datastore

                // Object either already exists, or is not yet being inserted.
                id = api.getIdForObject(value);

                // Check if the PersistenceCapable exists in this datastore
                boolean requiresPersisting = false;
                if (ec.getApiAdapter().isDetached(value) && ownerSM != null)
                {
                    // Detached object so needs attaching
                    if (ownerSM.isInserting())
                    {
                        // Inserting other object, and this object is detached but if detached from this datastore
                        // we can just return the value now and attach later (in InsertRequest)
                        if (!ec.getNucleusContext().getPersistenceConfiguration().getBooleanProperty("datanucleus.attachSameDatastore"))
                        {
                            if (ec.getObjectFromCache(api.getIdForObject(value)) != null)
                            {
                                // Object is in cache so exists for this datastore, so no point checking
                            }
                            else
                            {
                                try
                                {
                                    Object obj = ec.findObject(api.getIdForObject(value), true, false,
                                        value.getClass().getName());
                                    if (obj != null)
                                    {
                                        // Make sure this object is not retained in cache etc
                                        ObjectProvider objSM = ec.findObjectProvider(obj);
                                        if (objSM != null)
                                        {
                                            ec.evictFromTransaction(objSM);
                                        }
                                        ec.removeObjectFromLevel1Cache(api.getIdForObject(value));
                                    }
                                }
                                catch (NucleusObjectNotFoundException onfe)
                                {
                                    // Object doesnt yet exist
                                    requiresPersisting = true;
                                }
                            }
                        }
                    }
                    else
                    {
                        requiresPersisting = true;
                    }
                }
                else if (id == null)
                {
                    // Transient object, so we need to persist it
                    requiresPersisting = true;
                }
                else
                {
                    ExecutionContext pcEC = ec.getApiAdapter().getExecutionContext(value);
                    if (pcEC != null && ec != pcEC)
                    {
                        throw new NucleusUserException(LOCALISER.msg("041015"), id);
                    }
                }

                if (requiresPersisting)
                {
                    // PERSISTENCE-BY-REACHABILITY
                    // This PC object needs persisting (new or detached) to do the "set"
                    if (mmd != null && !mmd.isCascadePersist() && !ec.getApiAdapter().isDetached(value))
                    {
                        // Related PC object not persistent, but cant do cascade-persist so throw exception
                        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                        {
                            NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("007006", 
                                mmd.getFullFieldName()));
                        }
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
                    }

                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("007007", 
                            mmd != null ? mmd.getFullFieldName() : null));
                    }

                    try
                    {
                        Object pcNew = ec.persistObjectInternal(value, null, -1, ObjectProvider.PC);
                        if (hasDatastoreAttributedPrimaryKeyValues)
                        {
                            ec.flushInternal(false);
                        }
                        id = api.getIdForObject(pcNew);
                        if (ec.getApiAdapter().isDetached(value) && ownerSM != null)
                        {
                            // Update any detached reference to refer to the attached variant
                            ownerSM.replaceFieldMakeDirty(ownerFieldNumber, pcNew);
                            RelationType relationType = mmd.getRelationType(clr);
                            if (relationType == RelationType.MANY_TO_ONE_BI)
                            {
                                // TODO Update the container to refer to the attached object
                                if (NucleusLogger.PERSISTENCE.isInfoEnabled())
                                {
                                    NucleusLogger.PERSISTENCE.info("PCMapping.setObject : object " + ownerSM.getInternalObjectId() + 
                                        " has field " + ownerFieldNumber + " that is 1-N bidirectional." + 
                                        " Have just attached the N side so should really update the reference in the 1 side collection" +
                                        " to refer to this attached object. Not yet implemented");
                                }
                            }
                            else if (relationType == RelationType.ONE_TO_ONE_BI)
                            {
                                AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
                                // TODO Cater for more than 1 related field
                                ObjectProvider relatedSM = ec.findObjectProvider(pcNew);
                                relatedSM.replaceFieldMakeDirty(relatedMmds[0].getAbsoluteFieldNumber(), ownerSM.getObject());
                            }
                        }
                    }
                    catch (NotYetFlushedException e)
                    {
                        setObjectAsNull(ec, ps, param);
                        throw new NotYetFlushedException(value);
                    }
                }

                if (valueSM != null)
                {
                    valueSM.setStoringPC();
                }

                // If the field doesn't map to any datastore fields (e.g remote FK), omit the set process
                if (getNumberOfDatastoreMappings() > 0)
                {
                    if (id instanceof OID)
                    {
                        OID oid = (OID)id;
                        try
                        {
                            // Try as a Long
                            getDatastoreMapping(0).setObject(ps, param[0], oid.getKeyValue());
                        }
                        catch (Exception e)
                        {
                            // Must be a String
                            getDatastoreMapping(0).setObject(ps, param[0], oid.getKeyValue().toString());
                        }
                    }
                    else
                    {
                        boolean fieldsSet = false;
                        if (api.isSingleFieldIdentity(id) && javaTypeMappings.length > 1)
                        {
                            Object key = api.getTargetKeyForSingleFieldIdentity(id);
                            AbstractClassMetaData keyCmd = ec.getMetaDataManager().getMetaDataForClass(key.getClass(), clr);
                            if (keyCmd != null && keyCmd.getIdentityType() == IdentityType.NONDURABLE)
                            {
                                // Embedded ID - Make sure these are called starting at lowest first, in order
                                // We cannot just call OP.provideFields with all fields since that does last first
                                ObjectProvider keyOP = ec.findObjectProvider(key);
                                int[] fieldNums = keyCmd.getAllMemberPositions();
                                FieldManager fm = new AppIDObjectIdFieldManager(param, ec, ps, javaTypeMappings);
                                for (int i=0;i<fieldNums.length;i++)
                                {
                                    keyOP.provideFields(new int[] {fieldNums[i]}, fm);
                                }
                                fieldsSet = true;
                            }
                        }

                        if (!fieldsSet)
                        {
                            // TODO Factor out this PersistenceCapable reference
                            ((PersistenceCapable)value).jdoCopyKeyFieldsFromObjectId(
                                new AppIDObjectIdFieldManager(param, ec, ps, javaTypeMappings), id);
                        }
                    }
                }
            }
            else
            {
                if (valueSM != null)
                {
                    valueSM.setStoringPC();
                }

                if (getNumberOfDatastoreMappings() > 0)
                {
                    // Object is in the process of being inserted so we cant use its id currently and we need to store
                    // a foreign key to it (which we cant yet do). Just put "null" in and throw "NotYetFlushedException",
                    // to be caught by ParameterSetter and will signal to the PC object being inserted that it needs 
                    // to inform this object when it is inserted.
                    setObjectAsNull(ec, ps, param);
                    throw new NotYetFlushedException(value);
                }
            }
        }
        finally
        {
            if (valueSM != null)
            {
                valueSM.unsetStoringPC();
            }
        }
    }

    /**
     * Returns an instance of a persistable class.
     * Processes the FK field and generates the id of the object from the result values, and hence the object itself.
     * @param ec execution context
     * @param rs The ResultSet
     * @param resultIndexes indexes in the ResultSet to retrieve
     * @return The persistable object
     */
    public Object getObject(ExecutionContext ec, final Object rs, int[] resultIndexes)
    {
        // Check for null FK
        if (storeMgr.getResultValueAtPosition(rs, this, resultIndexes[0]) == null)
        {
            // Assumption : if the first param is null, then the field is null
            return null;
        }

        if (cmd == null)
        {
            cmd = ec.getMetaDataManager().getMetaDataForClass(getType(),ec.getClassLoaderResolver());
        }

        // Return the object represented by this mapping
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            return MappingHelper.getObjectForDatastoreIdentity(ec, this, rs, resultIndexes, cmd);
        }
        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            return MappingHelper.getObjectForApplicationIdentity(ec, this, rs, resultIndexes, cmd);
        }
        else
        {
            return null;
        }
    }

    // ----------------------- Implementation of MappingCallbacks --------------------------

    /**
     * Method executed just after a fetch of the owning object, allowing any necessary action
     * to this field and the object stored in it.
     * @param sm StateManager for the owner.
     */
    public void postFetch(ObjectProvider sm)
    {
    }

    public void insertPostProcessing(ObjectProvider op)
    {
    }

    /**
     * Method executed just after the insert of the owning object, allowing any necessary action
     * to this field and the object stored in it.
     * @param sm StateManager for the owner
     */
    public void postInsert(ObjectProvider sm)
    {
        Object pc = sm.provideField(mmd.getAbsoluteFieldNumber());
        if (pc == null)
        {
            // Has been set to null so nothing to do
            return;
        }

        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
        RelationType relationType = mmd.getRelationType(clr);
        if (pc != null)
        {
            if (relationType == RelationType.ONE_TO_ONE_BI)
            {
                ObjectProvider otherSM = sm.getExecutionContext().findObjectProvider(pc);
                AbstractMemberMetaData relatedMmd = mmd.getRelatedMemberMetaDataForObject(clr, sm.getObject(), pc);
                if (relatedMmd == null)
                {
                    // Fsck knows why
                    throw new NucleusUserException("You have a field " + mmd.getFullFieldName() + " that is 1-1 bidir yet cannot find the equivalent field at the other side. Why is that?");
                }
                Object relatedValue = otherSM.provideField(relatedMmd.getAbsoluteFieldNumber());
                if (relatedValue == null)
                {
                    // Managed Relations : Other side not set so update it in memory
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("041018",
                            sm.getObjectAsPrintable(), mmd.getFullFieldName(),
                            StringUtils.toJVMIDString(pc), relatedMmd.getFullFieldName()));
                    }
                    otherSM.replaceField(relatedMmd.getAbsoluteFieldNumber(), sm.getObject());
                }
                else if (relatedValue != sm.getObject())
                {
                    // Managed Relations : Other side is inconsistent so throw exception
                    throw new NucleusUserException(
                        LOCALISER.msg("041020",
                            sm.getObjectAsPrintable(), mmd.getFullFieldName(),
                            StringUtils.toJVMIDString(pc),
                            StringUtils.toJVMIDString(relatedValue)));
                }
            }
            else if (relationType == RelationType.MANY_TO_ONE_BI && relatedMmds[0].hasCollection())
            {
                // TODO Make sure we have this PC in the collection at the other side
                ObjectProvider otherSM = sm.getExecutionContext().findObjectProvider(pc);
                if (otherSM != null)
                {
                    // Managed Relations : add to the collection on the other side
                    Collection relatedColl = (Collection)otherSM.provideField(relatedMmds[0].getAbsoluteFieldNumber());
                    if (relatedColl != null && !(relatedColl instanceof SCOCollection))
                    {
                        // TODO Make sure the collection is a wrapper
                        boolean contained = relatedColl.contains(sm.getObject());
                        if (!contained)
                        {
                            NucleusLogger.PERSISTENCE.info(
                                LOCALISER.msg("041022",
                                sm.getObjectAsPrintable(), mmd.getFullFieldName(),
                                StringUtils.toJVMIDString(pc), relatedMmds[0].getFullFieldName()));
                            // TODO Enable this. CUrrently causes issues with
                            // PMImplTest, InheritanceStrategyTest, TCK "inheritance1.conf"
                            /*relatedColl.add(sm.getObject());*/
                        }
                    }
                }
            }
            else if (relationType == RelationType.MANY_TO_ONE_UNI)
            {
                ObjectProvider otherSM = sm.getExecutionContext().findObjectProvider(pc);
                if (otherSM == null)
                {
                    // Related object is not yet persisted so persist it
                    Object other = sm.getExecutionContext().persistObjectInternal(pc, null, -1, ObjectProvider.PC);
                    otherSM = sm.getExecutionContext().findObjectProvider(other);
                }

                // Add join table entry
                PersistableRelationStore store = 
                    (PersistableRelationStore) storeMgr.getBackingStoreForField(
                        sm.getExecutionContext().getClassLoaderResolver(), mmd, mmd.getType());
                store.add(sm, otherSM);
            }
        }
    }

    /**
     * Method executed just afer any update of the owning object, allowing any necessary action
     * to this field and the object stored in it.
     * @param sm StateManager for the owner
     */
    public void postUpdate(ObjectProvider sm)
    {
        Object pc = sm.provideField(mmd.getAbsoluteFieldNumber());
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (pc == null)
        {
            if (relationType == RelationType.MANY_TO_ONE_UNI)
            {
                // Update join table entry
                PersistableRelationStore store =
                    (PersistableRelationStore) storeMgr.getBackingStoreForField(
                        sm.getExecutionContext().getClassLoaderResolver(), mmd, mmd.getType());
                store.remove(sm);
            }

            return;
        }
        else
        {
            ObjectProvider otherSM = sm.getExecutionContext().findObjectProvider(pc);
            if (otherSM == null)
            {
                if (relationType == RelationType.ONE_TO_ONE_BI || relationType == RelationType.MANY_TO_ONE_BI || 
                    relationType == RelationType.MANY_TO_ONE_UNI)
                {
                    // Related object is not yet persisted (e.g 1-1 with FK at other side) so persist it
                    Object other = sm.getExecutionContext().persistObjectInternal(pc, null, -1, ObjectProvider.PC);
                    otherSM = sm.getExecutionContext().findObjectProvider(other);
                }
            }

            if (relationType == RelationType.MANY_TO_ONE_UNI)
            {
                // Update join table entry
                PersistableRelationStore store =
                    (PersistableRelationStore) storeMgr.getBackingStoreForField(
                        sm.getExecutionContext().getClassLoaderResolver(), mmd, mmd.getType());
                store.update(sm, otherSM);
            }
        }
    }

    /**
     * Method executed just before the owning object is deleted, allowing tidying up of any
     * relation information.
     * @param sm StateManager for the owner
     */
    public void preDelete(ObjectProvider sm)
    {
        ExecutionContext ec = sm.getExecutionContext();

        // makes sure field is loaded
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        try
        {
            sm.isLoaded(fieldNumber);
        }
        catch (JDOObjectNotFoundException onfe) // TODO Use different method that throws NucleusObjectNotFoundException
        {
            // Already deleted so just return
            return;
        }

        Object pc = sm.provideField(fieldNumber);
        if (pc == null)
        {
            // Null value so nothing to do
            return;
        }

        // N-1 Uni, so delete join table entry
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType == RelationType.MANY_TO_ONE_UNI)
        {
            // Update join table entry
            PersistableRelationStore store =
                (PersistableRelationStore) storeMgr.getBackingStoreForField(
                    sm.getExecutionContext().getClassLoaderResolver(), mmd, mmd.getType());
            store.remove(sm);
        }

        // Check if we should delete the related object when this object is deleted
        boolean dependent = mmd.isDependent();
        if (mmd.isCascadeRemoveOrphans())
        {
            // JPA allows "orphan removal" to define deletion of the other side
            dependent = true;
        }

        // Check if the field has a FK defined
        AbstractMemberMetaData[] relatedMmds = mmd.getRelatedMemberMetaData(clr);
        // TODO Cater for more than 1 related field

        boolean hasFK = false;
        if (!dependent)
        {
            // Not dependent, so check if the datastore has a FK and will take care of it for us
            if (mmd.getForeignKeyMetaData() != null)
            {
                hasFK = true;
            }
            if (relatedMmds != null && relatedMmds[0].getForeignKeyMetaData() != null)
            {
                hasFK = true;
            }
            if (ec.getNucleusContext().getPersistenceConfiguration().getStringProperty(PropertyNames.PROPERTY_DELETION_POLICY).equals("JDO2"))
            {
                // JDO2 doesnt currently (2.0 spec) take note of foreign-key
                hasFK = false;
            }
        }

        // Basic rules for the following :-
        // 1. If it is dependent then we delete it (maybe after nulling).
        // 2. If it is not dependent and they have defined no FK then null it, else delete it
        // 3. If it is not dependent and they have a FK, let the datastore handle the delete
        // There may be some corner cases that this code doesn't yet cater for
        if (relationType == RelationType.ONE_TO_ONE_UNI ||
            (relationType == RelationType.ONE_TO_ONE_BI && mmd.getMappedBy() == null))
        {
            // 1-1 with FK at this side (owner of the relation)
            if (dependent)
            {
                boolean relatedObjectDeleted = ec.getApiAdapter().isDeleted(pc);
                if (isNullable() && !relatedObjectDeleted)
                {
                    // Other object not yet deleted - just null out the FK
                    // TODO Not doing this would cause errors in 1-1 uni relations (e.g AttachDetachTest)
                    // TODO Log this since it affects the resultant objects
                    sm.replaceFieldMakeDirty(fieldNumber, null);
                    storeMgr.getPersistenceHandler().updateObject(sm, new int[]{fieldNumber});
                    if (!relatedObjectDeleted)
                    {
                        // Mark the other object for deletion since not yet tagged
                        ec.deleteObjectInternal(pc);
                    }
                }
                else
                {
                    // Can't just delete the other object since that would cause a FK constraint violation
                    // Do nothing - handled by DeleteRequest
                    NucleusLogger.DATASTORE_PERSIST.warn("Delete of " + StringUtils.toJVMIDString(sm.getObject()) +
                        " needs delete of related object at " + mmd.getFullFieldName() + " but cannot delete it direct since FK is here");
                }
            }
            else
            {
                // We're deleting the FK at this side so shouldnt be an issue
                AbstractMemberMetaData relatedMmd = mmd.getRelatedMemberMetaDataForObject(clr, sm.getObject(), pc);
                if (relatedMmd != null)
                {
                    ObjectProvider otherSM = ec.findObjectProvider(pc);
                    if (otherSM != null)
                    {
                        // Managed Relations : 1-1 bidir, so null out the object at the other
                        Object currentValue = otherSM.provideField(relatedMmd.getAbsoluteFieldNumber());
                        if (currentValue != null)
                        {
                            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                            {
                                NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("041019",
                                    StringUtils.toJVMIDString(pc), relatedMmd.getFullFieldName(),
                                    sm.getObjectAsPrintable()));
                            }
                            otherSM.replaceFieldMakeDirty(relatedMmd.getAbsoluteFieldNumber(), null);

                            if (ec.getManageRelations())
                            {
                                otherSM.getExecutionContext().getRelationshipManager(otherSM).relationChange(
                                    relatedMmd.getAbsoluteFieldNumber(), sm.getObject(), null);
                            }
                        }
                    }
                }
            }
        }
        else if (relationType == RelationType.ONE_TO_ONE_BI && mmd.getMappedBy() != null)
        {
            // 1-1 with FK at other side
            DatastoreClass relatedTable = storeMgr.getDatastoreClass(relatedMmds[0].getClassName(), clr);
            JavaTypeMapping relatedMapping = relatedTable.getMemberMapping(relatedMmds[0]);
            boolean isNullable = relatedMapping.isNullable();
            ObjectProvider otherSM = ec.findObjectProvider(pc);
            if (dependent)
            {
                if (isNullable)
                {
                    // Null out the FK in the datastore using a direct update (since we are deleting)
                    otherSM.replaceFieldMakeDirty(relatedMmds[0].getAbsoluteFieldNumber(), null);
                    storeMgr.getPersistenceHandler().updateObject(
                        otherSM, new int[]{relatedMmds[0].getAbsoluteFieldNumber()});
                }
                // Mark the other object for deletion
                ec.deleteObjectInternal(pc);
            }
            else if (!hasFK)
            {
                if (isNullable())
                {
                    Object currentRelatedValue = otherSM.provideField(relatedMmds[0].getAbsoluteFieldNumber());
                    if (currentRelatedValue != null)
                    {
                        // Null out the FK in the datastore using a direct update (since we are deleting)
                        otherSM.replaceFieldMakeDirty(relatedMmds[0].getAbsoluteFieldNumber(), null);
                        storeMgr.getPersistenceHandler().updateObject(
                            otherSM, new int[]{relatedMmds[0].getAbsoluteFieldNumber()});

                        // Managed Relations : 1-1 bidir, so null out the object at the other
                        if (ec.getManageRelations())
                        {
                            otherSM.getExecutionContext().getRelationshipManager(otherSM).relationChange(
                                relatedMmds[0].getAbsoluteFieldNumber(), sm.getObject(), null);
                        }
                    }
                }
                else
                {
                    // TODO Remove it
                }
            }
            else
            {
                // User has a FK defined (in MetaData) so let the datastore take care of it
            }
        }
        else if (relationType == RelationType.MANY_TO_ONE_BI)
        {
            ObjectProvider otherSM = ec.findObjectProvider(pc);
            if (relatedMmds[0].getJoinMetaData() == null)
            {
                // N-1 with FK at this side
                if (otherSM.isDeleting())
                {
                    // Other object is being deleted too but this side has the FK so just delete this object
                }
                else
                {
                    // Other object is not being deleted so delete it if necessary
                    if (dependent)
                    {
                        if (isNullable())
                        {
                            // TODO Datastore nullability info can be unreliable so try to avoid this call
                            // Null out the FK in the datastore using a direct update (since we are deleting)
                            sm.replaceFieldMakeDirty(fieldNumber, null);
                            storeMgr.getPersistenceHandler().updateObject(sm, new int[]{fieldNumber});
                        }

                        if (ec.getApiAdapter().isDeleted(pc))
                        {
                            // Object is already tagged for deletion but we're deleting the FK so leave til flush()
                        }
                        else
                        {
                            // Mark the other object for deletion
                            ec.deleteObjectInternal(pc);
                        }
                    }
                    else
                    {
                        // Managed Relations : remove element from collection/map
                        if (relatedMmds[0].hasCollection())
                        {
                            // Only update the other side if not already being deleted
                            if (!ec.getApiAdapter().isDeleted(otherSM.getObject()) && !otherSM.isDeleting())
                            {
                                // Make sure the other object is updated in any caches
                                ec.markDirty(otherSM, false);

                                // Make sure collection field is loaded
                                otherSM.isLoaded(relatedMmds[0].getAbsoluteFieldNumber());
                                Collection otherColl = (Collection)otherSM.provideField(relatedMmds[0].getAbsoluteFieldNumber());
                                if (otherColl != null)
                                {
                                    if (ec.getManageRelations())
                                    {
                                        otherSM.getExecutionContext().getRelationshipManager(otherSM).relationRemove(
                                            relatedMmds[0].getAbsoluteFieldNumber(), sm.getObject());
                                    }
                                    // TODO Localise this message
                                    NucleusLogger.PERSISTENCE.debug("ManagedRelationships : delete of object causes removal from collection at " + relatedMmds[0].getFullFieldName());
                                    otherColl.remove(sm.getObject());
                                }
                            }
                        }
                        else if (relatedMmds[0].hasMap())
                        {
                            // TODO Cater for maps, but what is the key/value pair ?
                        }
                    }
                }
            }
            else
            {
                // N-1 with join table so no FK here so need to remove from Collection/Map first? (managed relations)
                if (dependent)
                {
                    // Mark the other object for deletion
                    ec.deleteObjectInternal(pc);
                }
                else
                {
                    // Managed Relations : remove element from collection/map
                    if (relatedMmds[0].hasCollection())
                    {
                        // Only update the other side if not already being deleted
                        if (!ec.getApiAdapter().isDeleted(otherSM.getObject()) && !otherSM.isDeleting())
                        {
                            // Make sure the other object is updated in any caches
                            ec.markDirty(otherSM, false);

                            // Make sure the other object has the collection loaded so does this change
                            otherSM.isLoaded(relatedMmds[0].getAbsoluteFieldNumber());
                            Collection otherColl = (Collection)otherSM.provideField(relatedMmds[0].getAbsoluteFieldNumber());
                            if (otherColl != null)
                            {
                                // TODO Localise this
                                NucleusLogger.PERSISTENCE.debug("ManagedRelationships : delete of object causes removal from collection at " + relatedMmds[0].getFullFieldName());
                                otherColl.remove(sm.getObject());
                            }
                        }
                    }
                    else if (relatedMmds[0].hasMap())
                    {
                        // TODO Cater for maps, but what is the key/value pair ?
                    }
                }
            }
        }
        else if (relationType == RelationType.MANY_TO_ONE_UNI)
        {
            // N-1 uni with join table
            if (dependent)
            {
                // Mark the other object for deletion
                ec.deleteObjectInternal(pc);
            }
        }
        else
        {
            // No relation so what is this field ?
        }
    }
}