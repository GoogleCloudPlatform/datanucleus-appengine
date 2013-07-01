/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved. 
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
    ...
**********************************************************************/
package org.datanucleus.store.mapped.mapping;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.util.NucleusLogger;

/**
 * Mapping for an embedded PC object. 
 * The PC object can be embedded directly (1-1 relation) or be the element of 
 * a collection, or be the key or value of a map.
 */
public abstract class EmbeddedMapping extends SingleFieldMapping
{
    protected DiscriminatorMetaData discrimMetaData;

    /** Mapping for a discriminator (when supporting inherited embedded objects. */
    protected DiscriminatorMapping discrimMapping;

    /** Mappings of the fields of the embedded PC. */
    protected List<JavaTypeMapping> javaTypeMappings;

    /** ClassLoader resolver */
    protected ClassLoaderResolver clr;

    /** EmbeddedMetaData for the object being embedded. */
    protected EmbeddedMetaData emd;

    /** Type name for the object being embedded. */
    protected String typeName;

    /** Type of PC object. Corresponds to the values in StateManagerImpl. */
    protected short objectType = -1;

    /** MetaData for the embedded class. */
    protected AbstractClassMetaData embCmd = null;

    /**
     * Initialize this JavaTypeMapping with the given DatastoreAdapter for the given FieldMetaData.
     * @param container The datastore container storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     * @param fmd FieldMetaData for the field to be mapped (if any)
     *  
     * @throws NucleusException
     */
    public void initialize(AbstractMemberMetaData fmd, DatastoreContainerObject container, ClassLoaderResolver clr)
    {
        throw new NucleusException("subclass must override this method").setFatal();
    }
    
    /**
     * Initialize this JavaTypeMapping with the given DatastoreAdapter for the given MetaData.
     * @param fmd metadata for the field
     * @param datastoreContainer Table for persisting this field
     * @param clr The ClassLoaderResolver
     * @param emd Embedded MetaData for the object being embedded
     * @param typeName type of the embedded PC object
     * @param objectType Type of the PC object being embedded (see StateManagerImpl object types)
     */
    public void initialize(AbstractMemberMetaData fmd,
                           DatastoreContainerObject datastoreContainer,
                           ClassLoaderResolver clr,
                           EmbeddedMetaData emd,
                           String typeName,
                           int objectType)
    {
    	super.initialize(fmd, datastoreContainer, clr);
        this.clr = clr;
        this.emd = emd;
        this.typeName = typeName;
        this.objectType = (short) objectType;

        // Find the MetaData for the embedded PC class
        MetaDataManager mmgr = datastoreContainer.getStoreManager().getMetaDataManager();
        AbstractClassMetaData rootEmbCmd = mmgr.getMetaDataForClass(typeName, clr);
        if (rootEmbCmd == null)
        {
            // Not found so must be an interface
            if (fmd != null)
            {
                // Try using the fieldTypes on the field/property - we support it if only 1 implementation
                String[] fieldTypes = fmd.getFieldTypes();
                if (fieldTypes != null && fieldTypes.length == 1)
                {
                    rootEmbCmd = mmgr.getMetaDataForClass(fieldTypes[0], clr);
                }
                else if (fieldTypes != null && fieldTypes.length > 1)
                {
                    // TODO Cater for multiple implementations
                    throw new NucleusUserException("Field " + fmd.getFullFieldName() + 
                        " is a reference field that is embedded with multiple possible implementations. " +
                        "DataNucleus doesnt support embedded reference fields that have more than 1 implementation");
                }
            }

            if (rootEmbCmd == null)
            {
                // Try a persistent interface
                rootEmbCmd = mmgr.getMetaDataForInterface(clr.classForName(typeName), clr);
                if (rootEmbCmd == null && fmd.getFieldTypes() != null && fmd.getFieldTypes().length == 1)
                {
                    // No MetaData for the type so try "fieldType" specified on the field
                    rootEmbCmd = mmgr.getMetaDataForInterface(clr.classForName(fmd.getFieldTypes()[0]), clr);
                }
            }
        }

        embCmd = rootEmbCmd;

        AbstractMemberMetaData[] embFmds;
        if (emd == null && rootEmbCmd.isEmbeddedOnly())
        {
            // No <embedded> block yet the class is defined as embedded-only so just use its own definition of fields
            embFmds = rootEmbCmd.getManagedMembers();
        }
        else
        {
            // <embedded> block so use those field definitions
            embFmds = emd.getMemberMetaData();
        }

        String[] subclasses = mmgr.getSubclassesForClass(rootEmbCmd.getFullClassName(), true);
        if (subclasses != null && subclasses.length > 0)
        {
            if (rootEmbCmd.hasDiscriminatorStrategy())
            {
                discrimMetaData = new DiscriminatorMetaData();

                // Set strategy based on the inheritance of the embedded object, otherwise class name.
                DiscriminatorMetaData dismd = rootEmbCmd.getDiscriminatorMetaDataRoot();
                if (dismd.getStrategy() != null && dismd.getStrategy() != DiscriminatorStrategy.NONE)
                {
                    discrimMetaData.setStrategy(dismd.getStrategy());
                }
                else
                {
                    discrimMetaData.setStrategy(DiscriminatorStrategy.CLASS_NAME); // Fallback to class name
                }

                // Set column for discriminator
                ColumnMetaData disColmd = new ColumnMetaData();
                disColmd.setAllowsNull(Boolean.TRUE);

                DiscriminatorMetaData embDismd = emd.getDiscriminatorMetaData();
                if (embDismd != null && embDismd.getColumnMetaData() != null)
                {
                    disColmd.setName(embDismd.getColumnMetaData().getName());
                }
                else
                {
                    ColumnMetaData colmd = dismd.getColumnMetaData();
                    if (colmd != null && colmd.getName() != null)
                    {
                        disColmd.setName(colmd.getName());
                    }
                }
                discrimMetaData.setColumnMetaData(disColmd);

                discrimMapping = DiscriminatorMapping.createDiscriminatorMapping(datastoreContainer, discrimMetaData);
                addDatastoreMapping(discrimMapping.getDatastoreMapping(0));
            }
            else
            {
                NucleusLogger.PERSISTENCE.info("Member " + mmd.getFullFieldName() + " is embedded and the type " + 
                    "(" + rootEmbCmd.getFullClassName() + ") has potential subclasses." +
                    " Impossible to detect which is stored embedded. Add a discriminator to the embedded type");
            }
        }

        // Add all fields of the embedded class (that are persistent)
        int[] pcFieldNumbers = rootEmbCmd.getAllMemberPositions();
        for (int i=0;i<pcFieldNumbers.length;i++)
        {
            AbstractMemberMetaData rootEmbMmd =
                rootEmbCmd.getMetaDataForManagedMemberAtAbsolutePosition(pcFieldNumbers[i]);
            if (rootEmbMmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
            {
                addMappingForMember(rootEmbCmd, rootEmbMmd, embFmds);
            }
        }

        // Add fields for any subtypes (that are persistent)
        if (discrimMapping != null && subclasses != null && subclasses.length > 0)
        {
            for (int i=0;i<subclasses.length;i++)
            {
                AbstractClassMetaData subEmbCmd = storeMgr.getMetaDataManager().getMetaDataForClass(subclasses[i], clr);
                AbstractMemberMetaData[] subEmbMmds = subEmbCmd.getManagedMembers();
                if (subEmbMmds != null)
                {
                    for (int j=0;j<subEmbMmds.length;j++)
                    {
                        if (subEmbMmds[j].getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
                        {
                            addMappingForMember(subEmbCmd, subEmbMmds[j], embFmds);
                        }
                    }
                }
            }
        }
    }

    /**
     * Method to add a mapping for the specified member to this mapping.
     * @param embCmd Class that the member belongs to
     * @param embMmd Member to be added
     * @param embMmds The metadata defining mapping information for the members (if any)
     */
    private void addMappingForMember(AbstractClassMetaData embCmd, AbstractMemberMetaData embMmd,
            AbstractMemberMetaData[] embMmds)
    {
        if (emd != null && emd.getOwnerMember() != null && emd.getOwnerMember().equals(embMmd.getName()))
        {
            // Do nothing since we don't map owner fields (since the owner is the containing object)
        }
        else
        {
            AbstractMemberMetaData embeddedMmd = null;
            for (int j=0;j<embMmds.length;j++)
            {
                // Why are these even possible ? Why are they here ? Why don't they use localised messages ?
                if (embMmds[j] == null)
                {
                    throw new RuntimeException("embMmds[j] is null for class=" + embCmd.toString() + " type="+ typeName);
                }
                AbstractMemberMetaData embMmdForMmds = embCmd.getMetaDataForMember(embMmds[j].getName());
                if (embMmdForMmds != null)
                {
                    if (embMmdForMmds.getAbsoluteFieldNumber() == embMmd.getAbsoluteFieldNumber())
                    {
                        // Same as the member we are processing, so use it
                        embeddedMmd = embMmds[j];
                    }
                }
            }

            // Add mapping
            JavaTypeMapping embMmdMapping;
            MappingManager mapMgr = datastoreContainer.getStoreManager().getMappingManager();
            if (embeddedMmd != null)
            {
                // User has provided a field definition so map with that
                embMmdMapping = mapMgr.getMapping(datastoreContainer, embeddedMmd, clr, FieldRole.ROLE_FIELD);
                if (embeddedMmd.getAbsoluteFieldNumber() < 0)
                {
                    // Embedded AbstractMemberMetaData don't have the field number etc, so we set the field number directly
                    embMmdMapping.setAbsFieldNumber(embMmd.getAbsoluteFieldNumber());
                }
            }
            else
            {
                // User hasn't provided a field definition so map with the classes own definition
                embMmdMapping = mapMgr.getMapping(datastoreContainer, embMmd, clr, FieldRole.ROLE_FIELD);
            }
            this.addJavaTypeMapping(embMmdMapping);

            for (int j=0; j<embMmdMapping.getNumberOfDatastoreMappings(); j++)
            {
                // Register column with mapping
                DatastoreMapping datastoreMapping = embMmdMapping.getDatastoreMapping(j);
                this.addDatastoreMapping(datastoreMapping);

                if (this.mmd.isPrimaryKey())
                {
                    // Overall embedded field should be part of PK, so make all datastore fields part of it
                    DatastoreField datastoreFld = datastoreMapping.getDatastoreField();
                    if (datastoreFld != null)
                    {
                        datastoreFld.setAsPrimaryKey();
                    }
                }
            }
        }
    }

    /**
     * Method to prepare a field mapping for use in the datastore.
     * Overridden so it does nothing
     */
    protected void prepareDatastoreMapping()
    {
    }

    /**
     * Add a new JavaTypeMapping to manage.
     * @param mapping the JavaTypeMapping
     */
    public void addJavaTypeMapping(JavaTypeMapping mapping)
    {
        if (javaTypeMappings == null)
        {
            javaTypeMappings = new ArrayList();
        }
        if (mapping == null)
        {
            throw new NucleusException("mapping argument in EmbeddedMapping.addJavaTypeMapping is null").setFatal();
        }
        javaTypeMappings.add(mapping);
    }

    /**
     * Accessor for the number of java type mappings
     * @return Number of java type mappings of the fields of the embedded PC element
     */
    public int getNumberOfJavaTypeMappings()
    {
        return javaTypeMappings != null ? javaTypeMappings.size() : 0;
    }

    /**
     * Accessor for the java type mappings
     * @param i the index position of the java type mapping
     * @return the java type mapping
     */
    public JavaTypeMapping getJavaTypeMapping(int i)
    {
        if (javaTypeMappings == null)
        {
            return null;
        }
        return javaTypeMappings.get(i);
    }

    /**
     * Accessor for the sub type mapping for a particular field name
     * @param fieldName The field name
     * @return The type mapping for that field in the embedded object
     */
    public JavaTypeMapping getJavaTypeMapping(String fieldName)
    {
        if (javaTypeMappings == null)
        {
            return null;
        }
        Iterator iter = javaTypeMappings.iterator();
        while (iter.hasNext())
        {
            JavaTypeMapping m = (JavaTypeMapping)iter.next();
            if (m.getMemberMetaData().getName().equals(fieldName))
            {
                return m;
            }
        }
        return null;
    }

    public JavaTypeMapping getDiscriminatorMapping()
    {
        return discrimMapping;
    }

    /**
     * Mutator for the embedded object in the datastore.
     * @param ec execution context
     * @param ps The Prepared Statement
     * @param param Param numbers in the PreparedStatement for the fields of this object
     * @param value The embedded object to use
     */
    public void setObject(ExecutionContext ec, Object ps, int[] param, Object value)
    {
        setObject(ec, ps, param, value, null, -1);
    }

    /**
     * Mutator for the embedded object in the datastore.
     * @param ec ExecutionContext
     * @param ps The Prepared Statement
     * @param param Param numbers in the PreparedStatement for the fields of this object
     * @param value The embedded object to use
     * @param ownerSM StateManager of the owning object containing this embedded object
     * @param ownerFieldNumber Field number in the owning object where this is stored
     */
    public void setObject(ExecutionContext ec, Object ps, int[] param, Object value, ObjectProvider ownerSM, int ownerFieldNumber)
    {
        if (value == null)
        {
            int n = 0;
            String nullColumn = null;
            String nullValue = null;
            if (emd != null)
            {
                nullColumn = emd.getNullIndicatorColumn();
                nullValue = emd.getNullIndicatorValue();
            }
            if (discrimMapping != null)
            {
                discrimMapping.setObject(ec, ps, new int[]{param[n]}, null);
                n++;
            }

            for (int i=0; i<javaTypeMappings.size(); i++)
            {
                JavaTypeMapping mapping = javaTypeMappings.get(i);
                int[] posMapping = new int[mapping.getNumberOfDatastoreMappings()];
                for (int j=0; j<posMapping.length; j++)
                {
                    posMapping[j] = param[n++];
                }

                // Null out this field unless it is the null-indicator column and has a value
                // in which case apply the required value
                if (nullColumn != null && nullValue != null &&
                    mapping.getMemberMetaData().getColumnMetaData().length > 0 &&
                    mapping.getMemberMetaData().getColumnMetaData()[0].getName().equals(nullColumn))
                {
                    // Try to cater for user having an integer based column and value
                    if (mapping instanceof IntegerMapping ||
                        mapping instanceof BigIntegerMapping ||
                        mapping instanceof LongMapping ||
                        mapping instanceof ShortMapping)
                    {
                        Object convertedValue = null;
                        try
                        {
                            if (mapping instanceof IntegerMapping ||
                                mapping instanceof ShortMapping)
                            {
                                convertedValue = Integer.valueOf(nullValue);
                            }
                            else if (mapping instanceof LongMapping ||
                                mapping instanceof BigIntegerMapping)
                            {
                                convertedValue = Long.valueOf(nullValue);
                            }
                        }
                        catch (Exception e)
                        {
                        }
                        mapping.setObject(ec, ps, posMapping, convertedValue);
                    }
                    else
                    {
                        mapping.setObject(ec, ps, posMapping, nullValue);
                    }
                }
                else
                {
                    if (mapping.getNumberOfDatastoreMappings() > 0)
                    {
                        mapping.setObject(ec, ps, posMapping, null);
                    }
                }
            }
        }
        else
        {
            ApiAdapter api = ec.getApiAdapter();
            if (!api.isPersistable(value))
            {
                throw new NucleusException(LOCALISER.msg("041016", value.getClass(), value)).setFatal();
            }

            AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(value.getClass(), ec.getClassLoaderResolver());
            ObjectProvider embSM = ec.findObjectProvider(value);
            if (embSM == null || api.getExecutionContext(value) == null)
            {
                // Assign a StateManager to manage our embedded object
                embSM = ec.newObjectProviderForEmbedded(value, false, ownerSM, ownerFieldNumber);
                embSM.setPcObjectType(objectType);
            }

            int n = 0;

            if (discrimMapping != null)
            {
                if (discrimMetaData.getStrategy() == DiscriminatorStrategy.CLASS_NAME)
                {
                    discrimMapping.setObject(ec, ps, new int[]{param[n]}, value.getClass().getName());
                }
                else if (discrimMetaData.getStrategy() == DiscriminatorStrategy.VALUE_MAP)
                {
                    DiscriminatorMetaData valueDismd = embCmd.getInheritanceMetaData().getDiscriminatorMetaData();
                    discrimMapping.setObject(ec, ps, new int[]{param[n]}, valueDismd.getValue());
                }
                n++;
            }

            for (int i=0; i<javaTypeMappings.size(); i++)
            {
                JavaTypeMapping mapping = javaTypeMappings.get(i);
                int[] posMapping = new int[mapping.getNumberOfDatastoreMappings()];
                for (int j=0; j<posMapping.length; j++)
                {
                    posMapping[j] = param[n++];
                }

                // Retrieve value of member from Embedded StateManager
                int embAbsFieldNum = embCmd.getAbsolutePositionOfMember(mapping.getMemberMetaData().getName());
                if (embAbsFieldNum >= 0)
                {
                    // Member is present in this embedded type
                    Object fieldValue = embSM.provideField(embAbsFieldNum);
                    if (mapping instanceof EmbeddedPCMapping)
                    {
                        mapping.setObject(ec, ps, posMapping, fieldValue, embSM, embAbsFieldNum);
                    }
                    else
                    {
                        if (mapping.getNumberOfDatastoreMappings() > 0)
                        {
                            mapping.setObject(ec, ps, posMapping, fieldValue);
                        }
                    }
                }
                else
                {
                    mapping.setObject(ec, ps, posMapping, null);
                }
            }
        }
    }

    /**
     * Accessor for the embedded object from the result set
     * @param ec ExecutionContext
     * @param rs The ResultSet
     * @param param Array of param numbers in the ResultSet for the fields of this object
     * @return The embedded object
     */
    public Object getObject(ExecutionContext ec, Object rs, int[] param)
    {
        return getObject(ec, rs, param, null, -1);
    }

    /**
     * Accessor for the embedded object from the result set
     * @param ec ExecutionContext
     * @param rs The ResultSet
     * @param param Array of param numbers in the ResultSet for the fields of this object
     * @param ownerSM StateManager of the owning object containing this embedded object
     * @param ownerFieldNumber Field number in the owning object where this is stored
     * @return The embedded object
     */
    public Object getObject(ExecutionContext ec, Object rs, int[] param, ObjectProvider ownerSM, int ownerFieldNumber)
    {
        Object value = null;

        int n = 0;

        // Determine the type of the embedded object
        AbstractClassMetaData embCmd = this.embCmd;
        if (discrimMapping != null)
        {
            Object discrimValue = discrimMapping.getObject(ec, rs, new int[]{param[n]});
            if (discrimMetaData.getStrategy() == DiscriminatorStrategy.CLASS_NAME)
            {
                embCmd = storeMgr.getMetaDataManager().getMetaDataForClass((String)discrimValue, clr);
            }
            else if (discrimMetaData.getStrategy() == DiscriminatorStrategy.VALUE_MAP)
            {
                String className = MetaDataUtils.getClassNameFromDiscriminatorValue((String)discrimValue, discrimMetaData, ec);
                embCmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
            }
            n++;
        }

        // Create a PersistenceCapable to put the values into
        Class embeddedType = clr.classForName(embCmd.getFullClassName());
        if (mmd.getFieldTypes() != null && mmd.getFieldTypes().length > 0)
        {
            // Embedded type has field-type defined so use that as our embedded type
            embeddedType = ec.getClassLoaderResolver().classForName(mmd.getFieldTypes()[0]);
        }
        ObjectProvider embSM = ec.newObjectProviderForHollow(embeddedType, (Object)null);
        embSM.setPcObjectType(objectType);
        value = embSM.getObject();

        String nullColumn = null;
        String nullValue = null;
        if (emd != null)
        {
            nullColumn = emd.getNullIndicatorColumn();
            nullValue = emd.getNullIndicatorValue();
        }

        // Populate the field values
        for (int i=0; i<javaTypeMappings.size(); i++)
        {
            JavaTypeMapping mapping = javaTypeMappings.get(i);
            int embAbsFieldNum = embCmd.getAbsolutePositionOfMember(mapping.getMemberMetaData().getName());
            if (embAbsFieldNum >= 0)
            {
                // Mapping for field that is present in this embedded type, so set the field
                if (mapping instanceof EmbeddedPCMapping)
                {
                    // We have a nested embedded
                    int numSubParams = mapping.getNumberOfDatastoreMappings();
                    int[] subParam = new int[numSubParams];
                    int k = 0;
                    for (int j=n;j<n+numSubParams;j++)
                    {
                        subParam[k++] = param[j];
                    }
                    n += numSubParams;

                    // Use the sub-object mapping to extract the value for that object
                    Object subValue = mapping.getObject(ec, rs, subParam, embSM, embAbsFieldNum);
                    if (subValue != null)
                    {
                        embSM.replaceField(embAbsFieldNum, subValue);
                    }

                    // TODO Check the null column and its value in the sub-embedded ?
                }
                else
                {
                    // Extract the value(s) for this field and update the PC if it is not null
                    int[] posMapping = new int[mapping.getNumberOfDatastoreMappings()];
                    for (int j=0; j<posMapping.length; j++)
                    {
                        posMapping[j] = param[n++];
                    }
                    Object fieldValue = mapping.getObject(ec, rs, posMapping);

                    // Check for the null column and its value and break if this matches the null check
                    if (nullColumn != null && 
                        mapping.getMemberMetaData().getColumnMetaData()[0].getName().equals(nullColumn))
                    {
                        if ((nullValue == null && fieldValue == null) ||
                            (nullValue != null && fieldValue.toString().equals(nullValue)))
                        {
                            value = null;
                            break;
                        }
                    }

                    // Set the field value
                    if (fieldValue != null)
                    {
                        embSM.replaceField(embAbsFieldNum, fieldValue);
                    }
                    else
                    {
                        // If the value is null, but the field is not a primitive update it
                        AbstractMemberMetaData embFmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embAbsFieldNum);
                        if (!embFmd.getType().isPrimitive())
                        {
                            embSM.replaceField(embAbsFieldNum, fieldValue);
                        }
                    }
                }
            }
            else
            {
                // Mapping not present in this embedded type so maybe subclass, so just omit the positions
                int numSubParams = mapping.getNumberOfDatastoreMappings();
                n += numSubParams;
            }
        }

        // Update owner field in the element (if present)
        if (emd != null)
        {
            String ownerField = emd.getOwnerMember();
            if (ownerField != null)
            {
                int ownerFieldNumberInElement = embCmd.getAbsolutePositionOfMember(ownerField);
                if (ownerFieldNumberInElement >= 0)
                {
                    embSM.replaceField(ownerFieldNumberInElement, ownerSM.getObject());
                }
            }
        }
        
        // Register our owner now that we have our values set
        if (value != null && ownerSM != null)
        {
            ec.registerEmbeddedRelation(ownerSM, ownerFieldNumber, embSM);
        }

        return value;
    }

    /**
     * Accessor for the Java type being represented here.
     * @return The Java type
     */
    public Class getJavaType()
    {
        return clr.classForName(typeName);
    }
}