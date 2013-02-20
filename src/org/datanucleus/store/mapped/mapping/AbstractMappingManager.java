/******************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved. 
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
*****************************************************************/
package org.datanucleus.store.mapped.mapping;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassNameConstants;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.mapped.DatastoreAdapter;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.exceptions.NoTableManagedException;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Base implementation of a MappingManager. Datastores should extend this to add their own specifics, and to
 * add the initialisation of their supported types.
 * <P>
 * The idea behind a MappingManager is that at the Java side we have a series of Java type mappings,
 * and at the datastore side we have a series of datastore type mappings. We need a link between the two
 * to say that "this Java type can map to any of these 3 datastore types, and by default use this one".
 */
public abstract class AbstractMappingManager implements MappingManager
{
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.mapped.Localisation", MappingManager.class.getClassLoader());

    /** Store Manager that this relates to. */
    protected final MappedStoreManager storeMgr;

    /**
     * Constructor for a mapping manager for an ORM datastore.
     * @param storeMgr The StoreManager
     */
    public AbstractMappingManager(MappedStoreManager storeMgr)
    {
        this.storeMgr = storeMgr;
    }

    /**
     * Load all datastore mappings defined in the associated plugins.
     * To be implemented by the datastore mapping managers since they have the knowledge of
     * the attributes supported by that datastore (e.g RDBMS datastores use jdbc-type, sql-type)
     * @param mgr the PluginManager
     * @param clr the ClassLoaderResolver
     * @param vendorId the datastore vendor id
     */
    public abstract void loadDatastoreMapping(PluginManager mgr, ClassLoaderResolver clr, String vendorId);

    /**
     * Accessor for the mapping for the specified class. Usually only called by JDOQL query expressions.
     * If the type has its own table returns the id mapping of the table.
     * If the type doesn't have its own table then creates the mapping and, if it has a simple
     * datastore representation, creates the datastore mapping. The JavaTypeMapping has no metadata/table
     * associated.
     * @param c Java type
     * @param serialised Whether the type is serialised
     * @param embedded Whether the type is embedded
     * @param clr ClassLoader resolver
     * @return The mapping for the class.
     */
    public JavaTypeMapping getMappingWithDatastoreMapping(Class c, boolean serialised, boolean embedded, 
            ClassLoaderResolver clr)
    {
        try
        {
            // TODO This doesn't take into account serialised/embedded
            // If the type has its own table just take the id mapping of its table
            DatastoreClass datastoreClass = storeMgr.getDatastoreClass(c.getName(), clr);
            return datastoreClass.getIdMapping();
        }
        catch (NoTableManagedException ex)
        {
            // Doesn't allow for whether a field is serialised/embedded so they get the default mapping only
            Class mc = getMappingClass(c, serialised, embedded, null);
            mc = getOverrideMappingClass(mc, null, -1); // Allow for overriding in subclasses
            JavaTypeMapping m = MappingFactory.createMapping(mc, storeMgr, c.getName());
            if (m == null)
            {
                String name = mc.getName();
                name = name.substring(name.lastIndexOf('.') + 1);
                throw new NucleusUserException(LOCALISER.msg("041012", name));
            }
            if (m.hasSimpleDatastoreRepresentation())
            {
                // Create the datastore mapping (NOT the column)
                createDatastoreMapping(m, null, m.getJavaTypeForDatastoreMapping(0));
                // TODO How to handle SingleFieldMultiMapping cases ?
            }
            return m;
        }
    }

    /**
     * Accessor for the mapping for the specified class.
     * This simply creates a JavaTypeMapping for the java type and returns it. The mapping
     * has no underlying datastore mapping(s) and no associated field/table.
     * @param c Java type
     * @return The mapping for the class.
     */
    public JavaTypeMapping getMapping(Class c)
    {
        return getMapping(c, false, false, (String)null);
    }

    /**
     * Accessor for the mapping for the specified class.
     * This simply creates a JavaTypeMapping for the java type and returns it.
     * The mapping has no underlying datastore mapping(s) and no associated field/table.
     * @param c Java type
     * @param serialised Whether the type is serialised
     * @param embedded Whether the type is embedded
     * @param fieldName Name of the field (for logging)
     * @return The mapping for the class.
     */
    public JavaTypeMapping getMapping(Class c, boolean serialised, boolean embedded, String fieldName)
    {
        Class mc = getMappingClass(c, serialised, embedded, fieldName);
        mc = getOverrideMappingClass(mc, null, -1); // Allow for overriding in subclasses

        JavaTypeMapping m = MappingFactory.createMapping(mc, storeMgr, c.getName());
        if (m == null)
        {
            String name = mc.getName();
            name = name.substring(name.lastIndexOf('.') + 1);
            throw new NucleusUserException(LOCALISER.msg("041012",name));
        }
        return m;
    }

    /**
     * Accessor for the mapping for the field of the specified table.
     * Can be used for fields of a class, elements of a collection of a class, elements of an array of
     * a class, keys of a map of a class, values of a map of a class. This is controlled by the role
     * argument.
     * @param clr The ClassLoaderResolver
     * @param fieldRole Role that this mapping plays for the field
     * @param datastoreContainer Table to add the mapping to
     * @param fmd FieldMetaData for the field to map
     * @return The mapping for the field.
     */
    public JavaTypeMapping getMapping(DatastoreContainerObject datastoreContainer,
            AbstractMemberMetaData fmd, ClassLoaderResolver clr, int fieldRole)
    {
        Class mc = null;
        DatastoreAdapter dba = datastoreContainer.getStoreManager().getDatastoreAdapter();

        AbstractMemberMetaData overrideMmd = null;
        if (fmd.getTypeConverterName() != null)
        {
            // Member should be mapped using a TypeConverter (which defines the datastore type)
            mc = TypeConverterMapping.class;
        }
        else if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT || fieldRole == FieldRole.ROLE_ARRAY_ELEMENT)
        {
            // Mapping a collection/array element (in a join table)
            mc = getElementMappingClass(datastoreContainer, fmd, dba, clr);
        }
        else if (fieldRole == FieldRole.ROLE_MAP_KEY)
        {
            // Mapping a map key (in a join table)
            mc = getKeyMappingClass(datastoreContainer, fmd, dba, clr);
        }
        else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
        {
            // Mapping a map value (in a join table)
            mc = getValueMappingClass(datastoreContainer, fmd, dba, clr);
        }
        else
        {
            // Assumed to be a normal field
            String userMappingClassName = fmd.getValueForExtension("mapping-class");
            if (userMappingClassName != null)
            {
                // User has defined their own mapping class for this field so use that
                try
                {
                    mc = clr.classForName(userMappingClassName);
                }
                catch (NucleusException jpe)
                {
                    throw new NucleusUserException(LOCALISER.msg("041014", 
                        fmd.getFullFieldName(), userMappingClassName)).setFatal();
                }
            }
            else
            {
                AbstractClassMetaData acmd = null;
                if (fmd.getType().isInterface())
                {
                    acmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForInterface(fmd.getType(), clr);
                }
                else
                {
                    acmd = storeMgr.getNucleusContext().getMetaDataManager().getMetaDataForClass(fmd.getType(), clr);
                }

                if (fmd.isSerialized())
                {
                    // Field is marked as serialised then we have no other option - serialise it
                    mc = getMappingClass(fmd.getType(), true, false, fmd.getFullFieldName());
                }
                else if (fmd.getEmbeddedMetaData() != null)
                {
                    // Field has an <embedded> specification so use that
                    mc = getMappingClass(fmd.getType(), false, true, fmd.getFullFieldName());
                }
                else if (acmd != null && acmd.isEmbeddedOnly())
                {
                    // If the reference type is declared with embedded only
                    mc = getMappingClass(fmd.getType(), false, true, fmd.getFullFieldName());
                }
                else if (fmd.isEmbedded())
                {
                    // Otherwise, if the field is embedded then we request that it be serialised into the owner table
                    // This is particularly for java.lang.Object which should be "embedded" by default, and hence serialised
                    mc = getMappingClass(fmd.getType(), true, false, fmd.getFullFieldName());
                }
                else
                {
                    // Non-embedded/non-serialised - Just get the basic mapping for the type
                    mc = getMappingClass(fmd.getType(), false, false, fmd.getFullFieldName());

                    if (fmd.getParent() instanceof EmbeddedMetaData && fmd.getRelationType(clr) != RelationType.NONE)
                    {
                        // See NUCCORE-697 - always need to use the real member metadata for the mapping
                        // so that it can find sub-fields when persisting/querying etc
                        AbstractClassMetaData cmdForFmd = datastoreContainer.getStoreManager().getMetaDataManager().getMetaDataForClass(fmd.getClassName(), clr);
                        overrideMmd = cmdForFmd.getMetaDataForMember(fmd.getName());
                    }
                }
            }
        }
        mc = getOverrideMappingClass(mc, fmd, fieldRole); // Allow for overriding in subclasses

        // Create the mapping of the selected type
        JavaTypeMapping m = MappingFactory.createMapping(mc, dba, fmd, fieldRole, datastoreContainer, clr);
        if (m == null)
        {
            throw new NucleusException(LOCALISER.msg("041011", mc.getName())).setFatal();
        }
        if (overrideMmd != null)
        {
            m.setMemberMetaData(overrideMmd);
        }

        return m;
    }

    /**
     * Convenience method to allow overriding of particular mapping classes.
     * @param mappingClass The mapping class selected
     * @param fmd Field meta data for the field (if appropriate)
     * @param fieldRole Role for the field (e.g collection element)
     * @return The mapping class to use
     */
    protected Class getOverrideMappingClass(Class mappingClass, AbstractMemberMetaData fmd, int fieldRole)
    {
        return mappingClass;
    }

    /**
     * Accessor for the mapping class for the specified class.
     * Provides special handling for interface types and for classes that are being embedded in a field.
     * Refers others to its mapping manager lookup.
     * @param c Class to query
     * @param serialised Whether the field is serialised
     * @param embedded Whether the field is embedded
     * @param fieldName The full field name (for logging only)
     * @return The mapping class for the class
     **/
    protected Class getMappingClass(Class c, boolean serialised, boolean embedded, String fieldName)
    {
        ApiAdapter api = storeMgr.getApiAdapter();
        if (api.isPersistable(c))
        {
            // Persistence Capable field
            if (serialised)
            {
                // Serialised PC field
                return SerialisedPCMapping.class;
            }
            else if (embedded)
            {
                // Embedded PC field
                return EmbeddedPCMapping.class;
            }
            else
            {
                // PC field
                return PersistableMapping.class;
            }
        }

        if (c.isInterface() && !storeMgr.getMappedTypeManager().isSupportedMappedType(c.getName()))
        {
            // Interface field
            if (serialised)
            {
                // Serialised Interface field
                return SerialisedReferenceMapping.class;
            }
            else if (embedded)
            {
                // Embedded interface field - just default to an embedded PCMapping!
                return EmbeddedPCMapping.class;
            }
            else
            {
                // Interface field
                return InterfaceMapping.class;
            }
        }

        if (c == java.lang.Object.class)
        {
            // Object field
            if (serialised)
            {
                // Serialised Object field
                return SerialisedReferenceMapping.class;
            }
            else if (embedded)
            {
                // Embedded Object field - do we ever want to support this ? I think not ;-)
                throw new NucleusUserException(LOCALISER.msg("041042", fieldName)).setFatal();
            }
            else
            {
                // Object field as reference to PC object
                return ObjectMapping.class;
            }
        }

        if (c.isArray())
        {
            // Array field
            if (api.isPersistable(c.getComponentType()))
            {
                // Array of PC objects
                return ArrayMapping.class;
            }
            else if (c.getComponentType().isInterface() &&
                !storeMgr.getMappedTypeManager().isSupportedMappedType(c.getComponentType().getName()))
            {
                // Array of interface objects
                return ArrayMapping.class;
            }
            else if (c.getComponentType() == java.lang.Object.class)
            {
                // Array of Object reference objects
                return ArrayMapping.class;
            }
            // Other array types will be caught by the default mappings
        }

        // Try the default mapping (doesn't allow for serialised setting)
        Class mappingClass = getDefaultJavaTypeMapping(c);
        if (mappingClass == null)
        {
            Class superClass = c.getSuperclass();
            while (superClass != null && !superClass.getName().equals(ClassNameConstants.Object) && 
                    mappingClass == null)
            {
                mappingClass = getDefaultJavaTypeMapping(superClass);
                superClass = superClass.getSuperclass();
            }
        }
        if (mappingClass == null)
        {
            if (storeMgr.getMappedTypeManager().isSupportedMappedType(c.getName()))
            {
                // "supported" type yet no FCO mapping !
                throw new NucleusUserException(LOCALISER.msg("041001", fieldName, c.getName()));
            }
            else
            {
                Class superClass = c; // start in this class
                while (superClass!=null && !superClass.getName().equals(ClassNameConstants.Object) && mappingClass == null)
                {
                    Class[] interfaces = superClass.getInterfaces();
                    for( int i=0; i<interfaces.length && mappingClass == null; i++)
                    {
                        mappingClass = getDefaultJavaTypeMapping(interfaces[i]);
                    }
                    superClass = superClass.getSuperclass();
                }
                if (mappingClass == null)
                {
                    //TODO if serialised == false, should we raise an exception?
                    // Treat as serialised
                    mappingClass = SerialisedMapping.class;
                }
            }
        }
        return mappingClass;
    }

    /**
     * Convenience accessor for the mapping class of the element mapping for a collection/array of elements.
     * Currently only used where the collection/array elements are either serialised or embedded into a 
     * join table.
     * @param container The container
     * @param mmd MetaData for the collection field/property containing the collection/array of PCs
     * @param dba Database adapter
     * @param clr ClassLoader resolver
     * @return The mapping class
     */
    protected Class getElementMappingClass(DatastoreContainerObject container, 
            AbstractMemberMetaData mmd, DatastoreAdapter dba, ClassLoaderResolver clr)
    {
        if (!mmd.hasCollection() && !mmd.hasArray())
        {
            // TODO Localise this message
            throw new NucleusException("Attempt to get element mapping for field " + mmd.getFullFieldName() + 
                " that has no collection/array!").setFatal();
        }
        if (mmd.getJoinMetaData() == null)
        {
            AbstractMemberMetaData[] refMmds = mmd.getRelatedMemberMetaData(clr);
            if (refMmds == null || refMmds.length == 0)
            {
                // TODO Localise this
                throw new NucleusException("Attempt to get element mapping for field " + mmd.getFullFieldName() + 
                    " that has no join table defined for the collection/array").setFatal();
            }
            else
            {
                if (refMmds[0].getJoinMetaData() == null)
                {
                    // TODO Localise this
                    throw new NucleusException("Attempt to get element mapping for field " + mmd.getFullFieldName() + 
                        " that has no join table defined for the collection/array").setFatal();
                }
            }
        }

        String userMappingClassName = null;
        if (mmd.getElementMetaData() != null)
        {
            userMappingClassName = mmd.getElementMetaData().getValueForExtension("mapping-class");
        }
        if (userMappingClassName != null)
        {
            // User has defined their own mapping class for this element so use that
            try
            {
                return clr.classForName(userMappingClassName);
            }
            catch (NucleusException jpe)
            {
                throw new NucleusUserException(LOCALISER.msg("041014", userMappingClassName)).setFatal();
            }
        }

        boolean serialised = ((mmd.hasCollection() && mmd.getCollection().isSerializedElement()) ||
            (mmd.hasArray() && mmd.getArray().isSerializedElement()));
        boolean embeddedPC = (mmd.getElementMetaData() != null && mmd.getElementMetaData().getEmbeddedMetaData() != null);
        boolean elementPC = ((mmd.hasCollection() && mmd.getCollection().elementIsPersistent()) ||
            (mmd.hasArray() && mmd.getArray().elementIsPersistent()));
        boolean embedded = true;
        if (mmd.hasCollection())
        {
            embedded = mmd.getCollection().isEmbeddedElement();
        }
        else if (mmd.hasArray())
        {
            embedded = mmd.getArray().isEmbeddedElement();
        }

        Class elementCls = null;
        if (mmd.hasCollection())
        {
            elementCls = clr.classForName(mmd.getCollection().getElementType());
        }
        else if (mmd.hasArray())
        {
            // Use basic element type rather than any restricted type
            elementCls = mmd.getType().getComponentType();
//            elementCls = clr.classForName(mmd.getArray().getElementType());
        }
        boolean elementReference = ClassUtils.isReferenceType(elementCls);

        Class mc = null;
        if (serialised)
        {
            if (elementPC)
            {
                // Serialised PC element
                mc = SerialisedElementPCMapping.class;
            }
            else if (elementReference)
            {
                // Serialised Reference element
                mc = SerialisedReferenceMapping.class;
            }
            else
            {
                // Serialised Non-PC element
                mc = SerialisedMapping.class;
            }
        }
        else if (embedded)
        {
            if (embeddedPC)
            {
                // Embedded PC type
                mc = EmbeddedElementPCMapping.class;
            }
            else if (elementPC)
            {
                // "Embedded" PC type but no <embedded> so dont embed for now. Is this correct?
                mc = PersistableMapping.class;
            }
            else
            {
                // Embedded Non-PC type
                mc = getMappingClass(elementCls, serialised, embedded, mmd.getFullFieldName());
            }
        }
        else
        {
            // Normal element mapping
            mc = getMappingClass(elementCls, serialised, embedded, mmd.getFullFieldName());
            // TODO Allow for other element mappings
            /*throw new NucleusException("Attempt to get element mapping for field " + mmd.getFullFieldName() +
                " of element-type=" + elementCls.getName() + 
                " when not embedded/serialised - please report this to the developers").setFatal();*/
        }

        return mc;
    }

    /**
     * Convenience accessor for the mapping class of the key mapping for a map of PC keys.
     * Currently only used where the keys are either serialised or embedded into a join table.
     * @param container The container
     * @param mmd MetaData for the field containing the map that this key is for
     * @param dba Database adapter
     * @param clr ClassLoader resolver
     * @return The mapping class
     */
    protected Class getKeyMappingClass(DatastoreContainerObject container,
            AbstractMemberMetaData mmd,
            DatastoreAdapter dba,
            ClassLoaderResolver clr)
    {
        if (mmd.getMap() == null)
        {
            // TODO Localise this
            throw new NucleusException("Attempt to get key mapping for field " + mmd.getFullFieldName() + 
                " that has no map!").setFatal();
        }

        String userMappingClassName = null;
        if (mmd.getKeyMetaData() != null)
        {
            userMappingClassName = mmd.getKeyMetaData().getValueForExtension("mapping-class");
        }
        if (userMappingClassName != null)
        {
            // User has defined their own mapping class for this key so use that
            try
            {
                return clr.classForName(userMappingClassName);
            }
            catch (NucleusException jpe)
            {
                throw new NucleusUserException(LOCALISER.msg("041014", userMappingClassName)).setFatal();
            }
        }

        boolean serialised = (mmd.hasMap() && mmd.getMap().isSerializedKey());
        boolean embedded = (mmd.hasMap() && mmd.getMap().isEmbeddedKey());
        boolean embeddedPC = (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getEmbeddedMetaData() != null);
        boolean keyPC = (mmd.hasMap() && mmd.getMap().keyIsPersistent());
        Class keyCls = clr.classForName(mmd.getMap().getKeyType());
        boolean keyReference = ClassUtils.isReferenceType(keyCls);

        Class mc = null;
        if (serialised)
        {
            if (keyPC)
            {
                // Serialised PC key
                mc = SerialisedKeyPCMapping.class;
            }
            else if (keyReference)
            {
                // Serialised Reference key
                mc = SerialisedReferenceMapping.class;
            }
            else
            {
                // Serialised Non-PC element
                mc = SerialisedMapping.class;
            }
        }
        else if (embedded)
        {
            if (embeddedPC)
            {
                // Embedded PC key
                mc = EmbeddedKeyPCMapping.class;
            }
            else if (keyPC)
            {
                // "Embedded" PC type but no <embedded> so dont embed for now. Is this correct?
                mc = PersistableMapping.class;
            }
            else
            {
                // Embedded Non-PC type
                mc = getMappingClass(keyCls, serialised, embedded, mmd.getFullFieldName());
            }
        }
        else
        {
            // Normal key mapping
            mc = getMappingClass(keyCls, serialised, embedded, mmd.getFullFieldName());
/*            // TODO Allow for other key mappings
            throw new NucleusException("Attempt to get key mapping for field " + mmd.getFullFieldName() + 
                " when not embedded or serialised - please report this to the developers").setFatal();*/
        }

        return mc;
    }

    /**
     * Convenience accessor for the mapping class of the value mapping for a map of values.
     * Currently only used where the value are either serialised or embedded into a join table.
     * @param container The container
     * @param mmd MetaData for the field/property containing the map that this value is for
     * @param dba Database adapter
     * @param clr ClassLoader resolver
     * @return The mapping class
     */
    protected Class getValueMappingClass(DatastoreContainerObject container,
            AbstractMemberMetaData mmd, DatastoreAdapter dba, ClassLoaderResolver clr)
    {
        if (mmd.getMap() == null)
        {
            // TODO Localise this
            throw new NucleusException("Attempt to get value mapping for field " + mmd.getFullFieldName() + 
                " that has no map!").setFatal();
        }

        String userMappingClassName = null;
        if (mmd.getValueMetaData() != null)
        {
            userMappingClassName = mmd.getValueMetaData().getValueForExtension("mapping-class");
        }
        if (userMappingClassName != null)
        {
            // User has defined their own mapping class for this value so use that
            try
            {
                return clr.classForName(userMappingClassName);
            }
            catch (NucleusException jpe)
            {
                throw new NucleusUserException(LOCALISER.msg("041014", userMappingClassName)).setFatal();
            }
        }

        boolean serialised = (mmd.hasMap() && mmd.getMap().isSerializedValue());
        boolean embedded = (mmd.hasMap() && mmd.getMap().isEmbeddedValue());
        boolean embeddedPC = (mmd.getValueMetaData() != null && mmd.getValueMetaData().getEmbeddedMetaData() != null);
        boolean valuePC = (mmd.hasMap() && mmd.getMap().valueIsPersistent());
        Class valueCls = clr.classForName(mmd.getMap().getValueType());
        boolean valueReference = ClassUtils.isReferenceType(valueCls);

        Class mc = null;
        if (serialised)
        {
            if (valuePC)
            {
                // Serialised PC value
                mc = SerialisedValuePCMapping.class;
            }
            else if (valueReference)
            {
                // Serialised Reference value
                mc = SerialisedReferenceMapping.class;
            }
            else
            {
                // Serialised Non-PC element
                mc = SerialisedMapping.class;
            }
        }
        else if (embedded)
        {
            if (embeddedPC)
            {
                // Embedded PC key
                mc = EmbeddedValuePCMapping.class;
            }
            else if (valuePC)
            {
                // "Embedded" PC type but no <embedded> so dont embed for now. Is this correct?
                mc = PersistableMapping.class;
            }
            else
            {
                // Embedded Non-PC type
                mc = getMappingClass(valueCls, serialised, embedded, mmd.getFullFieldName());
            }
        }
        else
        {
            // Normal value mapping
            mc = getMappingClass(valueCls, serialised, embedded, mmd.getFullFieldName());
/*            // TODO Allow for other value mappings
            throw new NucleusException("Attempt to get value mapping for field " + mmd.getFullFieldName() + 
                " when not embedded or serialised - please report this to the developers").setFatal();*/
        }

        return mc;
    }

    /**
     * Method to return the default java type mapping class for a specified java type.
     * @param javaType java type
     * @return The mapping class to use (by default)
     */
    protected Class getDefaultJavaTypeMapping(Class javaType)
    {
        Class cls = storeMgr.getMappedTypeManager().getMappingType(javaType.getName());        
        if (cls == null)
        {
        	NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("041000", javaType.getName()));
        	return null;
        }
        return cls;
    }

    protected static class TypeMapping
    {
        Class javaMappingType;
        
        boolean isDefault;
        
        /**
         * Constructor
         * @param javaMappingType Mapping type to use for thie java type
         * @param isDefault Whether it is the default mapping for this java type
         */
        public TypeMapping(Class javaMappingType, boolean isDefault)
        {
            this.javaMappingType = javaMappingType;
            this.isDefault = isDefault;
        }
        
        /**
         * @return Returns the isDefault.
         */
        public boolean isDefault()
        {
            return isDefault;
        }
        
        /**
         * Mutator for whether this is the default datastore mapping for the mapping
         * @param isDefault Whether it is the default.
         */
        public void setDefault(boolean isDefault)
        {
            this.isDefault = isDefault;
        }
        
        /**
         * @return Returns the mappingType.
         */
        public Class getMappingType()
        {
            return javaMappingType;
        }

        public void setMappingType(Class type)
        {
            javaMappingType = type;
        }
    }
}