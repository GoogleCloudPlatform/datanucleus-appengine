/**********************************************************************
Copyright (c) 2003 Erik Bengtson and others. All rights reserved. 
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
2003 Andy Jefferson - converted to use Reflection
2004 Andy Jefferson - output targetException when Invocation error
    ...
**********************************************************************/
package org.datanucleus.store.mapped.mapping;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.mapped.DatastoreAdapter;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.util.Localiser;

/**
 * Factory class for creating Mapping instances.
 * This is called to generate field mappings for the classes to be persisted.
 */
public final class MappingFactory
{
    private static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.Localisation", org.datanucleus.ClassConstants.NUCLEUS_CONTEXT_LOADER);

    /** Private constructor to prevent instantiation. */
    private MappingFactory()
    {
    }

    /**
     * Get a new instance of the Mapping using the StoreManager and type.
     * This creates a mapping that doesn't have its field/table set and typically used to represent a
     * parameter in a query.
     * @param mappingClass the Mapping class to be created
     * @param storeMgr The StoreManager
     * @param type The type
     * @return The Mapping
     */
    protected static JavaTypeMapping createMapping(Class mappingClass, MappedStoreManager storeMgr, String type)
    {
    	JavaTypeMapping mapping = null;
        try
        {
        	mapping = (JavaTypeMapping)mappingClass.newInstance();
        }
        catch (Exception e)
        {
            throw new NucleusException(LOCALISER.msg("041009", mappingClass.getName(), e), e).setFatal();
        }
        mapping.initialize(storeMgr, type);
        return mapping;
    }

    /**
     * Get a new instance of the Mapping providing full field/property details and the role of this
     * mapping within that field.
     * @param mappingClass the Mapping class to be created
     * @param dba Datastore Adapter
     * @param mmd MetaData for the field/property to be mapped
     * @param datastoreContainer The Table
     * @param clr The ClassLoaderResolver
     * @return The Mapping
     */
    public static JavaTypeMapping createMapping(Class mappingClass, DatastoreAdapter dba, 
            AbstractMemberMetaData mmd, int roleForField, DatastoreContainerObject datastoreContainer, 
            ClassLoaderResolver clr)
    {
        JavaTypeMapping mapping = null;
        try
        {
        	mapping = (JavaTypeMapping)mappingClass.newInstance();
        }
        catch (Exception e)
        {
            throw new NucleusException(LOCALISER.msg("041009", mappingClass.getName(), e), e).setFatal();
        }

        if (roleForField >= 0)
        {
            mapping.setRoleForMember(roleForField);
        }
        mapping.initialize(mmd, datastoreContainer, clr);

        return mapping;
    }
}