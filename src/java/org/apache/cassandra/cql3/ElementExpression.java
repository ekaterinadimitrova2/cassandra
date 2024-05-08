/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3;

import java.util.List;
import java.util.Objects;

import org.apache.cassandra.cql3.terms.Lists;
import org.apache.cassandra.cql3.terms.Maps;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public final class ElementExpression
{
    public enum Kind
    {
        UDT_FIELD
        {
            @Override
            public AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression.Raw element)
            {
                UserType userType = (UserType) columns.get(0).type;
                int fieldPosition = userType.fieldPosition(element.udtField);
                if (fieldPosition == -1)
                    throw invalidRequest("Unknown field " + element.udtField + " for UDT " + columns.get(0).name);

                return userType.fieldType(fieldPosition);
            }

            @Override
            public String toCQLString(String element)
            {
                return '.' + element;
            }

            @Override
            public String toString()
            {
                return "UDT field";
            }
        },
        COLLECTION_ELEMENT
        {
            @Override
            public AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression.Raw element)
            {
                CollectionType<?> collectionType = (CollectionType<?>) columns.get(0).type;
                return collectionType.valueComparator();
            }

            @Override
            public String toCQLString(String element)
            {
                return '[' + element + ']';
            }

            @Override
            public String toString()
            {
                return "collection element";
            }
        };

        public abstract AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression.Raw element);
        public abstract String toCQLString(String element);
    }

    /**
     * The kind of element expression.
     */
    private final ElementExpression.Kind kind;

    /**
     * The type represented by this expression:
     *  - for a single column the type of the expression will be the one of the column
     *  - for a map element expression the type will be the one of the map value
     *  - for a multi-column expression the type will be a tuple type
     *  - for a collection element expression the type will be the one of the collection elements
     *  - for a UDT field expression the type will be the one of the UDT field
     */
    private final AbstractType<?> type;
    private final FieldIdentifier fieldIdentifier;
    private final Term collectionElement;

    ElementExpression(ElementExpression.Kind kind, AbstractType<?> type, FieldIdentifier udtField, Term collectionElement)
    {
        this.kind = kind;
        this.type = type;
        this.fieldIdentifier = udtField;
        this.collectionElement = collectionElement;
    }

    /**
     * Returns the expression kind.
     * @return the expression kind.
     */
    public ElementExpression.Kind kind()
    {
        return kind;
    }

    /**
     * Returns the expression UDT field in case of a UDT field expression.
     * @return the expression UDT field.
     */
    public FieldIdentifier fieldIdentifier()
    {
        return fieldIdentifier;
    }

    /**
     * Returns the expression element type.
     * @return type.
     */
    public AbstractType<?> type()
    {
        return type;
    }

    /**
     * Returns the expression collection element in case of a collection element expression.
     * @return the expression collection element.
     */
    public Term collectionElement()
    {
        return collectionElement;
    }

    @Override
    public String toString()
    {
        return this.kind.toString();
    }

    public static final class Raw
    {
        private final Kind kind;
        private final Term.Raw rawCollectionElement;

        private final FieldIdentifier udtField;

        Raw(Term.Raw collectionElement, FieldIdentifier udtField, Kind kind)
        {
            this.rawCollectionElement = collectionElement;
            this.udtField = udtField;
            this.kind = kind;
        }

        ElementExpression prepare(TableMetadata table, ColumnIdentifier identifier, AbstractType<?> type)
        {
            if (rawCollectionElement != null)
                return new ElementExpression(Kind.COLLECTION_ELEMENT, type, null, prepareCollectionElement(table, rawCollectionElement, identifier));

            return new ElementExpression(Kind.UDT_FIELD, type, udtField, null);
        }

        private Term prepareCollectionElement(TableMetadata table, Term.Raw rawCollectionElement, ColumnIdentifier identifier)
        {
            ColumnSpecification elementSpec;
            ColumnMetadata receiver = table.getExistingColumn(identifier);

            switch ((((CollectionType<?>) receiver.type).kind))
            {
                case LIST:
                    elementSpec = Lists.indexSpecOf(receiver);
                    break;
                case MAP:
                    elementSpec = Maps.keySpecOf(receiver);
                    break;
                case SET:
                    throw invalidRequest("Invalid element access syntax for set column %s", receiver.name);
                default:
                    throw new AssertionError();
            }

            return rawCollectionElement.prepare(table.keyspace, elementSpec);
        }

        /**
         * Returns the collection element if this is a collection element expression, {@code null} otherwise.
         * @return rawCollectionElement.
         */
        public Term.Raw rawCollectionElement()
        {
            return rawCollectionElement;
        }

        /**
         * Returns the collection element if this is a collection element expression, {@code null} otherwise.
         * @return rawCollectionElement.
         */
        public FieldIdentifier rawUdtField()
        {
            return udtField;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rawCollectionElement, udtField);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof ElementExpression.Raw))
                return false;

            ElementExpression.Raw r = (ElementExpression.Raw) o;
            return Objects.equals(rawCollectionElement, r.rawCollectionElement) && Objects.equals(udtField, r.udtField);
        }

        public Kind kind()
        {
            return kind;
        }

        @Override
        public String toString()
        {
            return this.kind.toString();
        }
    }
}
