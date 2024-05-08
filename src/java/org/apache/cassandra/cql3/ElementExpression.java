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
import java.util.stream.Stream;

import org.apache.cassandra.cql3.terms.Lists;
import org.apache.cassandra.cql3.terms.Maps;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public interface ElementExpression
{
    AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns);
    String toCQLString(Stream<String> columns);
    class UDTFieldExpression implements ElementExpression
    {
        private final FieldIdentifier udtField;

        public UDTFieldExpression(FieldIdentifier udtField)
        {
            this.udtField = udtField;
        }

        public FieldIdentifier element()
        {
            return udtField;
        }

        @Override
        public AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns)
        {
            UserType userType = (UserType) columns.get(0).type;
            int fieldPosition = userType.fieldPosition(udtField);
            if (fieldPosition == -1) {
                throw invalidRequest("Unknown field " + udtField + " for column " + columns.get(0).name);
            }
            return userType.fieldType(fieldPosition);
        }

        @Override
        public String toCQLString(Stream<String> columns)
        {
            //KATE I need the ColumnMetadata to get the type, fix it
            UserType userType = (UserType) columns.get(0).type;
            int fieldPosition = userType.fieldPosition(udtField);
            type = userType.fieldType(fieldPosition).asCQL3Type();
            u = type.toCQLLiteral(udtField.bytes);
            return columns.findFirst().orElseThrow() + '.' + u;
        }
    }
    class CollectionElementExpression implements ElementExpression
    {
        private final Term collectionElement;

        public CollectionElementExpression(Term collectionElement)
        {
            this.collectionElement = collectionElement;
        }

        public Term element()
        {
            return collectionElement;
        }

        @Override
        public AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns) {
            return ((CollectionType<?>) columns.get(0).type).valueComparator();
        }

        @Override
        public String toCQLString(Stream<String> columns)
        {
            // KATE I need the ColumnMetadata to get the type, fix it
            CQL3Type type = ((CollectionType<?>) columns.get(0).type).valueComparator().asCQL3Type();
            // If a Term is not terminal it can be a row marker or a function.
            // We ignore the fact that it could be a function for now.
            String k = collectionElement.isTerminal() ? type.toCQLLiteral(((Term.Terminal) collectionElement).get()) : "?";
            return new StringBuilder().append(columns.findFirst().orElseThrow())
                                      .append('[')
                                      .append(k)
                                      .append(']')
                                      .toString();
        }
    }

    class Raw
    {
        private final Term.Raw rawCollectionElement;

        private final FieldIdentifier udtField;

        Raw(Term.Raw collectionElement, FieldIdentifier udtField)
        {
            this.rawCollectionElement = collectionElement;
            this.udtField = udtField;
        }

        ElementExpression prepare(TableMetadata table, ColumnIdentifier identifier)
        {
            if (rawCollectionElement != null)
                return new CollectionElementExpression(prepareCollectionElement(table, rawCollectionElement, identifier));

            return new UDTFieldExpression(udtField);
        }

        private Term prepareCollectionElement(TableMetadata table, Term.Raw rawCollectionElement, ColumnIdentifier identifier)
        {
            ColumnSpecification elementSpec;
            ColumnMetadata receiver = table.getExistingColumn(identifier);

            switch ((((CollectionType<?>) receiver.type).kind)) {
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

    }

}
