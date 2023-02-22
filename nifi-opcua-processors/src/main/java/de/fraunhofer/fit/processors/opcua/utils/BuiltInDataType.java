package de.fraunhofer.fit.processors.opcua.utils;


import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;

import java.util.Objects;

public class BuiltInDataType {

    private enum DataType {
        Boolean, Bool,
        Byte,
        Int16,
        UInt16,
        Int32,
        UInt32,
        Int64, Long,
        UInt64, ULong,
        Float,
        Double,
        String;
    }

    /**
     * Factory method to build an object of the specified datatype from its string representation.
     * Unsigned data types are based on the Eclipse Milo library implementation.
     * All the other data types are based on the internal Java representation.
     *
     * @param datatype The string name of the OPC UA Built-in Data Type
     * @param value The string representation of the typed value
     * @return A Java object representing the actual typed implementation of the value
     */
    public static Object build(String datatype, String value) {
        if (value == null)
            return null;
        else if (datatype.equalsIgnoreCase(DataType.Boolean.name()) || datatype.equalsIgnoreCase(DataType.Bool.name()))
            return Boolean.valueOf(value);
        else if (datatype.equalsIgnoreCase(DataType.Byte.name()))
            return Unsigned.ubyte(value);
        else if (datatype.equalsIgnoreCase(DataType.Int16.name()))
            return Short.valueOf(value);
        else if (datatype.equalsIgnoreCase(DataType.UInt16.name()))
            return Unsigned.ushort(value);
        else if (datatype.equalsIgnoreCase(DataType.Int32.name()))
            return Integer.valueOf(value);
        else if (datatype.equalsIgnoreCase(DataType.UInt32.name()))
            return Unsigned.uint(value);
        else if (datatype.equalsIgnoreCase(DataType.Int64.name()) || datatype.equalsIgnoreCase(DataType.Long.name()))
            return Long.valueOf(value);
        else if (datatype.equalsIgnoreCase(DataType.UInt64.name()) || datatype.equalsIgnoreCase(DataType.ULong.name()))
            return Unsigned.ulong(value);
        else if (datatype.equalsIgnoreCase(DataType.Float.name()))
            return Float.valueOf(value);
        else if (datatype.equalsIgnoreCase(DataType.Double.name()))
            return Double.valueOf(value);
        else if (datatype.equalsIgnoreCase(DataType.String.name()))
            return value;
        else
            throw new TypeNotPresentException(datatype, new Exception("Built-in Data Type not found."));
    }
}