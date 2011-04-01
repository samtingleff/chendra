/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.rubicon.data.thrift.types;

import org.apache.commons.lang.builder.HashCodeBuilder;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

public class TDoubleMap implements TBase<TDoubleMap._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("TDoubleMap");

  private static final TField VALUES_FIELD_DESC = new TField("values", TType.MAP, (short)1);

  private Map<String,Double> values;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    VALUES((short)1, "values");

    private static final Map<Integer, _Fields> byId = new HashMap<Integer, _Fields>();
    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byId.put((int)field._thriftId, field);
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      return byId.get(fieldId);
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments

  public static final Map<_Fields, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new EnumMap<_Fields, FieldMetaData>(_Fields.class) {{
    put(_Fields.VALUES, new FieldMetaData("values", TFieldRequirementType.DEFAULT, 
        new MapMetaData(TType.MAP, 
            new FieldValueMetaData(TType.STRING), 
            new FieldValueMetaData(TType.DOUBLE))));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(TDoubleMap.class, metaDataMap);
  }

  public TDoubleMap() {
  }

  public TDoubleMap(
    Map<String,Double> values)
  {
    this();
    this.values = values;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TDoubleMap(TDoubleMap other) {
    if (other.isSetValues()) {
      Map<String,Double> __this__values = new HashMap<String,Double>();
      for (Map.Entry<String, Double> other_element : other.values.entrySet()) {

        String other_element_key = other_element.getKey();
        Double other_element_value = other_element.getValue();

        String __this__values_copy_key = other_element_key;

        Double __this__values_copy_value = other_element_value;

        __this__values.put(__this__values_copy_key, __this__values_copy_value);
      }
      this.values = __this__values;
    }
  }

  public TDoubleMap deepCopy() {
    return new TDoubleMap(this);
  }

  @Deprecated
  public TDoubleMap clone() {
    return new TDoubleMap(this);
  }

  public int getValuesSize() {
    return (this.values == null) ? 0 : this.values.size();
  }

  public void putToValues(String key, double val) {
    if (this.values == null) {
      this.values = new HashMap<String,Double>();
    }
    this.values.put(key, val);
  }

  public Map<String,Double> getValues() {
    return this.values;
  }

  public TDoubleMap setValues(Map<String,Double> values) {
    this.values = values;
    return this;
  }

  public void unsetValues() {
    this.values = null;
  }

  /** Returns true if field values is set (has been asigned a value) and false otherwise */
  public boolean isSetValues() {
    return this.values != null;
  }

  public void setValuesIsSet(boolean value) {
    if (!value) {
      this.values = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case VALUES:
      if (value == null) {
        unsetValues();
      } else {
        setValues((Map<String,Double>)value);
      }
      break;

    }
  }

  public void setFieldValue(int fieldID, Object value) {
    setFieldValue(_Fields.findByThriftIdOrThrow(fieldID), value);
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case VALUES:
      return getValues();

    }
    throw new IllegalStateException();
  }

  public Object getFieldValue(int fieldId) {
    return getFieldValue(_Fields.findByThriftIdOrThrow(fieldId));
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    switch (field) {
    case VALUES:
      return isSetValues();
    }
    throw new IllegalStateException();
  }

  public boolean isSet(int fieldID) {
    return isSet(_Fields.findByThriftIdOrThrow(fieldID));
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TDoubleMap)
      return this.equals((TDoubleMap)that);
    return false;
  }

  public boolean equals(TDoubleMap that) {
    if (that == null)
      return false;

    boolean this_present_values = true && this.isSetValues();
    boolean that_present_values = true && that.isSetValues();
    if (this_present_values || that_present_values) {
      if (!(this_present_values && that_present_values))
        return false;
      if (!this.values.equals(that.values))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_values = true && (isSetValues());
    builder.append(present_values);
    if (present_values)
      builder.append(values);

    return builder.toHashCode();
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // VALUES
          if (field.type == TType.MAP) {
            {
              TMap _map10 = iprot.readMapBegin();
              this.values = new HashMap<String,Double>(2*_map10.size);
              for (int _i11 = 0; _i11 < _map10.size; ++_i11)
              {
                String _key12;
                double _val13;
                _key12 = iprot.readString();
                _val13 = iprot.readDouble();
                this.values.put(_key12, _val13);
              }
              iprot.readMapEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.values != null) {
      oprot.writeFieldBegin(VALUES_FIELD_DESC);
      {
        oprot.writeMapBegin(new TMap(TType.STRING, TType.DOUBLE, this.values.size()));
        for (Map.Entry<String, Double> _iter14 : this.values.entrySet())
        {
          oprot.writeString(_iter14.getKey());
          oprot.writeDouble(_iter14.getValue());
        }
        oprot.writeMapEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TDoubleMap(");
    boolean first = true;

    sb.append("values:");
    if (this.values == null) {
      sb.append("null");
    } else {
      sb.append(this.values);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }

}
