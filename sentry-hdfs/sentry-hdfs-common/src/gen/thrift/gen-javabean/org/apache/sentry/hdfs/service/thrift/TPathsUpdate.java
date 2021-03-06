/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.sentry.hdfs.service.thrift;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
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
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPathsUpdate implements org.apache.thrift.TBase<TPathsUpdate, TPathsUpdate._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TPathsUpdate");

  private static final org.apache.thrift.protocol.TField HAS_FULL_IMAGE_FIELD_DESC = new org.apache.thrift.protocol.TField("hasFullImage", org.apache.thrift.protocol.TType.BOOL, (short)1);
  private static final org.apache.thrift.protocol.TField PATHS_DUMP_FIELD_DESC = new org.apache.thrift.protocol.TField("pathsDump", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField SEQ_NUM_FIELD_DESC = new org.apache.thrift.protocol.TField("seqNum", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField PATH_CHANGES_FIELD_DESC = new org.apache.thrift.protocol.TField("pathChanges", org.apache.thrift.protocol.TType.LIST, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TPathsUpdateStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TPathsUpdateTupleSchemeFactory());
  }

  private boolean hasFullImage; // required
  private TPathsDump pathsDump; // optional
  private long seqNum; // required
  private List<TPathChanges> pathChanges; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    HAS_FULL_IMAGE((short)1, "hasFullImage"),
    PATHS_DUMP((short)2, "pathsDump"),
    SEQ_NUM((short)3, "seqNum"),
    PATH_CHANGES((short)4, "pathChanges");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // HAS_FULL_IMAGE
          return HAS_FULL_IMAGE;
        case 2: // PATHS_DUMP
          return PATHS_DUMP;
        case 3: // SEQ_NUM
          return SEQ_NUM;
        case 4: // PATH_CHANGES
          return PATH_CHANGES;
        default:
          return null;
      }
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
  private static final int __HASFULLIMAGE_ISSET_ID = 0;
  private static final int __SEQNUM_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  private _Fields optionals[] = {_Fields.PATHS_DUMP};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.HAS_FULL_IMAGE, new org.apache.thrift.meta_data.FieldMetaData("hasFullImage", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.PATHS_DUMP, new org.apache.thrift.meta_data.FieldMetaData("pathsDump", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TPathsDump.class)));
    tmpMap.put(_Fields.SEQ_NUM, new org.apache.thrift.meta_data.FieldMetaData("seqNum", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.PATH_CHANGES, new org.apache.thrift.meta_data.FieldMetaData("pathChanges", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TPathChanges.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TPathsUpdate.class, metaDataMap);
  }

  public TPathsUpdate() {
  }

  public TPathsUpdate(
    boolean hasFullImage,
    long seqNum,
    List<TPathChanges> pathChanges)
  {
    this();
    this.hasFullImage = hasFullImage;
    setHasFullImageIsSet(true);
    this.seqNum = seqNum;
    setSeqNumIsSet(true);
    this.pathChanges = pathChanges;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TPathsUpdate(TPathsUpdate other) {
    __isset_bitfield = other.__isset_bitfield;
    this.hasFullImage = other.hasFullImage;
    if (other.isSetPathsDump()) {
      this.pathsDump = new TPathsDump(other.pathsDump);
    }
    this.seqNum = other.seqNum;
    if (other.isSetPathChanges()) {
      List<TPathChanges> __this__pathChanges = new ArrayList<TPathChanges>();
      for (TPathChanges other_element : other.pathChanges) {
        __this__pathChanges.add(new TPathChanges(other_element));
      }
      this.pathChanges = __this__pathChanges;
    }
  }

  public TPathsUpdate deepCopy() {
    return new TPathsUpdate(this);
  }

  @Override
  public void clear() {
    setHasFullImageIsSet(false);
    this.hasFullImage = false;
    this.pathsDump = null;
    setSeqNumIsSet(false);
    this.seqNum = 0;
    this.pathChanges = null;
  }

  public boolean isHasFullImage() {
    return this.hasFullImage;
  }

  public void setHasFullImage(boolean hasFullImage) {
    this.hasFullImage = hasFullImage;
    setHasFullImageIsSet(true);
  }

  public void unsetHasFullImage() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __HASFULLIMAGE_ISSET_ID);
  }

  /** Returns true if field hasFullImage is set (has been assigned a value) and false otherwise */
  public boolean isSetHasFullImage() {
    return EncodingUtils.testBit(__isset_bitfield, __HASFULLIMAGE_ISSET_ID);
  }

  public void setHasFullImageIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __HASFULLIMAGE_ISSET_ID, value);
  }

  public TPathsDump getPathsDump() {
    return this.pathsDump;
  }

  public void setPathsDump(TPathsDump pathsDump) {
    this.pathsDump = pathsDump;
  }

  public void unsetPathsDump() {
    this.pathsDump = null;
  }

  /** Returns true if field pathsDump is set (has been assigned a value) and false otherwise */
  public boolean isSetPathsDump() {
    return this.pathsDump != null;
  }

  public void setPathsDumpIsSet(boolean value) {
    if (!value) {
      this.pathsDump = null;
    }
  }

  public long getSeqNum() {
    return this.seqNum;
  }

  public void setSeqNum(long seqNum) {
    this.seqNum = seqNum;
    setSeqNumIsSet(true);
  }

  public void unsetSeqNum() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __SEQNUM_ISSET_ID);
  }

  /** Returns true if field seqNum is set (has been assigned a value) and false otherwise */
  public boolean isSetSeqNum() {
    return EncodingUtils.testBit(__isset_bitfield, __SEQNUM_ISSET_ID);
  }

  public void setSeqNumIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __SEQNUM_ISSET_ID, value);
  }

  public int getPathChangesSize() {
    return (this.pathChanges == null) ? 0 : this.pathChanges.size();
  }

  public java.util.Iterator<TPathChanges> getPathChangesIterator() {
    return (this.pathChanges == null) ? null : this.pathChanges.iterator();
  }

  public void addToPathChanges(TPathChanges elem) {
    if (this.pathChanges == null) {
      this.pathChanges = new ArrayList<TPathChanges>();
    }
    this.pathChanges.add(elem);
  }

  public List<TPathChanges> getPathChanges() {
    return this.pathChanges;
  }

  public void setPathChanges(List<TPathChanges> pathChanges) {
    this.pathChanges = pathChanges;
  }

  public void unsetPathChanges() {
    this.pathChanges = null;
  }

  /** Returns true if field pathChanges is set (has been assigned a value) and false otherwise */
  public boolean isSetPathChanges() {
    return this.pathChanges != null;
  }

  public void setPathChangesIsSet(boolean value) {
    if (!value) {
      this.pathChanges = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case HAS_FULL_IMAGE:
      if (value == null) {
        unsetHasFullImage();
      } else {
        setHasFullImage((Boolean)value);
      }
      break;

    case PATHS_DUMP:
      if (value == null) {
        unsetPathsDump();
      } else {
        setPathsDump((TPathsDump)value);
      }
      break;

    case SEQ_NUM:
      if (value == null) {
        unsetSeqNum();
      } else {
        setSeqNum((Long)value);
      }
      break;

    case PATH_CHANGES:
      if (value == null) {
        unsetPathChanges();
      } else {
        setPathChanges((List<TPathChanges>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case HAS_FULL_IMAGE:
      return Boolean.valueOf(isHasFullImage());

    case PATHS_DUMP:
      return getPathsDump();

    case SEQ_NUM:
      return Long.valueOf(getSeqNum());

    case PATH_CHANGES:
      return getPathChanges();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case HAS_FULL_IMAGE:
      return isSetHasFullImage();
    case PATHS_DUMP:
      return isSetPathsDump();
    case SEQ_NUM:
      return isSetSeqNum();
    case PATH_CHANGES:
      return isSetPathChanges();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TPathsUpdate)
      return this.equals((TPathsUpdate)that);
    return false;
  }

  public boolean equals(TPathsUpdate that) {
    if (that == null)
      return false;

    boolean this_present_hasFullImage = true;
    boolean that_present_hasFullImage = true;
    if (this_present_hasFullImage || that_present_hasFullImage) {
      if (!(this_present_hasFullImage && that_present_hasFullImage))
        return false;
      if (this.hasFullImage != that.hasFullImage)
        return false;
    }

    boolean this_present_pathsDump = true && this.isSetPathsDump();
    boolean that_present_pathsDump = true && that.isSetPathsDump();
    if (this_present_pathsDump || that_present_pathsDump) {
      if (!(this_present_pathsDump && that_present_pathsDump))
        return false;
      if (!this.pathsDump.equals(that.pathsDump))
        return false;
    }

    boolean this_present_seqNum = true;
    boolean that_present_seqNum = true;
    if (this_present_seqNum || that_present_seqNum) {
      if (!(this_present_seqNum && that_present_seqNum))
        return false;
      if (this.seqNum != that.seqNum)
        return false;
    }

    boolean this_present_pathChanges = true && this.isSetPathChanges();
    boolean that_present_pathChanges = true && that.isSetPathChanges();
    if (this_present_pathChanges || that_present_pathChanges) {
      if (!(this_present_pathChanges && that_present_pathChanges))
        return false;
      if (!this.pathChanges.equals(that.pathChanges))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_hasFullImage = true;
    builder.append(present_hasFullImage);
    if (present_hasFullImage)
      builder.append(hasFullImage);

    boolean present_pathsDump = true && (isSetPathsDump());
    builder.append(present_pathsDump);
    if (present_pathsDump)
      builder.append(pathsDump);

    boolean present_seqNum = true;
    builder.append(present_seqNum);
    if (present_seqNum)
      builder.append(seqNum);

    boolean present_pathChanges = true && (isSetPathChanges());
    builder.append(present_pathChanges);
    if (present_pathChanges)
      builder.append(pathChanges);

    return builder.toHashCode();
  }

  public int compareTo(TPathsUpdate other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TPathsUpdate typedOther = (TPathsUpdate)other;

    lastComparison = Boolean.valueOf(isSetHasFullImage()).compareTo(typedOther.isSetHasFullImage());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHasFullImage()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.hasFullImage, typedOther.hasFullImage);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPathsDump()).compareTo(typedOther.isSetPathsDump());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPathsDump()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.pathsDump, typedOther.pathsDump);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSeqNum()).compareTo(typedOther.isSetSeqNum());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSeqNum()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.seqNum, typedOther.seqNum);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPathChanges()).compareTo(typedOther.isSetPathChanges());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPathChanges()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.pathChanges, typedOther.pathChanges);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TPathsUpdate(");
    boolean first = true;

    sb.append("hasFullImage:");
    sb.append(this.hasFullImage);
    first = false;
    if (isSetPathsDump()) {
      if (!first) sb.append(", ");
      sb.append("pathsDump:");
      if (this.pathsDump == null) {
        sb.append("null");
      } else {
        sb.append(this.pathsDump);
      }
      first = false;
    }
    if (!first) sb.append(", ");
    sb.append("seqNum:");
    sb.append(this.seqNum);
    first = false;
    if (!first) sb.append(", ");
    sb.append("pathChanges:");
    if (this.pathChanges == null) {
      sb.append("null");
    } else {
      sb.append(this.pathChanges);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetHasFullImage()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'hasFullImage' is unset! Struct:" + toString());
    }

    if (!isSetSeqNum()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'seqNum' is unset! Struct:" + toString());
    }

    if (!isSetPathChanges()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'pathChanges' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (pathsDump != null) {
      pathsDump.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TPathsUpdateStandardSchemeFactory implements SchemeFactory {
    public TPathsUpdateStandardScheme getScheme() {
      return new TPathsUpdateStandardScheme();
    }
  }

  private static class TPathsUpdateStandardScheme extends StandardScheme<TPathsUpdate> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TPathsUpdate struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // HAS_FULL_IMAGE
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.hasFullImage = iprot.readBool();
              struct.setHasFullImageIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // PATHS_DUMP
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.pathsDump = new TPathsDump();
              struct.pathsDump.read(iprot);
              struct.setPathsDumpIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SEQ_NUM
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.seqNum = iprot.readI64();
              struct.setSeqNumIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // PATH_CHANGES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list50 = iprot.readListBegin();
                struct.pathChanges = new ArrayList<TPathChanges>(_list50.size);
                for (int _i51 = 0; _i51 < _list50.size; ++_i51)
                {
                  TPathChanges _elem52; // required
                  _elem52 = new TPathChanges();
                  _elem52.read(iprot);
                  struct.pathChanges.add(_elem52);
                }
                iprot.readListEnd();
              }
              struct.setPathChangesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TPathsUpdate struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(HAS_FULL_IMAGE_FIELD_DESC);
      oprot.writeBool(struct.hasFullImage);
      oprot.writeFieldEnd();
      if (struct.pathsDump != null) {
        if (struct.isSetPathsDump()) {
          oprot.writeFieldBegin(PATHS_DUMP_FIELD_DESC);
          struct.pathsDump.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldBegin(SEQ_NUM_FIELD_DESC);
      oprot.writeI64(struct.seqNum);
      oprot.writeFieldEnd();
      if (struct.pathChanges != null) {
        oprot.writeFieldBegin(PATH_CHANGES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.pathChanges.size()));
          for (TPathChanges _iter53 : struct.pathChanges)
          {
            _iter53.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TPathsUpdateTupleSchemeFactory implements SchemeFactory {
    public TPathsUpdateTupleScheme getScheme() {
      return new TPathsUpdateTupleScheme();
    }
  }

  private static class TPathsUpdateTupleScheme extends TupleScheme<TPathsUpdate> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TPathsUpdate struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeBool(struct.hasFullImage);
      oprot.writeI64(struct.seqNum);
      {
        oprot.writeI32(struct.pathChanges.size());
        for (TPathChanges _iter54 : struct.pathChanges)
        {
          _iter54.write(oprot);
        }
      }
      BitSet optionals = new BitSet();
      if (struct.isSetPathsDump()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetPathsDump()) {
        struct.pathsDump.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TPathsUpdate struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.hasFullImage = iprot.readBool();
      struct.setHasFullImageIsSet(true);
      struct.seqNum = iprot.readI64();
      struct.setSeqNumIsSet(true);
      {
        org.apache.thrift.protocol.TList _list55 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.pathChanges = new ArrayList<TPathChanges>(_list55.size);
        for (int _i56 = 0; _i56 < _list55.size; ++_i56)
        {
          TPathChanges _elem57; // required
          _elem57 = new TPathChanges();
          _elem57.read(iprot);
          struct.pathChanges.add(_elem57);
        }
      }
      struct.setPathChangesIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.pathsDump = new TPathsDump();
        struct.pathsDump.read(iprot);
        struct.setPathsDumpIsSet(true);
      }
    }
  }

}

