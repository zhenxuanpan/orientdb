package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 12/12/14
 */
public class ONonTxOperationPerformedWALRecord extends OAbstractWALRecord {
  private OLogSequenceNumber prevLsn;

  public ONonTxOperationPerformedWALRecord() {
  }

  public ONonTxOperationPerformedWALRecord(OLogSequenceNumber prevLsn) {
    this.prevLsn = prevLsn;
  }

  public OLogSequenceNumber getPrevLsn() {
    return prevLsn;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(prevLsn.getSegment(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(prevLsn.getPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    long segment = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    prevLsn = new OLogSequenceNumber(segment, position);

    return offset;
  }

  @Override
  public int serializedSize() {
    return 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }
}