package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.sql.parser.OInteger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class OPageChanges {
  private List<OPair<Integer, byte[]>> changes = new ArrayList<>();

  public void setLongValue(ByteBuffer buffer, long value, int offset) {
    final byte[] change = new byte[OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serializeNative(value, change, 0);

    addChanges(buffer, offset, change);
    buffer.putLong(offset, value);
  }

  public void setIntValue(ByteBuffer buffer, int value, int offset) {
    final byte[] change = new byte[OIntegerSerializer.INT_SIZE];
    OIntegerSerializer.INSTANCE.serializeNative(value, change, 0);

    addChanges(buffer, offset, change);
    buffer.putInt(offset, value);
  }

  public void setByteValue(ByteBuffer buffer, byte value, int offset) {
    final byte[] change = new byte[1];

    OIntegerSerializer.INSTANCE.serializeNative(value, change, 0);

    addChanges(buffer, offset, change);
    buffer.put(offset, value);
  }

  public void setBinaryValue(ByteBuffer buffer, byte[] value, int offset) {
    addChanges(buffer, offset, value);
    buffer.position(offset);
    buffer.put(value);
  }

  public void moveData(ByteBuffer buffer, int from, int to, int len) {
    final byte[] change = new byte[len];
    buffer.position(from);
    buffer.get(change);

    addChanges(buffer, to, change);
    buffer.position(to);
    buffer.put(change);
  }

  private void addChanges(ByteBuffer buffer, int position, byte[] change) {
    final byte[] changeChunk = new byte[2 * change.length];

    System.arraycopy(change, 0, changeChunk, 0, change.length);
    buffer.position(position);
    buffer.get(changeChunk, change.length, change.length);

    changes.add(new OPair<>(position, changeChunk));
  }

  public int serializedSize() {
    int size = OIntegerSerializer.INT_SIZE;
    size += 2 * changes.size() * OIntegerSerializer.INT_SIZE;

    for (OPair<Integer, byte[]> ch : changes) {
      size += ch.value.length;
    }

    return size;
  }

  public int toStream(int offset, byte[] stream) {
    OIntegerSerializer.INSTANCE.serializeNative(offset, stream, changes.size());
    offset += OIntegerSerializer.INT_SIZE;

    for (OPair<Integer, byte[]> ch : changes) {
      OIntegerSerializer.INSTANCE.serializeNative(ch.key, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      OIntegerSerializer.INSTANCE.serializeNative(ch.value.length, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(ch.value, 0, stream, offset, ch.value.length);
      offset += ch.value.length;
    }

    return offset;
  }

  public int fromStream(int offset, byte[] stream) {
    int listLength = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int items = 0;
    while (items < listLength) {
      final int pos = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final int chSize = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final byte[] ch = new byte[chSize];
      System.arraycopy(stream, offset, ch, 0, ch.length);
      offset += ch.length;

      changes.add(new OPair<>(pos, ch));
    }

    return offset;
  }

  public void restoreData(ByteBuffer buffer) {
    final ListIterator<OPair<Integer, byte[]>> listIterator = changes.listIterator(changes.size());

    while (listIterator.hasPrevious()) {
      final OPair<Integer, byte[]> pair = listIterator.previous();
      buffer.position(pair.key);
      buffer.put(pair.value, pair.value.length / 2, pair.value.length / 2);
    }
  }

  public void applyData(ByteBuffer buffer) {
    for (OPair<Integer, byte[]> pair : changes) {
      buffer.position(pair.key);
      buffer.put(pair.value, 0, pair.value.length / 2);
    }
  }
}
