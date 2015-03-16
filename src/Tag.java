package org.hbase.async;

import java.util.ArrayList;
import java.util.List;

import org.hbase.async.Bytes;

/**
 * Tags are part of cells and is used to add meta-data about Key-Values.
 * Tags can be used to specify cell TTL (Time-to-Live), etc.
 */
public class Tag {

  public static final byte ACL_TAG_TYPE = (byte) 1;
  public static final byte VISIBILITY_TAG_TYPE = (byte) 2;
  // public static final byte LOG_REPLAY_TAG_TYPE = (byte) 3; // deprecated
  public static final byte VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE = (byte)4;
  public static final byte STRING_VIS_TAG_TYPE = (byte) 7;
  public static final byte TTL_TAG_TYPE = (byte)8;
  
  public Tag(byte tagType, String tag) {
    this(tagType, tag.getBytes());
  }

  /**
   * @param tag_type
   * @param tag_data
   */
  public Tag(byte tag_type, byte[] tag_data) {
    this.tag_type = tag_type;
    if (tag_data.length > MAX_TAG_DATA_LENGTH) {
      throw new IllegalArgumentException(
          "Tag data exceeds max length: " + MAX_TAG_DATA_LENGTH);
    }
    tag_length = TAG_HEADER_SIZE + tag_data.length;
    byte_array = new byte[tag_length];
    Bytes.setShort(byte_array, (byte)(tag_data.length + TAG_TYPE_HEADER_SIZE), 0);
    Bytes.setByte(byte_array, tag_type, 2);
    Bytes.setBytes(byte_array, tag_data, 3);
  }

  /**
   * Creates a Tag from the specified byte array and offset. Presumes
   * <code>bytes</code> content starting at <code>offset</code> is formatted as
   * a Tag blob.
   * The bytes to include the tag type, tag length and actual tag bytes.
   * @param bytes
   *          byte array
   * @param offset
   *          offset to start of Tag
   */
  public Tag(byte[] bytes, int offset) {
    this(bytes, offset, getLength(bytes, offset));
  }

  /**
   * Creates a Tag from the specified byte array, starting at offset, and for length
   * <code>length</code>. Presumes <code>bytes</code> content starting at <code>offset</code> is
   * formatted as a Tag blob.
   * @param bytes
   *          byte array
   * @param offset
   *          offset to start of the Tag
   * @param length
   *          length of the Tag
   */
  public Tag(byte[] bytes, int offset, int length) {
    if (length > MAX_TAG_DATA_LENGTH) {
      throw new IllegalArgumentException(
          "Invalid tag data being passed. Its length can not exceed " + MAX_TAG_DATA_LENGTH);
    }
    this.byte_array = bytes;
    this.tag_offset_in_byte_array = offset;
    this.tag_length = length;
    this.tag_type = bytes[offset + TAG_DATA_LENGTH_HEADER_SIZE];
  }

  /**
   * @return The byte array backing this Tag.
   */
  public byte[] getBackingByteArray() {
    return this.byte_array;
  }

  /**
   * @return the tag type
   */
  public byte getType() {
    return this.tag_type;
  }

  /**
   * @return Offset of actual tag bytes within the backed buffer
   */
  public int getTagOffset() {
    return this.tag_offset_in_byte_array + TAG_HEADER_SIZE;
  }

  /**
   * Creates the list of tags from the byte array b. Expected that b is in the
   * expected tag format
   * @param b
   * @param offset
   * @param length
   * @return List of tags
   */
  public static List<Tag> tagsFromByteArray(byte[] b, int offset, int length) {
    List<Tag> tags = new ArrayList<Tag>();
    int pos = offset;
    while (pos < offset + length) {
      int tag_len = Bytes.getShort(b, pos); // FIXME: Possible unsigned issues for length > Short.MAX_VALUE
      tags.add(new Tag(b, pos, tag_len + TAG_DATA_LENGTH_HEADER_SIZE));
      pos += TAG_DATA_LENGTH_HEADER_SIZE + tag_len;
    }
    return tags;
  }

  /**
   * Validates the integrity of tags encoded in a byte array
   * @param b
   */
  public static void checkTag(final byte b[]) {
    int length = b.length;
    int pos = 0;
    while (pos < length) {
        int tag_len = Bytes.getShort(b, pos); // FIXME: Possible unsigned issues for length > Short.MAX_VALUE
        if (length > MAX_TAG_DATA_LENGTH) {
            throw new IllegalArgumentException(
                "Invalid tag data being passed. Its length can not exceed " + MAX_TAG_DATA_LENGTH);
          }
        // TODO: Verify valid tag-type here?
        pos += tag_len + TAG_DATA_LENGTH_HEADER_SIZE;
        if (pos > length) {
            throw new IllegalArgumentException(
                "Invalid tag data being passed. Encoded tag exceeds length of backing byte array");
        }
    }
  }
  
  /**
   * Write a list of tags into a byte array
   * @param tags
   * @return the serialized tag data as bytes
   */
  public static byte[] tagsToByteArray(List<Tag> tags) {
    int length = 0;
    for (Tag tag: tags) {
      length += tag.tag_length;
    }
    byte[] b = new byte[length];
    int pos = 0;
    for (Tag tag: tags) {
      System.arraycopy(tag.byte_array, tag.tag_offset_in_byte_array, b, pos, tag.tag_length);
      pos += tag.tag_length;
    }
    return b;
  }

  /**
   * Returns the total length of the entire tag entity
   */
  int getLength() {
    return this.tag_length;
  }

  private static int getLength(byte[] bytes, int offset) {
    return TAG_DATA_LENGTH_HEADER_SIZE + Bytes.getInt(bytes, offset);
  }
  
  /**
   * Returns the offset of the entire tag entity
   */
  int getOffset() {
    return this.tag_offset_in_byte_array;
  }

  final static int TAG_DATA_LENGTH_HEADER_SIZE = 2;
  final static int TAG_TYPE_HEADER_SIZE = 1;
  final static int TAG_HEADER_SIZE = TAG_TYPE_HEADER_SIZE + TAG_DATA_LENGTH_HEADER_SIZE;
  static final int MAX_TAG_DATA_LENGTH = (2 * Short.MAX_VALUE) + 1 - TAG_TYPE_HEADER_SIZE;

  final byte tag_type;
  int tag_length = 0;
  final byte[] byte_array;
  int tag_offset_in_byte_array = 0;

}