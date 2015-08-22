package uk.co.thomasc.lvf.network;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class BinaryReader {

	public static long LongMaxValue = 0xFFFFFFFFFFFFFFFFL;

	InputStream reader;

	public BinaryReader(InputStream stream) {
		reader = stream;
	}

	public BinaryReader(byte[] data) {
		reader = new ByteArrayInputStream(data);
	}

	public long readLong() throws IOException {
		return getBuffer(8).getLong();
	}

	private ByteBuffer getBuffer(int size) throws IOException {
		final byte[] buffer = new byte[size];
		for (int i = 1; i <= size; i++) {
			buffer[i - 1] = (byte) reader.read();
		}
		return ByteBuffer.wrap(buffer);
	}

	public int readInt() throws IOException {
		return getBuffer(4).getInt();
	}

	public short readShort() throws IOException {
		return getBuffer(2).getShort();
	}

	public byte readByte() throws IOException {
		return (byte) reader.read();
	}

	public byte[] readBytes(int length) throws IOException {
		byte[] bytes = new byte[length];
		reader.read(bytes);
		return bytes;
	}

	public boolean isAtEnd() throws IOException {
		return reader.available() == 0;
	}

	public byte[] readBytes() throws IOException {
		return readBytes(reader.available());
	}

	public float readFloat() throws IOException {
		return getBuffer(4).getFloat();
	}

	public String readString() throws IOException {
		int strLength = readInt();
		return new String(readBytes(strLength));
	}

}
