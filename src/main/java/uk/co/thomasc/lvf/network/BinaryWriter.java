package uk.co.thomasc.lvf.network;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BinaryWriter {

	OutputStream os;
	ByteArrayOutputStream stream = null;

	public BinaryWriter(ByteArrayOutputStream stream) {
		this((OutputStream) stream);
		this.stream = stream;
	}

	public BinaryWriter(int size) {
		this(new ByteArrayOutputStream(size));
	}

	public BinaryWriter() {
		this(32);
	}

	public BinaryWriter(OutputStream outputStream) {
		os = outputStream;
	}

	public void write(short data) throws IOException {
		final ByteBuffer buffer = ByteBuffer.allocate(2);
		buffer.putShort(data);
		writeR(buffer);
	}

	public void write(int data) throws IOException {
		final ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.putInt(data);
		writeR(buffer);
	}

	public void write(long data) throws IOException {
		final ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putLong(data);
		writeR(buffer);
	}
	
	public void write(String data) throws IOException {
		write(data.getBytes().length);
		write(data.getBytes());
	}

	public byte[] toByteArray() {
		if (stream != null) {
			return stream.toByteArray();
		}
		return null;
	}

	public void writeR(ByteBuffer buffer) throws IOException {
		for (int i = 0; i < buffer.capacity(); i++) {
			write(buffer.get(i));
		}
	}

	public void write(byte[] data) throws IOException {
		os.write(data);
		os.flush();
	}

	public void write(byte data) throws IOException {
		os.write(data);
		os.flush();
	}

	public void flush() {
		try {
			os.flush();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

}
