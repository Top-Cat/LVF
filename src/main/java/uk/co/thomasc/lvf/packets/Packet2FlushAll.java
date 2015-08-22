package uk.co.thomasc.lvf.packets;

import java.io.IOException;

import uk.co.thomasc.lvf.network.BinaryReader;

public class Packet2FlushAll extends TaskPacket {
	
	public Packet2FlushAll() {
		
	}
	
	public void readPacket(BinaryReader input) throws IOException {
		super.readPacket(input);
	}

	public byte[] getBytes() throws IOException {
		return super.getBytes();
	}

	public int getId() {
		return 2;
	}
	
	public int getVersion() {
		return super.getVersion() + 0;
	}
	
}