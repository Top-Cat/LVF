package uk.co.thomasc.lvf.packets;

import java.io.IOException;

import uk.co.thomasc.lvf.network.BinaryReader;

public class Packet3Flush extends BusPacket {
	
	public Packet3Flush() {
		
	}
	
	public void readPacket(BinaryReader input) throws IOException {
		super.readPacket(input);
	}

	public byte[] getBytes() throws IOException {
		return super.getBytes();
	}

	public int getId() {
		return 3;
	}
	
	public int getVersion() {
		return super.getVersion() + 0;
	}
	
}