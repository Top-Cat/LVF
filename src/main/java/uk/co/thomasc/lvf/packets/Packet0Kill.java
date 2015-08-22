package uk.co.thomasc.lvf.packets;

import uk.co.thomasc.lvf.network.BinaryReader;

public class Packet0Kill implements Packet {
	
	public Packet0Kill() {
		
	}
	
	public void readPacket(BinaryReader input) {
		
	}

	public byte[] getBytes() {
		return new byte[0];
	}

	public int getId() {
		return 0;
	}
	
	public int getVersion() {
		return 0;
	}
	
}