package uk.co.thomasc.lvf.packets;

import java.io.IOException;

import uk.co.thomasc.lvf.network.BinaryReader;
import uk.co.thomasc.lvf.network.BinaryWriter;

public class Packet2Delete extends TaskPacket {
	
	public Packet2Delete() {
		
	}
	
	public void readPacket(BinaryReader input) throws IOException {
		super.readPacket(input);
	}

	public byte[] getBytes() throws IOException {
		BinaryWriter bw = new BinaryWriter();
		
		bw.write(super.getBytes());
		
		return bw.toByteArray();
	}

	public int getId() {
		return 2;
	}
	
	public int getVersion() {
		return super.getVersion() + 0;
	}
	
}