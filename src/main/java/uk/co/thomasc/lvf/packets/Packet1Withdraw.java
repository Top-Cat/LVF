package uk.co.thomasc.lvf.packets;

import java.io.IOException;

import uk.co.thomasc.lvf.network.BinaryReader;
import uk.co.thomasc.lvf.network.BinaryWriter;

public class Packet1Withdraw extends TaskPacket {
	
	public Packet1Withdraw() {
		
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
		return 1;
	}
	
	public int getVersion() {
		return super.getVersion() + 0;
	}
	
}