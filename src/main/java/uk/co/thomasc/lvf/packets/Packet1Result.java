package uk.co.thomasc.lvf.packets;

import java.io.IOException;

import lombok.Getter;
import lombok.Setter;
import uk.co.thomasc.lvf.network.BinaryReader;

public class Packet1Result implements Packet {
	
	@Getter @Setter private boolean success;
	
	public Packet1Result() {
		
	}
	
	public void readPacket(BinaryReader input) throws IOException {
		setSuccess(input.readByte() == 1);
	}

	public byte[] getBytes() {
		return new byte[] { (byte) (success ? 1 : 0) };
	}

	public int getId() {
		return 1;
	}
	
	public int getVersion() {
		return 0;
	}
	
}