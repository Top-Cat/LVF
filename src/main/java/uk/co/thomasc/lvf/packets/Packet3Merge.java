package uk.co.thomasc.lvf.packets;

import java.io.IOException;

import lombok.Getter;
import lombok.Setter;
import uk.co.thomasc.lvf.network.BinaryReader;
import uk.co.thomasc.lvf.network.BinaryWriter;

public class Packet3Merge extends TaskPacket {
	
	@Getter @Setter private int newUvi;
	
	public Packet3Merge() {
		
	}
	
	public void readPacket(BinaryReader input) throws IOException {
		super.readPacket(input);
		setNewUvi(input.readInt());
	}

	public byte[] getBytes() throws IOException {
		BinaryWriter bw = new BinaryWriter();
		
		bw.write(super.getBytes());
		bw.write(getNewUvi());
		
		return bw.toByteArray();
	}

	public int getId() {
		return 3;
	}
	
	public int getVersion() {
		return super.getVersion() + 0;
	}
	
}