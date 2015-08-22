package uk.co.thomasc.lvf.network;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import uk.co.thomasc.lvf.packets.Packet;
import uk.co.thomasc.lvf.packets.Packets;

public class Network {

	private static int MAGIC = 0x004C5646; // "LVF"
	
	public static void sendPacket(Socket socket, Packet packet) throws Exception {
		byte[] message = packet.getBytes();
		
		BinaryWriter os = new BinaryWriter(socket.getOutputStream());
		os.write(message.length);
		os.write(packet.getId());
		os.write(packet.getVersion());
		os.write(MAGIC);
		os.write(message);
		os.flush();
	}
	
	public static Packet readPacket(Socket socket) throws Exception {
		BinaryReader reader = new BinaryReader(socket.getInputStream());
		
		int packetLen = reader.readInt();
		int packetId = reader.readInt();
		int packetVersion = reader.readInt();
		int packetMagic = reader.readInt();
		
		if (packetMagic != MAGIC) {
			throw new IOException("Got a packet with invalid magic!");
		}
		
		byte[] message = reader.readBytes(packetLen);
		
		Packet packet = Packets.getPacket(packetId).createInstance();
		
		if (packet.getVersion() > packetVersion) {
			throw new IOException("Received an outdated packet!");
		} else if (packet.getVersion() < packetVersion) {
			throw new IOException("Received a newer packet!");
		}
		
		packet.readPacket(new BinaryReader(message));
		
		return packet;
	}
	
	public static Packet listenForPacket(Socket socket) throws Exception {
		return listenForPacket(socket, 10000);
	}
	
	public static Packet listenForPacket(Socket socket, int timeout) throws Exception {
		long lastReadTime = System.currentTimeMillis();
		InputStream is = socket.getInputStream();
		
		while (System.currentTimeMillis() - lastReadTime < timeout) {
			if (is.available() > 0) {
				Packet packet = readPacket(socket);
				return packet;
			}
			Thread.sleep(20);
		}
		throw new IOException("Socket timeout");
	}
	
}