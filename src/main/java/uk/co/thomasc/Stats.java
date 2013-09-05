package uk.co.thomasc;

public class Stats extends Thread {
	
	private int rows = 0;
	
	public Stats() {
		start();
	}
	
	public void incRows() {
		rows++;
	}
	
	@Override
	public void run() {
		while (true) {
			try {
				sleep(1000);
				
				System.out.println("Rows: " + rows + "\r");
				
				rows = 0;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}