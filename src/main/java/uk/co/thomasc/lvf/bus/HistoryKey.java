package uk.co.thomasc.lvf.bus;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A key used to determine what a History refers to
 * Used in a hash table for fast recovery of current History values
 */
@EqualsAndHashCode
@AllArgsConstructor
@Getter
public class HistoryKey {

	/**
	 * The day in "yyyy-MM-dd" format
	 * @see TFL#dateFormat
	 */
	private final String date;
	/**
	 * The lineid
	 */
	private final String lineid;
	/**
	 * The route
	 */
	private final String route;

}
