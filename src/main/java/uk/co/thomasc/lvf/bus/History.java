package uk.co.thomasc.lvf.bus;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A single history record containing
 * first and last seen times for a bus on a route on a day
 */
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class History {

	/**
	 * The times
	 */
	@Getter @Setter private Date firstSeen, lastSeen;

}
