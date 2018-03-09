package pajkouisn.utilities.constants;

public class Constant
{
	/**
	 * The caller references the constants using <tt>Consts.EMPTY_STRING</tt>, 
	 * and so on. Thus, the caller should be prevented from constructing objects of 
	 * this class, by declaring this private constructor. 
	 */
	private Constant()
	{
		//	this prevents even the native class from 
		//	calling this ctor as well :
		throw new AssertionError();
	}
	
	//	Can retrieve a max of 10 messages from SQS.
	public static final int maximumRetrievableMessages = 10;
}
