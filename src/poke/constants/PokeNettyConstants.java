package poke.constants;

/**
 * This class will hold all the constant related to the PokeNetty project.
 * @author krish
 *
 */
public class PokeNettyConstants {

	//This is for testing on my local machine. So you do not need to change anything.
	public static String DEFAULT_NAMESPACE_PATH = "/home/krish/Documents/CMPE275/server0/defaultNameSpace/";
	
	//This you need to change as this would be path where the document would be downloaded and stored for you client.
	public static String CLIENT_PATH_TOSTORE = "/home/krish/clientDocument/";
	
	//This you need to change as these are the your server defaults.
	public static String COMMON_LOCATION = "/home/krish/Documents/";
	public static String SERVER_NAME;
	public static String DEFAULT_NAMESPACE = "default";
	
	//This you need to change based on you adjacent node configuration.
	public static String MY_ADJACENT_NODE_HOST = "192.168.0.202";
    public static String MY_ADJACENT_NODE_PORT = "5571";
	
	//This you may not need to change as these would be common configuration for PostgreSQL DB connections.
	public static String DB_DRIVER = "org.postgresql.Driver";
	public static String DB_URL = "jdbc:postgresql://localhost:5432/fileedgedatbase";
    public static String DB_USER = "postgres";
    public static String DB_PASS = "postgres";
    
    public static String SEARCH_FOR_DOC_QUERY = "Select filepath "
    		+ "from fileedgedatbase.fileedge_schema.fed_localfiles "
    		+ "where namespace = ? and filename = ?";
    
    public static String DELETE_DOC_QUERY = "Delete from fileedgedatbase.fileedge_schema.fed_localfiles "
    		+ "where namespace = ? and filename = ?";
}
