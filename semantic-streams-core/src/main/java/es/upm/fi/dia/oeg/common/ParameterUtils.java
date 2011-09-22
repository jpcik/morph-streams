package es.upm.fi.dia.oeg.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;



public class ParameterUtils 
{
	public static String COMMENT_CHAR = "#";
	public static Properties load(InputStream fis) throws IOException
	{
        Properties props = new Properties();
        props.load(fis);    
        fis.close();
        return props;
	}
    public static Properties load(File propsFile) throws IOException 
    {
        FileInputStream fis = new FileInputStream(propsFile);     
        return load(fis);
    }
    
    public static String loadAsString(URL url) throws IOException, URISyntaxException
    {
    	FileReader fr = new FileReader(new File(url.toURI()));
		BufferedReader br = new BufferedReader(fr);
		String query = "";
		String buffer = "";		
		while((buffer= br.readLine()) != null)
		{
			if (!buffer.startsWith(COMMENT_CHAR))
			query = query + (buffer!=null?buffer+" \n ":" ");
		}		
		return query;	
    }
    
    public static String loadQuery(String path)
	{
		try {
			return ParameterUtils.loadAsString(ParameterUtils.class.getClassLoader().getResource(path));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return null;			
	}
	
   
}
