package com.tayo.redloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class ConfigLoader 
{
	private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

	
	public static List<String> getConfigObject(String bucketName, String key) throws IOException 
	{
      AmazonS3 s3Client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider());
        //AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        List<String> configList = null;
        try 
        {
           logger.info("Getting configuration Object from S3..");
            S3Object s3object = s3Client.getObject(new GetObjectRequest(
            		bucketName, key));
            logger.info("Content-Type: "  + 
            		s3object.getObjectMetadata().getContentType());
             configList = displayTextInputStream(s3object.getObjectContent());                    
                                   
        } catch (AmazonServiceException ase) {
        	logger.info("Caught an AmazonServiceException, which" +
            		" means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
        	logger.info("Error Message:    " + ase.getMessage());
        	logger.info("HTTP Status Code: " + ase.getStatusCode());
        	logger.info("AWS Error Code:   " + ase.getErrorCode());
        	logger.info("Error Type:       " + ase.getErrorType());
        	logger.info("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
        	logger.info("Caught an AmazonClientException, which means"+
            		" the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
        	logger.info("Error Message: " + ace.getMessage());
        }
        
        return configList;
    }

    private static List<String> displayTextInputStream(InputStream input)
    throws IOException 
    {
    	List<String> configList = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new 
        		InputStreamReader(input));
        while (true) 
        {
            String line = reader.readLine();
            if (line == null) break;
            configList.add(line.substring(line.indexOf("=")+1, line.length()));
            //System.out.println("    " + line);
        }        
        
        return configList;
    }
}
