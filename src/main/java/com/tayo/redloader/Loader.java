package com.tayo.redloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Loader 
{
	private  static final String BUCKET_NAME = "temitayo-redloader";
	private static final String CONFIG_PREFIX = "config/redl0ader_ciphertext.txt";	
	private static final String KEY_ARN = "arn:aws:kms:us-east-1:573906581002:key/37dc90dc-3f1c-4a77-a51d-a653b173fcdb "; 
	//private static final String tableName = "RedShiftS3FileSweeper2";
	
	
	private static final Logger logger = LoggerFactory.getLogger(Loader.class);
	

	public void handleRequest(S3Event input, Context context)
	{
		//
		context.getLogger().log("Input: " + input);
		context.getLogger().log("Bucket Name is : " + input.getRecords().get(0).getS3().getBucket().getName());
		//Load config files 
		List<String> configList = new ArrayList<String>();
		List<String> decryptedList = new ArrayList<String>();
		Statement stmt = null;
		Connection conn = null;
		try 
		{
			configList = ConfigLoader.getConfigObject(BUCKET_NAME, CONFIG_PREFIX);
			
			for(int i = 0; i < configList.size(); i++)
	        {
	        	logger.info(configList.get(i));
	        }
			
			//Pass config list to KMS Helper for decode
			decryptedList = KmsKeyHelper.getDecryptedConfig(KEY_ARN, configList);
		
			//create DB connection
		    conn = DBConnection.getConnection(decryptedList.get(2), decryptedList.get(3));
			
			if (conn != null)
			{
				logger.info("Successfully obtained valid connection" + conn.toString());
			}
									
			AmazonS3 s3Client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider());
			ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(BUCKET_NAME).withPrefix("incoming");
			
			ObjectListing  objList = s3Client.listObjects(listObjectsRequest);
			
			List<S3ObjectSummary> listing = objList.getObjectSummaries();
			
			int i = 0;
			boolean isCopySuccess = false;
			
			for(S3ObjectSummary obj : listing)
			{
				logger.info(++i + " " + obj.getKey());
				if(obj.getKey().endsWith(".txt"))
				{
					stmt = conn.createStatement();
					isCopySuccess = copyS3ToRed(stmt, obj, decryptedList.get(0),decryptedList.get(1));
					if(isCopySuccess)
					{
						//move to processed directory
						s3Client.copyObject(BUCKET_NAME, obj.getKey(), BUCKET_NAME, "processed/"+obj.getKey().substring(obj.getKey().lastIndexOf("/")+1, obj.getKey().length()));
					    s3Client.deleteObject(new DeleteObjectRequest(BUCKET_NAME, obj.getKey()));
						logger.info(obj.getKey() + "moved to processed successfully");
					}
					else
					{
						//move to failed directory
						s3Client.copyObject(BUCKET_NAME, obj.getKey(), BUCKET_NAME, "failed/"+obj.getKey().substring(obj.getKey().lastIndexOf("/")+1, obj.getKey().length()));
					    s3Client.deleteObject(new DeleteObjectRequest(BUCKET_NAME, obj.getKey()));					
						logger.info(obj.getKey() + "moved to failed successfully");
					}
					//verifying if table exists
					//:Todo Move to a separate
					/*AmazonDynamoDB dbclient = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
					DescribeTableRequest describeTableRequest = new DescribeTableRequest(DDBHelper.tableName);
					TableDescription  tableDescription = dbclient.describeTable(describeTableRequest).getTable();
					
					if(tableDescription == null)
					{
						logger.info("Table does not exist, creating table..." + DDBHelper.tableName );
						DDBHelper.createTable();
					}*/
					//Add item to table
					logger.info("Adding item " + obj.getKey().toString());
					PutItemOutcome pio = DDBHelper.addItemToTable(obj.getKey().toString(), String.valueOf(isCopySuccess));
					logger.info("Item added successfully " + pio.getItem().toJSONPretty());
				}
			}					
			
		} 
		catch (IOException e) 
		{	
			e.printStackTrace();
		} 
		catch (InterruptedException e) 
		{		
			e.printStackTrace();
		} 
		catch (SQLException e) 
		{			
			e.printStackTrace();
		}
		finally
		{
			if(stmt != null)
			{
				try 
				{
					stmt.close();
				} catch (SQLException e) 
				{				
					e.printStackTrace();
				}
			}
			if(conn != null)
			{
				try 
				{
					conn.close();
				} 
				catch (SQLException e) 
				{					
					e.printStackTrace();
				}
			}
		}
		
	}
	
	private static boolean copyS3ToRed(Statement stmt, S3ObjectSummary objKeyToCopy, String accessKey, String secretKey) 
	{
		boolean status = false;		
		logger.info("data received is  = "+ objKeyToCopy);
		if(objKeyToCopy != null && objKeyToCopy.getKey().length()!=0)
		{
			try
			{
				
				String sql = "copy public.ceo from " +"'" +"s3://" + objKeyToCopy.getBucketName()+"/" +objKeyToCopy.getKey()+ "'"  +
						"credentials 'aws_access_key_id="+accessKey+";aws_secret_access_key="+secretKey+"' region 'us-east-1'";
				logger.info("SQL is : " + sql);
				int i = stmt.executeUpdate(sql);
				stmt.close();				
				logger.info("Successfully loaded" + i);
				status =  true;
			}
			catch(Exception e)
			{
				logger.error(e.toString());
				status =  false;
			}
			 
		}
		return status;
	}
	

}
