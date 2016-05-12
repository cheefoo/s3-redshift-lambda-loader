package com.tayo.redloader;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class DDBHelper 
{
	static final String TABLE_NAME = "RedShiftS3FileSweeper";
	private static final String partitionKeyName = "filename";
	private static final String sortKeyName = "timestamp";	
	private static final Long readCapacityUnit = 5L;  //Modify as you wish
	private static final Long writeCapacityUnit = 5L;  //Modify as you wish
	
	//static AmazonDynamoDB dynamoDB = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new EnvironmentVariableCredentialsProvider()));
         
          
	
	static SimpleDateFormat dateFormatter = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	
	private static final Logger logger = LoggerFactory.getLogger(DDBHelper.class);

	
	public static void createTable()
	{
		//create ddb if it does not exist // should be moved to a separate process may slow down lambda function for the first time
		logger.info("Creating table " + TABLE_NAME);
		try
		{
			 ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
	            keySchema.add(new KeySchemaElement()
	                .withAttributeName(partitionKeyName)
	                .withKeyType(KeyType.HASH)); //Partition key
	            
	            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
	            attributeDefinitions.add(new AttributeDefinition()
	                .withAttributeName(partitionKeyName)
	                .withAttributeType("S"));

	           
	                keySchema.add(new KeySchemaElement()
	                    .withAttributeName(sortKeyName)
	                    .withKeyType(KeyType.RANGE)); //Sort key
	                attributeDefinitions.add(new AttributeDefinition()
	                    .withAttributeName(sortKeyName)
	                    .withAttributeType("S"));
	            

	            CreateTableRequest request = new CreateTableRequest()
	                    .withTableName(TABLE_NAME)
	                    .withKeySchema(keySchema)
	                    .withProvisionedThroughput( new ProvisionedThroughput()
	                        .withReadCapacityUnits(readCapacityUnit)
	                        .withWriteCapacityUnits(writeCapacityUnit));
	            request.setAttributeDefinitions(attributeDefinitions);

	            logger.info("Issuing CreateTable request for " + TABLE_NAME);
	            Table table = dynamoDB.createTable(request);
	            logger.info("Waiting for " + TABLE_NAME
	                + " to be created...this may take a while...");
	            table.waitForActive();
			
		}
		catch(Exception e)
		{
			logger.error(e.toString());
		}
	}
	
	public static PutItemOutcome addItemToTable(String filename, String status)
	{
		long timestamp = System.currentTimeMillis();
		Date date = new Date();
		date.setTime(timestamp);
		
		Table table = dynamoDB.getTable(TABLE_NAME);
		logger.info("Adding item to table " + table.getTableName());
		
		Item item = new Item().withPrimaryKey("filename", filename, sortKeyName, dateFormatter.format(date))
				.withString("status", status );
		PutItemOutcome pio = table.putItem(item);
		
		
		return pio;
		
	}
}
	
	