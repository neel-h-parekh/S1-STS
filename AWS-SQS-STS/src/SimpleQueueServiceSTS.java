import java.util.ArrayList;
import java.util.List;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.IOUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.*;

public class SimpleQueueServiceSTS implements Runnable
{
	public static Logger logger = LogManager.getLogger("SimpleQueueServiceSTS");

    static String noOfThreads;
    static int retryCount;
    static int retryWaitMilliSeconds;
	static String awsRegion;
	static int maxNoofMsg;
	static String sqsURL;
	static String dbURL; 
    static String masterUsername;
    static String masterUserPassword;
    static String tableName;
    static File f;
    static JSONObject properties;
    static InputStream is;
    static String jsonTxt;
    
	private Thread t;
	private String threadName;
	
	SimpleQueueServiceSTS(String name) 
	{
		threadName = name;
		logger.info("Creating " +  threadName );
	}
	
	AmazonSQS sqsClient;
	ReceiveMessageRequest rmsgr;
	ReceiveMessageResult rmr;
	List<Message> messages;
	String s3file;
	String messageReceiptHandle;
	String[] s3filedtls;
	String tbl_name;
	String file_ym;
	int file_day;
	int file_hr;
	int file_min;
	String file_name;
	String file_bucket;
	String file_create_date;
    Connection conn = null;
    Properties props = new Properties();
    Statement stmt = null;
    String sql;

	public static void getSetProperties()
	{
		try
		{
			f = new File("properties.json");
	        if (f.exists())
	        {
	            is = new FileInputStream("properties.json");
	            jsonTxt = IOUtils.toString(is);
	            logger.info(jsonTxt);
	            properties = new JSONObject(jsonTxt);
				awsRegion = properties.getJSONArray("aws-sqs-redshift-loader").getJSONObject(0).getJSONArray("aws").getJSONObject(0).getString("awsRegion");
				maxNoofMsg = Integer.parseInt(properties.getJSONArray("aws-sqs-redshift-loader").getJSONObject(0).getJSONArray("awsSQS").getJSONObject(0).getString("maxNoofMsg"));
				sqsURL = properties.getJSONArray("aws-sqs-redshift-loader").getJSONObject(0).getJSONArray("awsSQS").getJSONObject(0).getString("sqsURL");
				dbURL = properties.getJSONArray("aws-sqs-redshift-loader").getJSONObject(0).getJSONArray("awsREDSHIFT").getJSONObject(0).getString("redshiftdbURL");
				masterUsername = properties.getJSONArray("aws-sqs-redshift-loader").getJSONObject(0).getJSONArray("awsREDSHIFT").getJSONObject(0).getString("masterUsername");
				masterUserPassword = properties.getJSONArray("aws-sqs-redshift-loader").getJSONObject(0).getJSONArray("awsREDSHIFT").getJSONObject(0).getString("masterUserPassword");
				tableName = properties.getJSONArray("aws-sqs-redshift-loader").getJSONObject(0).getJSONArray("awsREDSHIFT").getJSONObject(0).getString("tableName");
				noOfThreads = properties.getString("runThreads");
				retryCount = Integer.parseInt(properties.getString("retryMax"));
				retryWaitMilliSeconds = Integer.parseInt(properties.getString("retryWaitSeconds"))*1000;
				logger.info("*************************Config*************************");
				logger.info("No of Threads to run : "+noOfThreads);
				logger.info("AWS Region : "+awsRegion);
				logger.info("Pool queue upto messages : "+maxNoofMsg);
				logger.info("AWS SQS Url : "+sqsURL);
				logger.info("AWS RedShift DB JDBC Url : "+dbURL);
				logger.info("AWS RedShift DB Username : "+masterUsername);
				logger.info("AWS RedShift DB Schema.Tablename : "+tableName);
				logger.info("Max Retry : "+retryCount);
				logger.info("Retry wait secnds : "+retryWaitMilliSeconds/1000);
	        }
	        else
	        {
	        	logger.error("properities file not found");
	        	logger.error("Exiting Application");
	        }
		}
		catch(Exception e)
		{
			logger.error("Caught an Exception while getting properties.json");
			logger.error(e.getMessage(),e);
			logger.error("Exiting Application...");
			System.exit(0);
		}
	}
	
	public void getSetDBConnection() throws Exception
	{
		int count = 0;
		int maxTriesWaitMilliSec = retryWaitMilliSeconds;
		while(true)
		{
			try
			{
				Class.forName("com.amazon.redshift.jdbc42.Driver");
				props.setProperty("user", masterUsername);
		        props.setProperty("password", masterUserPassword);
		        conn = DriverManager.getConnection(dbURL, props);
				conn.setAutoCommit(true);
				count = 0;
				break;
			}
			catch(Exception e)
			{
				logger.error("Caught an Exception while getting AWS RedShift DB Connection");
				logger.error(e.getMessage(),e);
		        if (count++ == retryCount) throw e;
		        logger.warn("Retry "+count+": Getting AWS RedShift DB Connection after "+(retryWaitMilliSeconds/1000)+" seconds.");
		        Thread.sleep(maxTriesWaitMilliSec);
			}
		}
	}
	
	public void getSQSClient() throws InterruptedException
	{
		int count = 0;
		int maxTriesWaitMilliSec = retryWaitMilliSeconds;
		while(true)
		{
			try
			{
				sqsClient = AmazonSQSClientBuilder.standard().withRegion(Regions.valueOf(awsRegion)).build();
				count = 0;
			    break;
			}
			catch(Exception e)
			{
				logger.error("Caught an Exception while getting SQS Client.");
				logger.error(e.getMessage(),e);
		        if (count++ == retryCount) throw e;
		        logger.warn("Retry "+count+": Getting SQS Client after "+(retryWaitMilliSeconds/1000)+" seconds.");
		        Thread.sleep(maxTriesWaitMilliSec);
			}
		}
	}
	
	public void getSQSMessages() throws Exception
	{
		int count = 0;
		int maxTriesWaitMilliSec = retryWaitMilliSeconds;
		while(true)
		{
	        try 
	        {
	            rmsgr = new ReceiveMessageRequest(sqsURL);
	            rmsgr.setMaxNumberOfMessages(maxNoofMsg);
	            rmr = sqsClient.receiveMessage(rmsgr);
	            messages = rmr.getMessages();
	            if(!messages.isEmpty())
	            {
	                for(Message msg:messages)
	                {
	                	logger.info("Received SQS MessageId : "+msg.getMessageId());
	                }
	            }
	            break;
	        }
	        catch (Exception e)
	        {
	        	logger.error("Caught an Exception while getting SQS Messages");
				logger.error(e.getMessage(),e);
		        if (count++ == retryCount) throw e;
		        logger.warn("Retry "+count+": Getting SQS Messages after "+(retryWaitMilliSeconds/1000)+" seconds.");
		        Thread.sleep(maxTriesWaitMilliSec);
	        }
		}
    }

	public void insertSQSMessagesdb()
	{
		try
		{
			if(!messages.isEmpty())
			{
				stmt = conn.createStatement();
				for (Message message : messages)
				{
	                JSONObject jobj = new JSONObject(message.getBody());
	                s3file=jobj.getJSONArray("Records").getJSONObject(0).getJSONObject("s3").getJSONObject("object").getString("key");
	                s3filedtls=s3file.split("\\/");
	            	tbl_name = s3filedtls[1];
	            	file_ym = s3filedtls[2];
	            	file_day = Integer.parseInt(s3filedtls[3]);
	            	file_hr = Integer.parseInt(s3filedtls[4]);
	            	file_min = Integer.parseInt(s3filedtls[5]);
	            	file_name = s3filedtls[6];
	            	file_bucket = jobj.getJSONArray("Records").getJSONObject(0).getJSONObject("s3").getJSONObject("bucket").getString("name");
	            	file_create_date = jobj.getJSONArray("Records").getJSONObject(0).getString("eventTime");
					sql = "insert into "+tableName+" (tbl_name,file_ym,file_day,file_hr,file_min,file_name,file_bucket,file_create_date) "
							+ "values ("
							+ "'"+tbl_name+"','"+file_ym+"',"+file_day+","+file_hr+","+file_min+",'"+file_name+"','"+file_bucket+"','"+file_create_date+"')";
					logger.info("Prepared SQL:\t\t"+sql);
					stmt.addBatch(sql);
				}
				stmt.executeBatch();
			}
		}
		catch(Exception e)
		{
			logger.error("Caught an Exception while inserting records into db");
			logger.error(e.getMessage(),e);
		}
	}
	
	public void deleteSQSMessages()
	{
		try
		{
			if(!messages.isEmpty())
			{
				DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest().withQueueUrl(sqsURL);
				List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = new ArrayList<DeleteMessageBatchRequestEntry>();
				for (Message msg : messages)
				{
					deleteMessageBatchRequestEntries.add(new DeleteMessageBatchRequestEntry().withId(msg.getMessageId()).withReceiptHandle(msg.getReceiptHandle()));
				}
				deleteMessageBatchRequest.setEntries(deleteMessageBatchRequestEntries);
				
				DeleteMessageBatchResult deleteMessageBatchResult = sqsClient.deleteMessageBatch(deleteMessageBatchRequest);
				
				for (DeleteMessageBatchResultEntry deleteMessageBatchResultEntry : deleteMessageBatchResult.getSuccessful())
				{
					logger.info("Deleted SQS MessageID : "+deleteMessageBatchResultEntry.getId());
			    }
				
				for (BatchResultErrorEntry errorDeleteMessageBatchEntry : deleteMessageBatchResult.getFailed())
				{
		            logger.error("Failed to delete SQS MessageId : "+errorDeleteMessageBatchEntry.getId());
		            logger.error(errorDeleteMessageBatchEntry.getMessage());
				}
			}
		}
		catch(Exception e)
		{
			logger.error("Exception while deleteing SQS messages");
			logger.error(e.getMessage(),e);
		}	
	}
	
	public void start() 
	{
		logger.info("Starting " +  threadName );
		if (t == null)
		{
			t = new Thread (this, threadName);
			t.start();
		}
	}
	
	public void run()
	{
		try
		{
			this.getSQSClient();
			this.getSetDBConnection();
	    	while (true)
	    	{
				this.getSQSMessages();
				this.insertSQSMessagesdb();
				this.deleteSQSMessages();
	    	}
		}
		catch(Exception e)
		{
			logger.error("Ending due to Exception"+e);
			logger.error(e.getStackTrace());
		}
	}
	
    public static void main(String[] args) throws Exception
    {
    	logger.info("Starting Application...");
    	
    	try
    	{
    		logger.info("Getting configuration properties");
        	getSetProperties();
        	
        	for(int i=1;i<=Integer.parseInt(noOfThreads);i++)
        	{
        		 new SimpleQueueServiceSTS("thread"+i).start();
        	}
    	}
    	catch(Exception e)
    	{
    		logger.error(e.getMessage(),e);
    	}
    }
}