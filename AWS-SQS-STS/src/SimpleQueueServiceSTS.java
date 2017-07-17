import java.util.List;
import java.util.Map.Entry;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class SimpleQueueServiceSTS
{
	static String awsRegion;
	static int maxmaxNoofMsg;
	static String sqsURL;
	
	AmazonSQS sqs;

	public static void setParam(String args[])
	{
		awsRegion = args[0];
		maxmaxNoofMsg = Integer.parseInt(args[1]);
		sqsURL = args[2];
	}
	
	public void setSQS()
	{
		sqs = AmazonSQSClientBuilder.standard().withRegion(awsRegion).build();
	}
	
	public void getSQSMessages()
	{
        try 
        {
            System.out.println("Receiving messages from S1 SQS.\n");
            ReceiveMessageRequest rmsgr = new ReceiveMessageRequest(sqsURL);
            rmsgr.setMaxNumberOfMessages(maxmaxNoofMsg);
            ReceiveMessageResult rmr = sqs.receiveMessage(rmsgr);
            List<Message> messages = rmr.getMessages();
            for (Message message : messages) 
            {
                System.out.println("  Message");
                System.out.println("    MessageId:     " + message.getMessageId());
                System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                System.out.println("    Body:          " + message.getBody());
                for (Entry<String, String> entry : message.getAttributes().entrySet()) 
                {
                    System.out.println("  Attribute");
                    System.out.println("    Name:  " + entry.getKey());
                    System.out.println("    Value: " + entry.getValue());
                }
            }
        } 
        catch (AmazonServiceException ase) 
        {
            System.out.println("Caught an AmazonServiceException, which means your request made it to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } 
        catch (AmazonClientException ace) 
        {
            System.out.println("Caught an AmazonClientException, which means the client encountered a serious internal problem while trying to communicate with SQS, such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        catch (Exception e)
        {
        	System.out.println("Exception while executing getMessages()");
        	e.printStackTrace();
        }
        
    }
	
    public static void main(String[] args) throws Exception
    {
    	if (args.length > 0)
    	{
    		try
    		{
    			setParam(args);
    		}
    		catch(Exception e)
    		{
    			System.out.println("Exception in getting arguments");
    			e.printStackTrace();
    		}
    	}
    	
    	SimpleQueueServiceSTS sqs1 = new SimpleQueueServiceSTS();
    	sqs1.setSQS();
    	while (true)
    	{
    		sqs1.getSQSMessages();
    	}
    }
}