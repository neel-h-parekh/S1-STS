import java.util.List;
import java.util.Map.Entry;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class SimpleQueueServiceSTS
{
    public static void main(String[] args) throws Exception 
    {
    	int maxNomsg = 0;
    	String sqs_url = null;//"https://sqs.us-east-1.amazonaws.com/882038671278/s1-pipeline-sqs"
    	if (args.length > 0)
    	{
    		try
    		{
    			maxNomsg = Integer.parseInt(args[0]);
    			sqs_url = args[1];
    		}
    		catch(Exception e)
    		{
    		}
    	}
    	AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    	
    	while (true)
    	{
    		
        try 
        {
            System.out.println("Receiving messages from S1 SQS.\n");
            ReceiveMessageRequest rmsgr = new ReceiveMessageRequest(sqs_url);
            rmsgr.setMaxNumberOfMessages(maxNomsg);
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
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    	}
    }
}