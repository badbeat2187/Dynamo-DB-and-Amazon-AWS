package dynamodbassignment5;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.model.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jxl.Sheet;
import org.apache.commons.lang.StringUtils;

//Class to handle all DynamoDb accesses
//Some of the methods may be used formt the 
//following website: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStartedDynamoDB.html
public class DynamoDbAccessor 
{
    //Header row to skip
    int rowNumToSkip = 0;
    
    //Number of columns to scan from the xls worksheet
    int columnToScan = 7;
    
    //Variables to store the two most frequent winning combination
    int maxCount = -1;
    int winningNumber1 = -1;
    int winningNumber2 = -1;
    
    //Column information of the Lotto table in DynamoDb
    String[] rowInfo = {"Date", "Num1", "Num2", "Num3", "Num4", "Num5", "Num6"};
    
    //Different HashMap to store the results
    HashMap<Integer, Integer> dictWinningNumberVersusCount;
    HashMap<Integer, HashMap<Integer, Integer>> 
            mapOfMonthAnyYearVersusWinningNumber;
    HashMap<Integer, HashMap<Integer, HashMap<Integer, Integer>>> 
            mapOfYearToMonthToWinningNumber;
    HashMap<Integer, HashMap<Integer, Integer>> 
            mapOfTwoFrequentNumbersInWinningPick;
    
    //Variable to store the AWS connection information
    AmazonDynamoDBClient client;
    
    //Constructor
    public DynamoDbAccessor() throws IOException
    {
        AWSCredentials credentials = new PropertiesCredentials(
                DynamoDbAccessor.class.getResourceAsStream("DynamoDb.properties"));

        client = new AmazonDynamoDBClient(credentials);
    }
    
    //Method to create the Lotto table with the read and write capacity
    //constraints
    public void CreateTable(String tableName, long readCapacityUnits, 
            long writeCapacityUnits) 
    {        
        try 
        {
            // Create a table with a primary key named 'name', which holds a string
            CreateTableRequest createTableRequest = 
             new CreateTableRequest().withTableName(tableName).
                    withKeySchema(new KeySchemaElement().
                    withAttributeName("Date").withKeyType("S")).
                    withProvisionedThroughput(new ProvisionedThroughput().
                    withReadCapacityUnits(readCapacityUnits).
                    withWriteCapacityUnits(writeCapacityUnits));
            
            TableDescription createdTableDescription = 
                    client.createTable(createTableRequest).getTableDescription();
            System.out.println("Created Table: " + createdTableDescription);

            // Wait for it to become active
            WaitForTableToBecomeAvailable(tableName);

        } catch (AmazonServiceException ase) 
        {
            System.err.println("Failed to create table " + tableName + " " + ase);
        }
    }
    
    //Method to check if the table is available after
    //it has been created
    private void WaitForTableToBecomeAvailable(String tableName) 
    {
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) 
        {
            try 
            {
                Thread.sleep(1000 * 20);
            }
            catch (Exception e) 
            {}
            try
            {
                DescribeTableRequest request = new DescribeTableRequest().
                        withTableName(tableName);
                TableDescription tableDescription = 
                        client.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println("  - current state: " + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            }
            catch (AmazonServiceException ase) 
            {
                if (ase.getErrorCode().equalsIgnoreCase
                        ("ResourceNotFoundException") == false) throw ase;
            }
        }

        throw new RuntimeException("Table " + tableName + " never went active");
    }
    
    //Method to fill the data into DynamoDb by reading value from Work sheet xls
    public void UploadLotteryData(String tableName, Sheet sheet) 
    {        
        try 
        {
            Map<String, AttributeValue> item = new HashMap<>();            
            PutItemRequest itemRequest;
            boolean allItemsLoaded = false;
            int rows = sheet.getRows();
            
            for(int row = 0;row < rows-1;row++) 
            {
                if(rowNumToSkip == row)
                {
                    continue;
                }
                
                for(int col = 0;col < columnToScan;col++)        
                {                 
                    String contents = sheet.getCell(col, row).getContents();
                    if(contents.equals(""))
                    {
                        allItemsLoaded = true;
                        break;
                    }
                    
                    item.put(rowInfo[col], new AttributeValue().
                            withS(contents));
                    
                    itemRequest = new PutItemRequest().withTableName
                    (tableName).withItem(item);                    
                    client.putItem(itemRequest);
                }
                
                if(allItemsLoaded)
                {
                    break;
                }
            }
        }   
        catch (AmazonServiceException ase) 
        {
            System.err.println("Failed to create item in " + tableName 
                    + " " + ase);
        }
    }
    
    //Method to delete a particular table from DynamoDb
     public void DeleteTable(String tableName)
     {
        try 
        {            
            DeleteTableRequest request = new DeleteTableRequest()
                .withTableName(tableName);
            
            DeleteTableResult result = client.deleteTable(request);               
        } 
        catch (AmazonServiceException ase) 
        {
            System.err.println("Failed to delete table " + tableName + " " + ase);
        }
    }
     
     //Mthod to check if the table has been deleted
     public void WaitForTableToBeDeleted(String tableName) 
     {
        System.out.println("Waiting for " + tableName + " while status DELETING...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try 
            {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = client.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println("  - current state: " + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            } catch (ResourceNotFoundException e) {
                System.out.println("Table " + tableName + " is not found. It was deleted.");
                return;
            }
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
        }
        throw new RuntimeException("Table " + tableName + " was never deleted");
    }
     
     //Mthod to print all tables under this account in DynamoDb database
    public void GetAllTables()
    {
        // Initial value for the first page of table names.
        String lastEvaluatedTableName = null;
        do 
        {
            ListTablesRequest listTablesRequest = new ListTablesRequest()
            .withLimit(10)
            .withExclusiveStartTableName(lastEvaluatedTableName);

            ListTablesResult result = client.listTables(listTablesRequest);
            lastEvaluatedTableName = result.getLastEvaluatedTableName();

            for (String name : result.getTableNames()) {
                System.out.println(name);
            }

        } while (lastEvaluatedTableName != null);
    }
    
    //Mthod to get all the results for this assignment
    public void GetResults(String tableName)
    {
        Map<String, AttributeValue> lastKeyEvaluated = null;
        do
        {
            ScanRequest scanRequest = new ScanRequest(tableName).
                    withSelect(Select.ALL_ATTRIBUTES);

            ScanResult result = client.scan(scanRequest);
            dictWinningNumberVersusCount = new HashMap<Integer, Integer>();
            mapOfMonthAnyYearVersusWinningNumber = 
                    new HashMap<Integer, HashMap<Integer,Integer>>();
            mapOfYearToMonthToWinningNumber = 
                    new HashMap<Integer,
                    HashMap<Integer,HashMap<Integer,Integer>>>();
            mapOfTwoFrequentNumbersInWinningPick = 
                    new HashMap<Integer, HashMap<Integer,Integer>>();
            
            for (Map<String, AttributeValue> item : result.getItems()) 
            {
                ProcessResults(item);
            }
                
            lastKeyEvaluated = result.getLastEvaluatedKey();
        }
        while (lastKeyEvaluated != null); 
        PrintResults();
    }
    
    //Intermediate method to fetch the results
    private void ProcessResults(Map<String, 
                                    AttributeValue> attributeList) 
    {
        PopulateWinningNumbersVersusCount(attributeList);
        PopulateMonthVersusWinningNumbersVersusCountForAllYears(attributeList);
        PopulateYearVersusMonthVersusWinningNumbersVersusCount(attributeList);
        GetTwoFrequentNumberOccuringInWinningPick(attributeList);
    }

    //Method to populate the result of Winnign number versus count
    private void PopulateWinningNumbersVersusCount(Map<String, 
                                    AttributeValue> attributeList) 
            throws NumberFormatException 
    {
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) 
        {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();
            
            if(!attributeName.equals(rowInfo[0]))
            {            
                int winningNumber = Integer.valueOf(value.getS());
                if(dictWinningNumberVersusCount.containsKey(winningNumber))
                {
                    int oldValue = 
                            dictWinningNumberVersusCount.get(winningNumber);
                    dictWinningNumberVersusCount.put(winningNumber, oldValue+1); 
                }
                else
                {
                    dictWinningNumberVersusCount.put(winningNumber, 1);
                }
            }
        }
    }

    //Method to populate the result of Month versus winning number
    //versus count for all years
    private void PopulateMonthVersusWinningNumbersVersusCountForAllYears
            (Map<String, AttributeValue> attributeList) 
    {
        int monthKey = -1;
        List listOfWinningNumbers = new ArrayList();
            
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) 
        {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();

            if(attributeName.equals(rowInfo[0]))
            {
                String date = value.getS();
                String[] dateValueSplit = StringUtils.split(date, "/");
                monthKey = Integer.valueOf(dateValueSplit[1]);            
            }
            else
            {
                listOfWinningNumbers.add(Integer.valueOf(value.getS()));
            }
        }
        
        CheckIfMapHasValueElseAdd(monthKey, listOfWinningNumbers);
    }
    
    //Helper method for method 
    //PopulateMonthVersusWinningNumbersVersusCountForAllYears
    private void CheckIfMapHasValueElseAdd(int monthKey, List winningNumberList)
    {
        for(Object number : winningNumberList)
        {
            int winningNumber = (int)number;
            
            if(mapOfMonthAnyYearVersusWinningNumber.containsKey(monthKey))
            {
                HashMap<Integer, Integer> mapOfWinningNumberVersusCount = 
                        mapOfMonthAnyYearVersusWinningNumber.get(monthKey);
                
                if(mapOfWinningNumberVersusCount.containsKey(winningNumber))
                {
                    int oldValue = mapOfWinningNumberVersusCount.
                                                get(winningNumber);
                    mapOfWinningNumberVersusCount.put(winningNumber,
                                                    oldValue+1);
                }
                else
                {
                    mapOfWinningNumberVersusCount.put(winningNumber, 1);
                }
                
                mapOfMonthAnyYearVersusWinningNumber.put(monthKey, 
                        mapOfWinningNumberVersusCount);
            }
            else
            {
                HashMap<Integer, Integer> mapOfWinningNumberVersusCount = 
                        new HashMap<Integer, Integer>();
                mapOfWinningNumberVersusCount.put(winningNumber, 1);
                
                mapOfMonthAnyYearVersusWinningNumber.put(monthKey, 
                                        mapOfWinningNumberVersusCount);
            }
        }            
    }

    //Method to Print all results
    private void PrintResults() 
    {
        for(Map.Entry<Integer, Integer> e : dictWinningNumberVersusCount.entrySet())
        {
            System.out.println(e.getKey()+": "+e.getValue());
        }
        
        for(Map.Entry<Integer, HashMap<Integer, Integer>> e : 
                mapOfMonthAnyYearVersusWinningNumber.entrySet())
        {
            System.out.println("Month : "+e.getKey()+"\n");
            for(Map.Entry<Integer, Integer> f : e.getValue().entrySet())
            {
                System.out.println(f.getKey()+": "+f.getValue());
            }            
        }
        
        for(Map.Entry<Integer, HashMap<Integer, HashMap<Integer, Integer>>> d : 
                mapOfYearToMonthToWinningNumber.entrySet())
        {
            System.out.println("Year : "+d.getKey()+"\n");
            for(Map.Entry<Integer, HashMap<Integer, Integer>> e : 
                    d.getValue().entrySet())
            {
                System.out.println("Month : "+e.getKey()+"\n");
                for(Map.Entry<Integer, Integer> f : e.getValue().entrySet())
                {
                    System.out.println(f.getKey()+": "+f.getValue());
                }            
            }
        }
        
        PrintResultForTwoFrequentNumberInWinningPick();
    }

    //Method to fill up the results for finding
    //count of winning number for each month for each year
    private void PopulateYearVersusMonthVersusWinningNumbersVersusCount
            (Map<String, AttributeValue> attributeList) 
    {
        int yearKey = -1;
        int monthKey = -1;
        List listOfWinningNumbers = new ArrayList();
            
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) 
        {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();

            if(attributeName.equals(rowInfo[0]))
            {
                String date = value.getS();
                String[] dateValueSplit = StringUtils.split(date, "/");
                monthKey = Integer.valueOf(dateValueSplit[1]);
                yearKey = Integer.valueOf(dateValueSplit[2]);
            }
            else
            {
                listOfWinningNumbers.add(Integer.valueOf(value.getS()));
            }
        }
        
        CheckIfMapHasValueElseAddForyearAndMonth(yearKey,
                                        monthKey, listOfWinningNumbers);
    }

    //Helper method for function
    //PopulateYearVersusMonthVersusWinningNumbersVersusCount
    private void CheckIfMapHasValueElseAddForyearAndMonth
            (int yearKey, int monthKey, List listOfWinningNumbers) 
    {
        for(Object number : listOfWinningNumbers)
        {
            int winningNumber = (int)number;
            
            if(mapOfYearToMonthToWinningNumber.containsKey(yearKey))
            {
                HashMap<Integer, HashMap<Integer, Integer>> 
                        mapOfMonthToWinningNumber = 
                        mapOfYearToMonthToWinningNumber.get(yearKey);
                
                if(mapOfMonthToWinningNumber.containsKey(monthKey))
                {
                    HashMap<Integer, Integer> mapOfWinningNumberVersusCount = 
                            mapOfMonthToWinningNumber.get(monthKey);

                    if(mapOfWinningNumberVersusCount.containsKey(winningNumber))
                    {
                        int oldValue = mapOfWinningNumberVersusCount.
                                                    get(winningNumber);
                        mapOfWinningNumberVersusCount.put(winningNumber,
                                                        oldValue+1);
                    }
                    else
                    {
                        mapOfWinningNumberVersusCount.put(winningNumber, 1);
                    }

                    mapOfMonthToWinningNumber.put(monthKey, 
                            mapOfWinningNumberVersusCount);
                }
                else
                {
                    HashMap<Integer, Integer> mapOfWinningNumberVersusCount = 
                            new HashMap<Integer, Integer>();
                    mapOfWinningNumberVersusCount.put(winningNumber, 1);

                    mapOfMonthToWinningNumber.put(monthKey, 
                                            mapOfWinningNumberVersusCount);
                    
                    mapOfYearToMonthToWinningNumber.put(yearKey, 
                            mapOfMonthToWinningNumber);                    
                }
            }
            else
            {
                HashMap<Integer, Integer> mapOfWinningNumberVersusCount = 
                        new HashMap<Integer, Integer>();
                mapOfWinningNumberVersusCount.put(winningNumber, 1);

                HashMap<Integer, HashMap<Integer, Integer>> 
                        mapOfMonthToWinningNumber = 
                        new HashMap<Integer, HashMap<Integer, Integer>>();
                
                mapOfMonthToWinningNumber.put(monthKey, 
                                        mapOfWinningNumberVersusCount);
                
                mapOfYearToMonthToWinningNumber.put(yearKey, 
                        mapOfMonthToWinningNumber);
            }
        }
    }

    //Method to fetch the two most frequent number
    //in a winning pick
    private void GetTwoFrequentNumberOccuringInWinningPick
            (Map<String, AttributeValue> attributeList) 
    {
        List<Integer> listOfWinningNumbers = new ArrayList();
        
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) 
        {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();

            if(!attributeName.equals(rowInfo[0]))
            {
                listOfWinningNumbers.add(Integer.valueOf(value.getS()));
            }            
        }
         
        Collections.sort(listOfWinningNumbers);
        PopulateMapOfTwoFrequentNumbersInWinningPick(listOfWinningNumbers);       
    }

    //Method to populate the AHshmap of two most
    //frequent numbers in a winning pick
    private void PopulateMapOfTwoFrequentNumbersInWinningPick
            (List<Integer> listOfWinningNumbers) 
    {
        for(int firstNumber =0; firstNumber<listOfWinningNumbers.size();
                firstNumber++)
        {
            for(int secondNumber = firstNumber+1;
                    secondNumber<listOfWinningNumbers.size();
                    secondNumber++)
            {
                if(mapOfTwoFrequentNumbersInWinningPick.
                        containsKey(listOfWinningNumbers.get(firstNumber)))
                {
                    HashMap<Integer, Integer> secondNumberToCount =
                            mapOfTwoFrequentNumbersInWinningPick.
                            get(listOfWinningNumbers.get(firstNumber));
                    
                    if(secondNumberToCount.
                            containsKey(listOfWinningNumbers.get(secondNumber)))
                    {
                        int oldCount = secondNumberToCount.
                                get(listOfWinningNumbers.get(secondNumber));
                        secondNumberToCount.
                                put(listOfWinningNumbers.get(secondNumber), 
                                oldCount+1);
                        
                        CheckForMaxCountForTwpFrequentWinningNumber
                                (listOfWinningNumbers.get(firstNumber)
                                , listOfWinningNumbers.get(secondNumber),
                                oldCount+1);
                    }
                    else
                    {
                        secondNumberToCount.put(
                                listOfWinningNumbers.get(secondNumber), 1);
                        
                        CheckForMaxCountForTwpFrequentWinningNumber
                                (listOfWinningNumbers.get(firstNumber),
                                listOfWinningNumbers.get(secondNumber), 1);
                    }
                    
                    mapOfTwoFrequentNumbersInWinningPick.
                            put(listOfWinningNumbers.get(firstNumber), 
                            secondNumberToCount);
                }
                else
                {
                    HashMap<Integer, Integer> secondNumberToCount =
                            new HashMap<Integer, Integer>();
                    secondNumberToCount.
                            put(listOfWinningNumbers.get(secondNumber), 1);
                    
                    mapOfTwoFrequentNumbersInWinningPick.
                            put(listOfWinningNumbers.get(firstNumber), 
                            secondNumberToCount);
                    
                    CheckForMaxCountForTwpFrequentWinningNumber
                                (listOfWinningNumbers.get(firstNumber),
                                listOfWinningNumbers.get(secondNumber), 1);
                }
            }
        }
    }

    //Print results of two most frequent number
    //in a winning pick
    private void PrintResultForTwoFrequentNumberInWinningPick() 
    {
        System.out.println("Maximum Count of two most frequent number is "+
                maxCount+ " and the numbers are "
                + winningNumber1+"and "+ winningNumber2);
    }

    //Method to compare the count of two most frequent winning
    //number and update the variables accordingly
    private void CheckForMaxCountForTwpFrequentWinningNumber
            (int firstNumber, int secondNumber, int count) 
    {
        if(count>maxCount)
        {
            maxCount = count;
            winningNumber1 = firstNumber;
            winningNumber2 = secondNumber;
        }     
    }
}
