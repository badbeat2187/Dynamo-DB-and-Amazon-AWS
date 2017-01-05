package dynamodbassignment5;

import jxl.*;
import java.io.*;

public class DynamoDbAssignment5 
{
    DynamoDbAccessor dbAccessor;
    String tableName = "Lotto";
    
    public DynamoDbAssignment5() throws IOException
    {
        //Creating instance of class which handles all access to DynamoDB
        dbAccessor = new DynamoDbAccessor();
        
        //Deleting the table if already exists
        //dbAccessor.DeleteTable(tableName);
        
        //Waiting for tables ot get deleted
        //dbAccessor.WaitForTableToBeDeleted(tableName);
        
        //Creating tables in DynamoDb
        //dbAccessor.CreateTable(tableName, 10L, 5L);
        
        //Printing all tables under my account in DynamDb
        dbAccessor.GetAllTables();
        
        //Method to get all the resuls
        dbAccessor.GetResults(tableName);
    }
    
    //Read the contents of excel file using JExcel API
    //Referred from http://www.mindfiresolutions.com/How-to-read-an-excelsheet-from-Java-217.php
    //Populate the data into DynamoDb
    private void PopulateDataToDb(String fileToRead)
    {
        int sheetNumToRead = 0;
           
        try 
        {
            Workbook wb = Workbook.getWorkbook(new File(fileToRead));
            if(wb.getNumberOfSheets()>sheetNumToRead-1)
            {
                Sheet sheet = wb.getSheet(sheetNumToRead);
                dbAccessor.UploadLotteryData(tableName, sheet);
            }
        }
        catch(Exception ioe) 
        {
            ioe.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws IOException 
    {
        // TODO code application logic here
        DynamoDbAssignment5 assignment5 = new DynamoDbAssignment5();
        //assignment5.PopulateDataToDb(".//data//fl_lotto.xls");       
    }
}
