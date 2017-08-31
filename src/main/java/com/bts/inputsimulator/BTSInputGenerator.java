package com.bts.inputsimulator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Date;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by cloudera on 8/27/17.
 */
public class BTSInputGenerator {
    public static void main(String[] args){
        try {
            String simulatorBaseDir ="/home/cloudera/simulator/";
            long scheduleMillis = 1000 * Long.parseLong(args[0]);
            Integer noOfMessagesInBatch = Integer.parseInt(args[1]);
            int noOfMessagesInFile = Integer.parseInt(args[2]);

            RunTask runTask = new RunTask(noOfMessagesInBatch, simulatorBaseDir,noOfMessagesInFile);
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(runTask, 0, scheduleMillis);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

class RunTask extends TimerTask{
    private Integer noOfMessages;
    private Long count;
    private String simulatorBaseDir;
    private int noOfMessagesInFile;

    public RunTask(Integer noOfMessages, String simulatorBaseDir, int noOfMessagesInFile) throws IOException {
        this.noOfMessages = noOfMessages;
        this.simulatorBaseDir = simulatorBaseDir;
        this.noOfMessagesInFile = noOfMessagesInFile;
        Scanner scanner = new Scanner(new File(simulatorBaseDir+"bkIdCount.txt"));
        count = scanner.nextLong();
    }
    @Override
    public void run() {
        System.out.println("Messages Generated count = " + count);
        System.out.println("Current Time = " + new Date().toString());

        try {
        String[] dealColumns = new String[]{"BusinessKey","CtgyCd"};
        String[] dealColumnValuePrefixes = new String[]{"bk",""};
        String[] tnxColumns = new String[]{"BusinessKey"};
        String[] tnxColumnValuePrefixes = new String[]{"bk"};
        String[] teColumns = new String[]{"BusinessKey"};
        String[] teColumnValuePrefixes = new String[]{"bk"};

        String dealMsg = "<Deal>         <Header>             <BulkId>MIG01</BulkId>             <ProcModeCd>1</ProcModeCd>                   <BusinessKey>bk1</BusinessKey>                   <SourceTimeStamp>2016-01-25T09:32:01+01:00</SourceTimeStamp>                   <SourceSystemID>S1</SourceSystemID>              </Header>         <Id>id1</Id>             <AccNo>CITINA000000001</AccNo>         <Category>1</Category>               <CtgyCd>1</CtgyCd>         <Status>100</Status>         <ExtTpDesc>DEP</ExtTpDesc>               <Desc>Deal Details</Desc>         <OrderDate>2016-01-25T09:32:01+01:00</OrderDate>         <CreatedAt>2016-01-25T09:32:01+01:00</CreatedAt>         <DealLink>             <Id>TEST20150808000010</Id>         </DealLink>     </Deal>\n";
        String tnxMsg = "<Transaction>         <Header>             <BulkId>MIG01</BulkId><!-- used only for migrations -->             <ProcModeCd>1</ProcModeCd><!-- Create --> 			<BusinessKey>bk2</BusinessKey> 			<SourceTimeStamp>2016-01-25T09:34:01+01:00</SourceTimeStamp> 			<SourceSystemID>S1</SourceSystemID>		         </Header>         <Id>id1</Id> 		<CustCode>AMZN</CustCode> <!-- Amazon.com, Inc --> 		<CcyCode>INR</CcyCode> 		<ExchangeCode>NYSE</ExchangeCode> 		<TransType>WEB-BUY</TransType>         <IdScCd>10</IdScCd><!-- P90SOSID -->         <TpCd>1</TpCd><!-- instrumentTransaction -->         <StsCd>1</StsCd><!-- booked --> 		<Status>100</Status>          <DealLink>             <Id>TEST22150808000010</Id>         </DealLink>     </Transaction>\n";
        String teMsg = "<TransactionElement>         <Header>             <BulkId>MIG01</BulkId><!-- used only for migrations -->             <ProcModeCd>1</ProcModeCd><!-- Create --> \t\t\t<BusinessKey>bk4</BusinessKey> \t\t\t<SourceTimeStamp>2016-01-25T09:36:01+01:00</SourceTimeStamp> \t\t\t<SourceSystemID>S1</SourceSystemID>\t         </Header>         <Id>id1</Id>         <IdScCd>3</IdScCd><!-- P90SOSIVID -->         <TpCd>3</TpCd><!-- instrumentMovement -->                <BookingTpCd>3</BookingTpCd><!-- book -->         <Qty>500</Qty> \t\t<SettlementAmount>1000000</SettlementAmount> \t\t<SettlementCurrency>INR</SettlementCurrency> \t\t<SettlementCode>101</SettlementCode> <!-- T+2 --> \t\t<CreditDebitFlag>2</CreditDebitFlag><!-- debit -->         <TradeDate>2016-01-25T09:32:01+01:00</TradeDate>         <ValueDate>2016-01-25T09:32:01+01:00</ValueDate>         <BookingDate>2016-01-25T09:32:01+01:00</BookingDate>         <SettlDate>2016-01-25T09:32:01+01:00</SettlDate>         <BookingId>TEST221508087500001000101</BookingId> \t\t<Status>100</Status>         <TrxLink>             <Id>TEST2215080800001000</Id>         </TrxLink>     </TransactionElement>\n";

            String dealCombined = "";
            String tnxCombined = "";
            String teCombined = "";
            for(int i=1; i<=noOfMessages ; i++) {
            count++;
            String finalDeal = dealMsg;
            for(int j=0; j<dealColumns.length; j++) {
                int startIndex = finalDeal.indexOf("<"+dealColumns[j]+">")+dealColumns[j].length()+2;
                int lastIndex = finalDeal.lastIndexOf("</"+dealColumns[j]+">");
                finalDeal = finalDeal.substring(0,startIndex)+dealColumnValuePrefixes[j]+count+finalDeal.substring(lastIndex);
            }

            String finalTnx = tnxMsg;
            for(int j=0; j<tnxColumns.length; j++) {
                int startIndex = finalTnx.indexOf("<"+tnxColumns[j]+">")+tnxColumns[j].length()+2;
                int lastIndex = finalTnx.indexOf("</"+tnxColumns[j]+">");
                finalTnx = finalTnx.substring(0,startIndex)+tnxColumnValuePrefixes[j]+count+finalTnx.substring(lastIndex);
            }

            String finalTe = teMsg;
            for(int j=0; j<teColumns.length; j++) {
                int startIndex = finalTe.indexOf("<"+teColumns[j]+">")+teColumns[j].length()+2;
                int lastIndex = finalTe.indexOf("</"+teColumns[j]+">");
                finalTe = finalTe.substring(0,startIndex)+teColumnValuePrefixes[j]+count+finalTe.substring(lastIndex);
            }

                dealCombined = dealCombined + finalDeal;
                tnxCombined = tnxCombined+finalTnx;
                teCombined = teCombined + finalTe;

                if(count % noOfMessagesInFile == 0) {
                    Writer dealWriter = new FileWriter(simulatorBaseDir + "deal/" + count + ".xml");
                    dealWriter.write(dealCombined);
                    dealWriter.close();
                    dealCombined ="";

                    Writer tnxWriter = new FileWriter(simulatorBaseDir + "tnx/" + count + ".xml");
                    tnxWriter.write(tnxCombined);
                    tnxWriter.close();
                    tnxCombined = "";

                    Writer teWriter = new FileWriter(simulatorBaseDir + "te/" + count + ".xml");
                    teWriter.write(teCombined);
                    teWriter.close();
                    teCombined="";
                }

        }

            Writer wr = new FileWriter(simulatorBaseDir+"bkIdCount.txt");
            wr.write(new Long(count).toString());
            wr.close();

        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            try {
                Writer wr = new FileWriter(simulatorBaseDir+"bkIdCount.txt");
                wr.write(new Long(count).toString());
                wr.close();
            }catch (Exception e1){
                e1.printStackTrace();
            }

        }
    }
}
