package com.bts.inputsimulator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

/**
 * Created by cloudera on 8/29/17.
 */
public class BTSMessageSender {
    public static void main(String[] args) {
        try {
            String simulatorBaseDir ="/home/cloudera/simulator/";
            String flumeBaseDir = "/home/cloudera/";
            long scheduleMillis = 1000 * Long.parseLong(args[0]);
            Integer noOfMessagesInBatch = Integer.parseInt(args[1]);
            int noOfMessagesInFile = Integer.parseInt(args[2]);

            SenderTask senderTask = new SenderTask(noOfMessagesInBatch, simulatorBaseDir, flumeBaseDir,noOfMessagesInFile);
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(senderTask, 0, scheduleMillis);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

class SenderTask extends TimerTask{

    private Integer noOfMessages;
    private  Long count;
    private String simulatorBaseDir;
    private String flumeBaseDir;
    private int noOfMessagesInFile;

    public SenderTask(Integer noOfMessages, String simulatorBaseDir, String flumeBaseDir,int noOfMessagesInFile ) throws IOException {
        this.noOfMessages = noOfMessages;
        this.simulatorBaseDir = simulatorBaseDir;
        this.flumeBaseDir = flumeBaseDir;
        this.noOfMessagesInFile = noOfMessagesInFile;
        Scanner scanner = new Scanner(new File(simulatorBaseDir+"MessagesSentCount.txt"));
        try {
            count = scanner.nextLong();
        }catch (Exception e){
            File file = new File(simulatorBaseDir+"deal/");
            List<Long> fileNumbers = new LinkedList<Long>();
            for(String filename : file.list()){
                fileNumbers.add(Long.parseLong(filename.replace(".xml","")));
            }
            Collections.sort(fileNumbers);
            count = fileNumbers.get(0) -noOfMessagesInFile;
        }

    }
    @Override
    public void run() {
        System.out.println("Messages Sent count = " + count);
        System.out.println("Current Time = " + new Date().toString());
        try {

            for(int i=1 ; i<=noOfMessages/noOfMessagesInFile; i++ ) {
                count = count+10;
                Long msgId = count;
                Files.move(Paths.get(simulatorBaseDir + "deal/"+ msgId +".xml"), Paths.get(flumeBaseDir + "spool-deal/"+ msgId +".xml"), StandardCopyOption.REPLACE_EXISTING);
                Files.move(Paths.get(simulatorBaseDir + "tnx/"+ msgId +".xml"), Paths.get(flumeBaseDir + "spool-transaction/"+ msgId +".xml"),StandardCopyOption.REPLACE_EXISTING);
                Files.move(Paths.get(simulatorBaseDir + "te/"+ msgId +".xml"), Paths.get(flumeBaseDir + "spool-te/"+ msgId +".xml"),StandardCopyOption.REPLACE_EXISTING);
                Writer wr = new FileWriter(simulatorBaseDir+"MessagesSentCount.txt");
                wr.write(new Long(count).toString());
                wr.close();
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                Writer wr = new FileWriter(simulatorBaseDir+"MessagesSentCount.txt");
                wr.write(new Long(count).toString());
                wr.close();
            }catch (Exception e1){
                e1.printStackTrace();
            }
        }
    }
}
