package com.bigdata.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NgramPartitioner extends Partitioner<Text, IntWritable>{
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks){
//        System.out.println("Number of reducer tasks: "+numReduceTasks);
        if (numReduceTasks == 5){
            Character partitionKey = key.toString().toLowerCase().charAt(0);
            if (partitionKey >= 'a' && partitionKey <= 'd')
                return 0;
            else if (partitionKey >= 'e' && partitionKey <='k')
                return 1;
            else if (partitionKey >= 'l' && partitionKey <= 'p')
                return 2;
            else if (partitionKey >= 'q' && partitionKey <= 'z')
                return 3;
            else {
                return 4;
            }
        }
        else if (numReduceTasks == 0){
            return 0;
        }
        else{
//            System.err.println("Supported partitioner number is 5 or 0.");
            return 0;
        }
    }



}
