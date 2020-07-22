package mapreduce;

import javafx.scene.shape.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.DoubleWritable;
import preprocess.TextTokenizer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;

import java.util.ArrayList;
import java.util.List;

public class EMailMapper3 extends Mapper<Object, Text, Text, Text>{
    List<String> wordList = new ArrayList<String>();
    List<Double> idfList = new ArrayList<Double>();
    public void setup(Context context) throws IOException, InterruptedException{
        String idffilename = context.getProfileParams();
        FileReader fr = new FileReader(idffilename);
        BufferedReader bf = new BufferedReader(fr);
        String str;
        while ((str = bf.readLine()) != null)
        {
            String word = str.split("\t")[0];
            double idf = Double.parseDouble(str.split("\t")[1]);
            wordList.add(word);
            idfList.add(idf);
        }

    }
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        String str = value.toString();
        String word = str.split("\t")[0];
        double tf = Double.parseDouble(str.split("\t")[1]);

        int index = wordList.indexOf(word);
        if(index!=-1)
        {
            Text newkey = new Text(fileName);
            Double tfidf = tf*idfList.get(index);
            Text newvalue = new Text(word+"#"+tfidf.toString());
            context.write(newkey,newvalue);
        }
    }
}
