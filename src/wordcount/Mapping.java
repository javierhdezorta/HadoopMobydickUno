package wordcount;

        import java.io.IOException;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Mapper;

public class Mapping extends Mapper<Object,Text,Text,IntWritable>{

    public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
        Text word=new Text();
        IntWritable one = new IntWritable(1);

        String[] words = value.toString().split(" ");//split es que hace una separacion
        for(String string: words){
            string=string.replaceAll("[$?¡¿):#=(*/,;.^+%&!''\"--]","");
            string=string.toLowerCase();
            word.set(string);
            context.write(word, one);//word es lo que contiente key y one es el equivalente a 1 y va contando de 1 en 1.
        }
    }
}
