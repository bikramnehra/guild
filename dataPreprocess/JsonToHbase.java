import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonToHbase extends Configured implements Tool
{
	public static class JsonHbaseReducer
    extends TableReducer<LongWritable,Text,LongWritable>
	{
		static class RetMetaData
		{
			public String key;
			public String family;
			public String col;
		}
		static RetMetaData getMetaData(String line) throws JsonParseException, JsonMappingException, IOException
		{
			RetMetaData ret = new RetMetaData();
			ObjectMapper json_mapper = new ObjectMapper();
			JsonNode data = json_mapper.readValue(line.toString(), JsonNode.class);
			ret.key = data.get("Col_Key").asText();
			ret.family = data.get("Col_Family").textValue();
			ret.col = data.get("Col_Name").textValue();
			return ret;
		}
		/**
		 * 
		 * @param key templatic key
		 * @param values iterator for lines that we need to insert in database
		 * @param context
		 * @throws JsonMappingException 
		 * @throws JsonParseException 
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		static public Put get_put(String line) throws ParseException, JsonParseException, JsonMappingException, IOException
		{

			RetMetaData ret = getMetaData(line);
			Put put = new Put(Bytes.toBytes(ret.key));
			put.addColumn(Bytes.toBytes(ret.family), 
					Bytes.toBytes(ret.col), 
					Bytes.toBytes(line));
			return put;
		}
		@Override
		public void reduce(LongWritable key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException
		{
			for(Text val : values)
				try {
					Put put = get_put(val.toString());
					
					context.write(new LongWritable(val.toString().hashCode()),
							put);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(-1);
				}
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(),
                new JsonToHbase(),
                args);
        System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf,"Json to Hbase");

		job.setJarByClass(JsonToHbase.class);
		job.setInputFormatClass(TextInputFormat.class);

		job.setReducerClass(JsonHbaseReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
	

		TextInputFormat.addInputPath(job,new Path(args[0]));
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.initTableReducerJob(args[1], JsonHbaseReducer.class, job);
		job.setNumReduceTasks(3);
		return job.waitForCompletion(true) ? 0: 1;
	}
}
