import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.log4j.*;

import scala.Tuple2;

/**
 * @author nidhinraj
 *
 * A sample spark streaming program to understand socket stream.
 * 
 * map vs flatMap transformation example code.
 * 
 * Note: For streaming data use netcat server.
 *		nc -L -p 8888 (Ubundu and Mac OS) 
 *
 *
 */
public class SparkApplication {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 8888);

		//map transformation
		lines.map(line -> new Tuple2<>(line, 1)).print();

		//flatMap transformation
		lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(line -> new Tuple2<>(line, 1)).print();

		//flatMap and filter transformations
		lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).filter(line -> !line.equals("nidhin"))
				.map(line -> new Tuple2<>(line, 1)).print();

		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}

}
