import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.io.FileWriter;

public class MatrixMultiplication {
	
    public static void main(String[] args) throws Exception {
			
			// If there is a number of arguments different to 2, then print in the terminal a "Warning" to let the user
			// know he needs to specify only two arguments:

			// input_directory: HDFS path where the input data is stored.
			// output_directory: HDFS path where the output data is stored.

    	if (args.length != 10) {

            System.err.println("Please provide a HDFS input directory and a HDFS output directory and the correct matrix specifications!");

            System.exit(0);

        }

			PrintReallyReallyBigMatrix(args);

			FileWriteToHDFS(args);

			// We define a new configuration

    	Configuration conf = new Configuration();

        // If A is an m x p matrix and B is an p x n matrix, then the product of A and B is the m x n matrix C = AB,  where the (i,j)th element
				// of C is computed as the inner product of the ith row of A with the jth column of B.

				// We set up the dimensions of our matrices.

        conf.set("m", args[2]);

        conf.set("p", args[3]);

        conf.set("n", args[7]);
				
				// We create a job called MatrixMultiplication with the configuration set up previously.

				Job job = new Job(conf, "MatrixMultiplication");

				// Set the Jar by finding where a given class came from.

        job.setJarByClass(MatrixMultiplication.class);

				// Set the key class for the job output data.

        job.setOutputKeyClass(Text.class);

				// Set the value class for job outputs.

        job.setOutputValueClass(Text.class);

				// Set the Mapper for the job.

        job.setMapperClass(Map.class);

				// Set the Reducer for the job.

        job.setReducerClass(Reduce.class);

				// Set the InputFormat for the job. An InputFormat for plain text files. 
				// Files are broken into lines. Either linefeed or carriage-return are used to signal end of line. 
				// Keys are the position in the file, and values are the line of text.

        job.setInputFormatClass(TextInputFormat.class);

				// Set the OutputFormat for the job. An OutputFormat that writes plain text files.

        job.setOutputFormatClass(TextOutputFormat.class);

				//  Add a Path to the list of inputs for the map-reduce job.

        FileInputFormat.addInputPath(job, new Path(args[0]));

				// Set the Path of the output directory for the map-reduce job.

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

				job.waitForCompletion(true);
 
    }

    public static class Map
  	
		// The input for our map function will be a file that contains the name of the matrix, the nubmer of the row i,
		// the number of the column j and the number of the value in that poisition (i,j). These are going to be text strings.

		extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
		
		@Override

			public void map(LongWritable key, Text value, Context context)

				throws IOException, InterruptedException {

					// We get the configuration defined in the main class.

					Configuration conf = context.getConfiguration();

					// We get the value of p.

					int m = Integer.parseInt(conf.get("m"));

					// We get the value of n.

					int n = Integer.parseInt(conf.get("n"));

					// We set up each line of the input file as string.

					String line = value.toString();

					// As we mentioned previously, the input file has the following format:
					// a file that contains the name of the matrix, the nubmer of the row i,
					// the number of the column j and the number of the value in that poisition
					// (i, j). In other words, we will have in each line the following
					// (A, i, j, Aij) -For example, for matrix A- . 

					// We create an array of string that will contain the strings of each line
					// in the input file.

					String[] MatricesInput = line.split(" ");

					// The output keys from the Map function are going to be Text.

					Text MapKey = new Text();

					// The output values from the Map function are going to be Text.

					Text MapValue = new Text();
					
					// "A" or "B"

					if (MatricesInput[0].equals("A")) {

						for (int k = 0; k < n; k++) {

							// MapKey.set(i,k);

							MapKey.set(MatricesInput[1] + "," + k);

							// MapValue.set(A,j,aij);

							MapValue.set(MatricesInput[0] + "," + MatricesInput[2]+ "," + MatricesInput[3]);

							context.write(MapKey, MapValue);

						}

		} else {

			for (int i = 0; i < m; i++) {

				MapKey.set(i + "," + MatricesInput[2]);

				// MapValue.set(B,j,bjk);

				MapValue.set("B," + MatricesInput[1] + "," + MatricesInput[3]);

				context.write(MapKey, MapValue);

			}

		}

	}

}

public static class Reduce

	// The input for our reduce function will be the output from our reduce function.It will take the (key, value)
	// pairs produced during the map function.

  extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

	@Override

	public void reduce(Text key, Iterable<Text> values, Context context)

			throws IOException, InterruptedException {

				String[] value;

				HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();

				HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();

				for (Text val : values) {

					value = val.toString().split(",");

					if (value[0].equals("A")) {

						hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));

					} else {

						hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));

					}

					}

					// We get the value of p.

					int p = Integer.parseInt(context.getConfiguration().get("p"));

					// We declare the variable for the sum.

					float sum = 0.0f;

					// We declare the variable for aij.

					float a_ij;

					// We declare the variable for bjk.

					float b_jk;

					for (int j = 0; j < p; j++) {

						a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;

						b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;

						sum += a_ij * b_jk;

					}

						context.write(null, new Text("(" + key.toString() + ") " + Float.toString(sum)));

				}

	}

	  public static void FileWriteToHDFS(String[] args) throws IOException, URISyntaxException

   {
      // 1. Get the instance of COnfiguration

      Configuration configuration = new Configuration();

      // 2. Create an InputStream to read the data from local file

			String workingDir = System.getProperty("user.dir");

      InputStream inputStream1 = new BufferedInputStream(new FileInputStream(workingDir + "/A"));

			InputStream inputStream2 = new BufferedInputStream(new FileInputStream(workingDir + "/B"));

      // 3. Get the HDFS instance

      FileSystem hdfs = FileSystem.get(new URI(args[0]), configuration);

      // 4. Open a OutputStream to write the data, this can be obtained from the FileSytem

      OutputStream outputStream1 = hdfs.create(new Path(args[0] + "A"),

      new Progressable() {  

              @Override

              public void progress() {

         System.out.println("....");

              }

                    });

			OutputStream outputStream2 = hdfs.create(new Path(args[0] + "B"),

      new Progressable() {  

              @Override

              public void progress() {

         System.out.println("....");

              }

                    });

      try

      {

        IOUtils.copyBytes(inputStream1, outputStream1, 4096, false); 

				IOUtils.copyBytes(inputStream2, outputStream2, 4096, false); 

      }

      finally

      {

        IOUtils.closeStream(inputStream1);

				IOUtils.closeStream(inputStream2);

        IOUtils.closeStream(outputStream1);

				IOUtils.closeStream(outputStream2);

      } 

  }

  public static void PrintReallyReallyBigMatrix(String[] args) {

        String matrixName_one = "A";

        int nRows_one = Integer.parseInt(args[2]);

        int nCols_one = Integer.parseInt(args[3]);

        int maxSize_one = Integer.parseInt(args[4]);

        float sparsity_one = Float.parseFloat(args[5]);

        String matrixName_two = "B";

        int nRows_two = Integer.parseInt(args[6]);

        int nCols_two = Integer.parseInt(args[7]);

        int maxSize_two = Integer.parseInt(args[8]);

        float sparsity_two = Float.parseFloat(args[9]);

        Random randFloat = new Random();

        float checkSparsity_one;

        float checkSparsity_two;

        try {

            FileWriter writerMatrix_one = new FileWriter(matrixName_one.concat(""));

            FileWriter writerMatrix_two = new FileWriter(matrixName_two.concat(""));

            for (int iRows = 0; iRows < Math.max(nRows_one,nRows_two) ; iRows++) {

                for (int iCols = 0; iCols < Math.max(nCols_one,nCols_two) ; iCols++) {

                    int randInt_one = ThreadLocalRandom.current().nextInt(0, maxSize_one + 1);

                    int randInt_two = ThreadLocalRandom.current().nextInt(0, maxSize_two + 1);

                    checkSparsity_one = randFloat.nextFloat();

                    checkSparsity_two = randFloat.nextFloat();

                    // Write matrix one.

                    if (iRows < nRows_one && iCols < nCols_one) {

                        if (checkSparsity_one < sparsity_one) {

                            String matrixEntry = matrixName_one + " " + iRows + " " + iCols + " "+ 0;

                            writerMatrix_one.write(matrixEntry);

                            writerMatrix_one.write(System.lineSeparator());

                        } else {

                            String matrixEntry = matrixName_one + " " + iRows + " " + iCols + " "+ randInt_one;

                            writerMatrix_one.write(matrixEntry);

                            writerMatrix_one.write(System.lineSeparator());

                        }

                    }

                    // Write matrix two.

                    if (iRows < nRows_two && iCols < nCols_two) {

                        if (checkSparsity_two < sparsity_two) {

                            String matrixEntry = matrixName_two + " " + iRows + " " + iCols + " "+ 0;

                            writerMatrix_two.write(matrixEntry);

                            writerMatrix_two.write(System.lineSeparator());

                        } else {

                            String matrixEntry = matrixName_two + " " + iRows + " " + iCols + " "+ randInt_two;

                            writerMatrix_two.write(matrixEntry);

                            writerMatrix_two.write(System.lineSeparator());

                        }

                    }

                }

            }

            writerMatrix_one.close();

            writerMatrix_two.close();

        } catch (java.io.IOException ioe) {

            System.out.println("File Writer Error");

        }

    }

}