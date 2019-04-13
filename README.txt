LABORATORY SESSION IV: ADVANCED DISTRIBUTED SYSTEMS
MARCH 24, 2019
ALEX3142

Other person



----------------------------------------------------------------------------
-------------------       Contents of Software      ------------------------
----------------------------------------------------------------------------

This submission consists of:

README.txt
MatrixMultiplication.java


----------------------------------------------------------------------------
-------------------       Aims and Objectives      -------------------------
----------------------------------------------------------------------------

The aim of this code is to use Hadoop to perform matrix multiplication using
mapreduce. The code also builds the matrices for the multiplication.

---------------------------------------------------------------------------
----------------------      Runnning the Code      ------------------------
---------------------------------------------------------------------------

In order to run the code you must have Hadoop and java installed. 

1) cd to the location of MatrixMultiplication.java, and run the command: 
"hadoop com.sun.tools.javac.Main MatrixMultiplication.java" this will
produce a number of ".class" files.

2) run the following command: "jar cf matrixmultiplication.jar MatrixMultiplication*.class"
this will produce a single ".jar" files.

3) Then run the following command ( items within "<>" are inputs which will be explained below)
hadoop jar matrixmultiplication.jar MatrixMultiplication <input hdfs> <output hdfs> <nRows_A> <nCols_A> <maxEle_A> <sparsity_A> <nRows_B> <nCols_B> <maxEle_B> <sparsity_B>

<input hdfs> (string) - location of input files on HDFS 
<output hdfs> (string) - location of output files on HDFS 
<nRows_i> (integer) - number rows of matrix i
<nCols_i> (integer) - number columns of matrix i
<maxEle_i> (integer) - the elements of the matrix are random integers between 1 and maxEle_i for matrix i
<sparsity_i> float between [0,1] - the (approximate) proportion of the matrix to be zero valued

Example input :
hadoop jar matrixmultiplication.jar MatrixMultiplication /user/hduser/input/ /user/hduser/output/ 100 100 10 0.25 100 100 50 0.75

This would create a matrix of size 100 by 100 with elements between 0 and 10 with approximately
25% of the elements zero, and second matrix of size 100 by 100 with elements between 0 and 50 
with approximately 75% of the elements zero.

The output of this tool is a file in the output directory (this will need to be deleted for the tool
to be run subsequent times) and the results are also printed out to the screen. The results are the 
elements of the output of the multiplication of the two matrices. 

Note the cerated matrices will be saved in the current working directory for inspection 
should the user want to.


---------------------------------------------------------------------------
-------------------------      The Solution      --------------------------
---------------------------------------------------------------------------

Mapping,

In the mapping phase of the program the key is a location (row_index_output, col_index_output)
and each key mapped to a tuple containing the matrix name (hard coded as A and B) the column index(/row
index) if matrix A(/B) and the element of the input matrix corresponding to, for matrix A
row index "row_index_output" column index j for all j of A, and for B 
row index "column_index_output" row index j of A

This is easier to explain in an example:

Matrix A
A_1_1  A_1_2

A_2_1  A_2_2

Matrix B
B_1_1  B_1_2  B_1_3

B_2_1  B_2_2  B_2_3

Mapping
(1,1) -> (A, 1, A_1_1)
(1,2) -> (A, 1, A_1_1)
(1,3) -> (A, 1, A_1_1)

(1,1) -> (A, 2, A_1_2)
(1,2) -> (A, 2, A_1_2)
(1,3) -> (A, 2, A_1_2)

...

(1,1) -> (B, 1, B_1_1)
(2,1) -> (B, 1, B_1_1)

(1,1) -> (B, 2, B_2_1)
(2,1) -> (B, 2, B_2_1)

...
...
...

Reduce,

In the reduce phase the maps a re grouped by key
and all values whose second input is the same are multiplied
together and then all the products are summed together for 
each key, this produces the element value for the element in
the output matrix corresponding to the key value.



