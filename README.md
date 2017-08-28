# BigData_Session06Assignments
YARN Assignments

-------------------------------------------------------------------------------------------------------------------------------------
Assignment 6.1

Write a Map Reduce program that takes the output of Task 5 (refer session 5, assignment 1)
as input, and produce output which is sorted on the total units sold.
You may use a single reducer for the sorting.
Use Sequence File formats as an output for Task 5 and as input for this task

File: 

Assignment 6.1.PDF - step-by-step execution of the code.

Television.JAVA, TelevisionMapper.JAVA & TelevisionReducer.JAVA - java files to calculate number of units sold by each company.

Television2.JAVA, TelevisionMapper2.JAVA & TelevisionReducer2.JAVA - java files to sort results based on number of units sold.

-------------------------------------------------------------------------------------------------------------------------------------
Assignment 6.2

Enhance the Map Reduce program of Task 8 (refer session 6, assignment 1) to use multiple reducers for sorting. The driver should accept three additional values: the minimum units sold, the maximum units sold and number of reducers to use. Use units sold as key and company as value. Write a custom partitioner to divide the keys on the basis of range. Take minimum to be 0 and maximum to be 10. Divide them across 2 reducers.

File:

Assignment 6.2.PDF - step-by-step execution of the code.

TelevisionReducer.java, Television.java, TelevisionMapper.java & TelevisionPartitioner.java - Java programs.

-------------------------------------------------------------------------------------------------------------------------------------
Assignment 6.3

Enhance the task 8 (refer session 6, assignment 1) to calculate the top 3 state-wise sales for each company. You may use multiple reducers for this activity

File:

Assignment 6.3.PDF - step-by-step execution of the code.

Television63a.java, Television63aMapper.java & Television63aReducer.java - Java programs to calculate the state wise sale of each company.

StateSales.java, Television63b.java, Television63bMapper.java & Television63bReducer.java - Java programs to calculate top 3 state wise sale of each company.
