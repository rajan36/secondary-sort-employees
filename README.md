# Secondary-sort-employees

This program is working example of mapreduce program, which uses secondary sorting to group and sort employees data.

Employees DataSet

| EmpID |	DOB |	FName |	LName |	Gender |	Hire_date |	DeptID |
| ----- | ----- | ----- | ----- | ----- | ----- | ----- |
|10003|	12/3/1959|	Parto|	Bamford|	M|	8/28/1986|	d004|
|10004|	5/1/1954|	Chirstian|	Koblick|	M|	12/1/1986|	d004|
|10005|	1/21/1955|	Kyoichi|	Maliniak|	M|	9/12/1989|	d003|
|10006|	12/3/1959|	Utsav|	Chatterjee|	M|	8/28/1986|	d004|
|10007|	5/1/1954|	Utsav|	C.|	M|	12/1/1986|	d005|
|10008|	1/21/1955|	Michael |	Gross|	M|	9/12/1989|	d006|
|10009|	12/3/1959|	Rodney|	Purtle|	M|	8/28/1986|	d007|
|10010|	5/1/1954|	Ahishek |	Kumar|	M|	12/1/1986|	d007|
|10011|	1/21/1955|	Abhilash|	Whatever|	M|	9/12/1989|	d007|
|10012|	12/3/1959|	Ranga|	Something|	M|	8/28/1986|	d003|
|10013|	5/1/1954|	John|	Doe|	M|	12/1/1986|	d006|
|10014|	1/21/1955|	John |	Talburt|	M|	9/12/1989|	d006|
|10015|	12/3/1959|	Daniel |	Pullen|	M|	8/28/1986|	d006|
|10016|	5/1/1954|	Ross|	Hupchuck|	M|	12/1/1986|	d006|
|10017|	1/21/1955|	Mountain|	Dew|	M|	9/12/1989|	d005|
|10018|	12/3/1959|	Coca|	Cola|	M|	8/28/1986|	d005|
|10019|	5/1/1954|	Pepsi|	Cola|	M|	12/1/1986|	d005|
|10020|	1/21/1955|	Janhavee|	Pande|	F|	9/12/1989|	d005|

Sort this dataset by deptID asc order and LName,FName,EmpID desc order.
Output DataSet 

|DeptID|	LName |	FName |	EmpID |
|----|----|----|----|
|d003|	Something|	Ranga|	10012|
|d003|	Maliniak|	Kyoichi|	10005|
|d004|	Koblick|	Chirstian|	10004|
|d004|	Chatterjee|	Utsav|	10006|
|d004|	Bamford|	Parto|	10003|
|d005|	Pande|	Janhavee|	10020|
|d005|	Dew|	Mountain|	10017|
|d005|	Cola|	Pepsi|	10019|
|d005|	Cola|	Coca|	10018|
|d005|	C.|	Utsav|	10007|
|d006|	Talburt|	John|	10014|
|d006|	Pullen|	Daniel|	10015|
|d006|	Hupchuck|	Ross|	10016|
|d006|	Gross|	Michael|	10008|
|d006|	Doe|	John|	10013|
|d007|	Whatever|	Abhilash|	10011|
|d007|	Purtle|	Rodney|	10009|
|d007|	Kumar|	Ahishek|	10010|

This should be the output of the mapreduce program.

## Class Descriptions

### CompositeKey 
Custom Writable class, which implements WritableComparable interface. It holds DeptId,LName,FName,EmpID.

### Mapper: SSEMapper
Outputs  key-value pair, Key as CompositeKey(deptId-lastName-firstName-empID) & value as IntWritable.  

### Partitioner: SSEPartitioner
Partitions data based on deptId hashcode.

### SortingComparator: SSESortingComparator
Sorting Comparator which sorts keys in map tasks. It sorts keys based on deptID asc, if deptId is same then it will sort keys by lastName-firstName-empID desc order. It extends WritableComparator class. It overrides compare method which compares two keys.

### GroupingComparator: SSEGroupingComparator
Grouping Comparator which groups keys in reduce tasks. It will group all keys with same deptId. It extends WritableComparator Class. It overrides compare method which compared two keys.

### Reducer: SSEReducer
Reducer class takes input CompositeKey,Iterator<Int> . for each iterator it will write data to context (deptID-lastName-firstName-empId).
  
### Driver: SSEDriver
Driver class which initializes hadoop mapreduce job with relevant configurations.
  

