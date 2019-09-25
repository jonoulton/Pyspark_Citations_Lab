\# CSCI 4253 / 5253 - Lab 4 - Patent Join in PySpark

In this lab, I use PySpark's RDD and DataFrame interfaces to solve the patent join problem.

## The Patent join problem explained

The goal of the patent join problem is to find *self-state patent citations*. Given two datasets, `cite75_99.txt.gz` and `apat63_99.txt.gz`.

The gzip'd datasets are included in the repository

PySpark can automatically uncompress gzip'd files.

The `acite75_99.txt` file contains a citation index, of the form
```
CITING CITED
```
where both `CITING` and `CITED` are integers. Each line
indicates that patent number `CITING` cites patent `CITED`.

The `pat63_99.txt` file contains the patent number, an (optional)
state in which the patent is filed and the total number of citations
made.

## Project Objective
My code augments the data in `pat63_99.txt` to include a column
indicating the number of patents cited that originate *from the same
state*. Obviously, this data can only be calculated for patents that
have originating state information (and thus, only those from the US) and only for cited patents that provide that information. 

For example, 
patent 6009554 (the last patent in pat63_99.txt) cited 9 patents. Those patents were awarded to people in
* NY, 
* IL, 
* Great Britain (no state), 
* NY, 
* NY,
* FL,
* NY,
* NY,
* NY. 

Therefore, my code updates the patent information line from:
```
6009554,1999,14606,1997,"US","NY",219390,2,,714,2,22,9,0,1,,,,12.7778,0.1111,0.1111,,
```

To be: 
```
6009554,1999,14606,1997,"US","NY",219390,2,,714,2,22,9,0,1,,,,12.7778,0.1111,0.1111,,6
```

The last value `,6` is the number of same-state citations.

The 2nd purpose of my project is to report the ten patents that have the most self-state citations sorted in descending order.

## Project Design

To do this, I first do a "data join” of the citations and
the patent data - for each cited patent, I determine the
state of the cited patent. You can then use that information to
produce the augmented patent information.

I produce an intermediate result, called "states", of the form:

|Cited|Cited_State|Citing|Citing_State|
|-----|-----|------|-----|
|2134795	|None	|5654603	|OH
|2201699	|None	|5654603	|OH
|3031593	|None	|5654603	|OH
|3093764	|OH	|5654603	|OH
|3437858	|OH	|5654603	|OH
|3852137	|PA	|5654603	|OH
|3904724	|PA	|5654603	|OH

This table says that patent `3852137` is from `PA` and `5654603` is from `OH`.
I construct this for each citing patent. From this, it's simple to determine
how many patents are self-cited for a given patent data line and then group and count those.

There are some complications:
* Not all patents in the 'cited' table are in the 'patent' table
* Not all patents cite other patents
* Not all patents are cited by other patents
* Lastly, the NaN/Null value used by PySpark makes sorting values involving Nan/Null and numeric values problematic; I filter out these `null` values prior to sorting.

## A Note on Implementations

This repository holds two jupyter notebook files, both written in Python 3. They accomplish the same purpose, both using the Pyspark platform, but do so in slightly different ways:
1. The "Lab-04-patent-dataframe.ipynb" file performs the operations using the Pyspark Dataframe class
2. The "Lab-04-patent-rdd.ipynb" file performs the operations using the Pyspark RDD class
