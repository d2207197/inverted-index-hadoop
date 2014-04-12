build-inverted-index-hadoop
===========================


## BuildInvertedIndex
### Mapper

Input

    file1: hello world hello

Output

    (hello file1): (file1 1 [1])
    (world file1): (file1 1 [2])
    (hello file1): (file1 1 [3])

Input

    file2: hello world world

Output

    (hello file2): (file2 1 [1])
    (world file2): (file2 1 [2])
    (world file2): (file2 1 [3])

### Combiner

Input

    (hello file1): (file1 1 [1])
                   (file1 1 [3])
				   
	(world file1): (file1 1 [2])

	(hello file2): (file2 1 [1])

	(world file2): (file2 1 [2])
	               (file2 1 [3])

Output

    (hello file1): (file1 2 [1 3]) 
	(world file1): (file1 1 [2])
    (hello file2): (file2 1 [1]) 
	(world file2): (file2 2 [2 3])

### Partitioner

partition by term only

    (hello file1) to hashCode('hello')
    (world file1) to hashCode('world')
	(hello file2) to hashCode('world')
	(world file2) to hashCode('world')

### GroupComparator

group by term only

    (hello file1).getFirst() compareTo (hello file2).getFirst()
    (world file1).getFirst() compareTo (world file2).getFirst()

### SortComparator

sort by term and file name. 

### Reducer

    (hello): (file1 2 [1 3])
	         (file2 1 [1])

    (hello 2): (file1 2 [1 3]) (file2 1 [1])




## Retrieval

### Mapper
file1 : tf-idf_aaa, offsets
file2 : tf-idf_aaa, offsets
file3 : tf-idf_aaa, offsets

file2 : tf-idf_bbb, offsets
file3 : tf-idf_bbb, offsets
file4 : tf-idf_bbb, offsets

file3 : tf-idf_ccc, offsets
file4 : tf-idf_ccc, offsets
file5 : tf-idf_ccc, offsets


aaa OR bbb ccc
=>


'aaa OR bbb' file1 : tf-idf_aaa
'aaa OR bbb' file2 : tf-idf_aaa offsets, tf-idf_bbb offsets
'aaa OR bbb' file3 : 
'aaa OR bbb' file4

fileName: String, score: Double, {term: offsets, term: offsets}

1   3 5 6 7 8
  2     6
