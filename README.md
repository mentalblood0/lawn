## lawn

Key-value store with mutable data files without trees

### Rationale

Key-value stores are often used as primary data store in database. Usually they are implemented as follows

To store key-value pair, reliably write it to file on disk (this file can be called 'Write Ahead Log') and then write it to in-memory tree structure ('memtable')

To recover key-value pairs on next program launch, read each one of them from WAL and write to memtable

Store recovery can take significant time for large databases, so most of key-value pairs eventually stored not in WAL as they were added, but in more search-and-iteration-friendly data structures on disk. Almost always it is just list of key-value pairs, sorted by keys and called 'Sorted Strings Table'

SST does not eliminate WAL, but key-value pairs from WAL periodically 'checkpointed' into SST. Checkpointing implementation is crucial for amount of writes on disk, as it involves rewriting of sorted lists. In lawn SST is split into index part and data part

Index part is just file filled with records of fixed size, sorted by key. First byte of each record indicates in which data-part file related key-value pair is stored. The remaining bytes form number which is the index of key-value pair in indicated file

Data part consists of 256 files. Each file filled with containers of fixed size. Size of each container determined by logarithm-like splitting of scale from 2 to maximum key-value pair size. Maximum key-value pair size is stated by user in configuration file of store

Each data part file is managed as pool: in the beginning of it there is a pointer to the last freed container. Just the same way the last freed container can point to the second last etc.. Freed containers are reused to keep fragmentation under control

Split into index part and data part reduces disk writes drastically: checkpointing do not mean overwriting entire data, just pointers to it stored in index. But it also increases time needed to access to key-value pair: even when iterating them sequentially, we need one random read for each of them

Need for random reads to retrieve data makes traditional memtable-SST merging strategy inefficient as it lineary depends on amount of key-value pairs stored on disk. Something like insertion sort using binary search may be better: `M` key-value pairs in memtable and `D` key-value pairs on disk mean maximum `2 * M * log2(D)` random disk accesses. lawn uses upgraded version of this algorithm called 'sparsed merge': first we look for the place to insert the middle element, then as we know that all elements before it will be placed on the left of the place we just found, we look for the place to insert the middle-of-memtable-first-half element into index-before-place-for-first-element. Effectively recursive nature of this algorithm not only reduces random accesses amount by half for random data, but also it gives the less random accesses the more 'grouped' and 'cornered' inserted key-value pairs appear in resulted list whereas simple insertion sort does the opposite
