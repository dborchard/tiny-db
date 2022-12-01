# Page Storage Architecture

Different DBMS manage pages in files on disk in different ways.

## Types

- Heap File Organization (Our Approach)
- Tree File Organization (Davis Base Original Requirement)
- Sorted File Organization (SST)
- Hashing File Organization

## NOTE

Iterator will be using Page to R/W values into data files.

## Flow

- Start with `HeapRecordScan` (Iterable API)
- If the storage is accessed in pages, we will use `HeapRecordPageImpl`
- HeapRecordPageImpl will invoke `Transactions` to fetch data from buffer cache.
- Transactions will call `FileManager` with `Page` containing ByteBuffer.
- `Page's` ByteBuffer is modified already by the Transaction. FileManager will persist it.

## Record Storage

- We are not using Slotted Page (Indirection).
- We are using Fixed Record Size. Even VARCHAR(len), we use len as fixed size (Based on
  maxBytesRequiredForString())