# Team Number: 9
# DBMS Assignment
# Team Member Names:
# 1) Arnav Mishra - 12340330
# 2) Sidhesh Kumar Patra - 12342060
# 3) Varun Rao - 12342320
# 4) Aryan Verma - 12340360

# B+ Tree Index Implementation

A high-performance B+ tree index implementation for database systems using memory-mapped I/O for efficient disk operations.

## Features

- **Disk-based B+ tree** with 4096-byte pages
- **Memory-mapped I/O** for fast file operations
- **Integer keys** with 100-byte fixed-size data records
- **Persistent storage** across program runs
- **Range queries** with efficient leaf node traversal
- **Optimized performance** with O3 compilation flags

## Quick Start

```bash
# Build everything
make

# Run tests
make test

# Clean up
make clean
```

## File Structure

```
├── bptree.cpp          # B+ tree implementation
├── driver.cpp          # Test driver program
├── minimal_test.cpp    # Minimal test for debugging
├── size_check.cpp      # Size validation utility
├── Makefile            # Build configuration
└── README.md           # This file
```

## Installation & Compilation

### Prerequisites

- Linux-based OS (Ubuntu recommended)
- g++ compiler with C++17 support
- Standard development tools (make)

### Build Commands

```bash
# Build library and driver
make

# Or build manually:
g++ -std=c++17 -O3 -march=native -fPIC -Wall -c bptree.cpp -o bptree.o
g++ -shared -o libbptree.so bptree.o
g++ -std=c++17 -O3 -march=native -Wall -o driver driver.cpp -L. -lbptree -Wl,-rpath,.
```

### Run Tests

```bash
# Full test suite
make test

# Minimal debug test
make debug

# Check size calculations
make sizes
```

## API Documentation

### Initialization & Cleanup

```c
void initTree(void);
void closeTree(void);
```

**Important:** Always call `initTree()` before using the B+ tree and `closeTree()` before exiting to ensure data is properly saved.

---

### writeData - Insert or Update

**Synopsis**
```c
int writeData(int key, const char* data);
```

**Description**

Inserts a new key-value pair or updates an existing key in the B+ tree index.

**Parameters**
- `key`: Integer key for indexing
- `data`: Pointer to 100-byte data array containing the record

**Return Value**
- Returns `1` (true) on successful insertion/update
- Returns `0` (false) on failure

**Behavior**
- If the key already exists, the data is **updated** (not duplicated)
- If the key is new, it's inserted in sorted order
- Automatically splits nodes when full
- Automatically creates new root when needed

**Example Usage**

```c
#include <cstring>

extern "C" {
    void initTree();
    int writeData(int key, const char* data);
    void closeTree();
}

int main() {
    initTree();
    
    // Insert a single record
    char data1[100] = {0};
    strcpy(data1, "Student: Alice, Grade: A");
    if (writeData(101, data1)) {
        printf("Record inserted successfully\n");
    }
    
    // Insert multiple records
    for (int i = 1; i <= 100; i++) {
        char data[100] = {0};
        snprintf(data, 100, "Record number %d", i);
        writeData(i, data);
    }
    
    // Update existing record
    char updated[100] = {0};
    strcpy(updated, "Student: Alice, Grade: A+");
    writeData(101, updated);  // Updates key 101
    
    closeTree();
    return 0;
}
```

---

### readData - Search by Key

**Synopsis**
```c
char* readData(int key);
```

**Description**

Searches for a key in the B+ tree and retrieves the associated data.

**Parameters**
- `key`: Integer key to search for

**Return Value**
- Returns pointer to 100-byte static data array if key exists
- Returns `NULL` if key not found

**Important Notes**
- The returned pointer points to **static memory** that gets overwritten on the next call
- If you need to keep the data, **copy it immediately** to your own buffer
- Do not free the returned pointer

**Example Usage**

```c
extern "C" {
    void initTree();
    char* readData(int key);
    void closeTree();
}

int main() {
    initTree();
    
    // Search for a key
    char* result = readData(101);
    if (result != NULL) {
        printf("Found: %s\n", result);
        
        // If you need to keep it, copy immediately
        char my_copy[100];
        memcpy(my_copy, result, 100);
    } else {
        printf("Key not found\n");
    }
    
    // Multiple searches
    int keys[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        char* data = readData(keys[i]);
        if (data) {
            printf("Key %d: %s\n", keys[i], data);
        }
    }
    
    closeTree();
    return 0;
}
```

---

### readRangeData - Range Query

**Synopsis**
```c
char** readRangeData(int lowerKey, int upperKey, int* n);
```

**Description**

Retrieves all records with keys in the range [lowerKey, upperKey] (inclusive of both bounds).

**Parameters**
- `lowerKey`: Lower bound of the range (inclusive)
- `upperKey`: Upper bound of the range (inclusive)
- `n`: Pointer to integer where the count of results will be stored

**Return Value**
- Returns array of pointers to 100-byte data arrays
- Returns `NULL` if no keys found in range
- Sets `*n` to the number of results found

**Memory Management**
- Each data array in the result is **dynamically allocated** with `new[]`
- The result array itself is **dynamically allocated** with `new[]`
- **You must free all memory** after use

**Example Usage**

```c
extern "C" {
    void initTree();
    int writeData(int key, const char* data);
    char** readRangeData(int lowerKey, int upperKey, int* n);
    void closeTree();
}

int main() {
    initTree();
    
    // Insert some data
    for (int i = 1; i <= 100; i++) {
        char data[100] = {0};
        snprintf(data, 100, "Record %d", i);
        writeData(i, data);
    }
    
    // Range query: Get all records from key 10 to 20
    int count = 0;
    char** results = readRangeData(10, 20, &count);
    
    if (results != NULL) {
        printf("Found %d records in range [10, 20]:\n", count);
        
        for (int i = 0; i < count; i++) {
            printf("  %s\n", results[i]);
            
            // Free each individual result
            delete[] results[i];
        }
        
        // Free the array itself
        delete[] results;
    } else {
        printf("No records found in range\n");
    }
    
    // Query with no results
    count = 0;
    results = readRangeData(1000, 2000, &count);
    if (results == NULL) {
        printf("No records in range [1000, 2000]\n");
    }
    
    closeTree();
    return 0;
}
```

---

### deleteData - Delete by Key

**Synopsis**
```c
int deleteData(int key);
```

**Description**

Deletes a key and its associated data from the B+ tree.

**Parameters**
- `key`: Integer key to delete

**Return Value**
- Returns `1` (true) if deletion successful
- Returns `0` (false) if key not found

**Behavior**
- Removes the key-value pair from the leaf node
- Shifts remaining records to maintain sorted order
- Does not rebalance nodes (simplified implementation)

**Example Usage**

```c
extern "C" {
    void initTree();
    int writeData(int key, const char* data);
    char* readData(int key);
    int deleteData(int key);
    void closeTree();
}

int main() {
    initTree();
    
    // Insert a record
    char data[100] = {0};
    strcpy(data, "To be deleted");
    writeData(50, data);
    
    // Verify it exists
    if (readData(50) != NULL) {
        printf("Key 50 exists\n");
    }
    
    // Delete it
    if (deleteData(50)) {
        printf("Key 50 deleted successfully\n");
    }
    
    // Verify deletion
    if (readData(50) == NULL) {
        printf("Key 50 no longer exists\n");
    }
    
    // Try to delete non-existent key
    if (!deleteData(999)) {
        printf("Key 999 not found\n");
    }
    
    // Bulk deletion
    for (int i = 10; i <= 20; i++) {
        if (deleteData(i)) {
            printf("Deleted key %d\n", i);
        }
    }
    
    closeTree();
    return 0;
}
```

---

## Complete Example Program

Here's a complete example showing all operations:

```c
#include <iostream>
#include <cstring>

extern "C" {
    void initTree();
    void closeTree();
    int writeData(int key, const char* data);
    char* readData(int key);
    char** readRangeData(int lowerKey, int upperKey, int* n);
    int deleteData(int key);
}

int main() {
    // Initialize
    initTree();
    std::cout << "B+ Tree initialized\n\n";
    
    // 1. Insert data
    std::cout << "=== Inserting Records ===\n";
    for (int i = 1; i <= 50; i++) {
        char data[100] = {0};
        snprintf(data, 100, "Student ID: %d, Name: Student_%d", i, i);
        writeData(i, data);
    }
    std::cout << "Inserted 50 records\n\n";
    
    // 2. Read specific keys
    std::cout << "=== Reading Specific Keys ===\n";
    int test_keys[] = {5, 15, 25, 35, 45};
    for (int key : test_keys) {
        char* result = readData(key);
        if (result) {
            std::cout << "Key " << key << ": " << result << "\n";
        }
    }
    std::cout << "\n";
    
    // 3. Range query
    std::cout << "=== Range Query [10-15] ===\n";
    int count = 0;
    char** results = readRangeData(10, 15, &count);
    if (results) {
        std::cout << "Found " << count << " records:\n";
        for (int i = 0; i < count; i++) {
            std::cout << "  " << results[i] << "\n";
            delete[] results[i];
        }
        delete[] results;
    }
    std::cout << "\n";
    
    // 4. Update existing record
    std::cout << "=== Updating Record ===\n";
    char updated[100] = {0};
    strcpy(updated, "Student ID: 25, Name: Updated_Student");
    writeData(25, updated);
    char* verify = readData(25);
    if (verify) {
        std::cout << "Updated key 25: " << verify << "\n";
    }
    std::cout << "\n";
    
    // 5. Delete records
    std::cout << "=== Deleting Records ===\n";
    for (int i = 40; i <= 45; i++) {
        if (deleteData(i)) {
            std::cout << "Deleted key " << i << "\n";
        }
    }
    
    
    // 6. Test non-existent key
    std::cout << "=== Testing Non-existent Key ===\n";
    if (readData(999) == NULL) {
        std::cout << "Key 999 not found (as expected)\n";
    }
    
    // Cleanup
    closeTree();
    std::cout << "\nB+ Tree closed successfully\n";
    
    return 0;
}
```

**Compile and run:**
```bash
g++ -std=c++17 -O3 -o myprogram myprogram.cpp -L. -lbptree -Wl,-rpath,.
./myprogram
```

## Performance Characteristics

### Time Complexity
- **Insert/Update**: O(log n)
- **Search**: O(log n)
- **Range Query**: O(log n + k), where k is the number of results
- **Delete**: O(log n)

### Space Usage
- **Page Size**: 4096 bytes
- **Leaf Node Capacity**: 35 records per node
- **Internal Node Capacity**: 100 keys per node
- **File Growth**: Dynamic, grows as needed

### Optimization Features
- Memory-mapped I/O for zero-copy file access
- CPU-specific optimizations (-march=native)
- Maximum compiler optimization (-O3)
- Efficient node splitting algorithms
- Sorted data in leaf nodes for fast range queries

## Common Use Cases

### 1. Student Database
```c
// Insert students
writeData(studentID, studentRecord);

// Find specific student
char* student = readData(studentID);

// Get all students in ID range
char** students = readRangeData(1000, 1100, &count);
```

### 2. Time-series Data
```c
// Insert data with timestamp as key
writeData(timestamp, sensorData);

// Query data in time range
char** data = readRangeData(startTime, endTime, &count);
```

### 3. Inventory System
```c
// Insert products
writeData(productID, productInfo);

// Update stock
writeData(productID, updatedInfo);

// Delete discontinued items
deleteData(productID);
```

## Persistence

- Data is stored in `bptree.idx` file
- Automatically persists across program runs
- First page contains metadata (root location)
- Safe to reopen and continue operations
- Call `closeTree()` to ensure all data is flushed to disk

## Troubleshooting

### Compilation Errors

```bash
# Check g++ version (need 7+)
g++ --version

# If compilation fails, try without -march=native
g++ -std=c++17 -O3 -fPIC -Wall -c bptree.cpp
```

### Runtime Issues

**"Segmentation fault"**
- Make sure you call `initTree()` before any operations
- Check that data buffer is exactly 100 bytes
- Ensure proper memory management for range queries

**"File permission denied"**
- Check write permissions in current directory
- Remove old `bptree.idx` file if corrupted: `rm bptree.idx`

**Memory leaks in range queries**
- Always delete individual results: `delete[] results[i]`
- Always delete result array: `delete[] results`

### Debug Tools

```bash
# Check if file is created
ls -lh bptree.idx

# Run with gdb for detailed error
gdb ./driver
run
bt

# Check for memory leaks
valgrind --leak-check=full ./driver
```

## Important Notes

1. **Thread Safety**: This implementation is **not thread-safe**. Use in single-threaded programs only.

2. **Data Size**: All records must be **exactly 100 bytes**. Shorter data should be padded with zeros.

3. **Key Uniqueness**: Keys must be unique. Inserting the same key twice **updates** the existing record.

4. **Deletion**: Delete operation is simplified and **does not rebalance** the tree. For production use, implement proper rebalancing.

5. **Static Memory**: `readData()` returns a static buffer. Copy the data if you need to store multiple results.


## Performance Tips

1. **Batch Inserts**: Insert multiple records before querying for better performance
2. **Sequential Keys**: Using sequential keys (1, 2, 3...) is fastest
3. **Range Queries**: More efficient than multiple single-key lookups
4. **File Cleanup**: Delete `bptree.idx` periodically to avoid fragmentation

## Building for Production

For production use, consider:
- Adding thread synchronization (mutexes)
- Implementing proper node rebalancing after delete
- Adding transaction support
- Implementing buffer management
- Adding compression for data storage
- Implementing B+ tree merging for deletes


