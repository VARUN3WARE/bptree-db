#include <iostream>
#include <cstring>
#include <cstdlib>
#include <ctime>

#define DATA_SIZE 100

extern "C" {
    void initTree();
    void closeTree();
    int writeData(int key, const char* data);
    char* readData(int key);
    char** readRangeData(int lowerKey, int upperKey, int* n);
    int deleteData(int key);
}

void print_data(const char* data) {
    if (data == nullptr) {
        std::cout << "NULL" << std::endl;
        return;
    }
    std::cout << "Data: ";
    for (int i = 0; i < 10 && i < DATA_SIZE; i++) {
        std::cout << (int)(unsigned char)data[i] << " ";
    }
    std::cout << ".." << std::endl;
}

int main() {
    initTree();
    
    std::cout << "-- B+ Tree Index Testing --" << std::endl << std::endl;
    
    // Test 1: Basic insert and read
    std::cout << "Test 1: Basic Insert and Read" << std::endl;
    char data1[DATA_SIZE] = {0};
    strcpy(data1, "Welcome to the Universe");
    
    if (writeData(100, data1)) {
        std::cout << " Inserted key 100" << std::endl;
    }
    
    char* result = readData(100);
    if (result && strcmp(result, "Welcome to the Universe") == 0) {
        std::cout << " Read key 100 successfully: " << result << std::endl;
    } else {
        std::cout << " Failed to read key 100" << std::endl;
    }
    std::cout << std::endl;
    
    // Test 2: Multiple inserts
    std::cout << "Test 2: Multiple Inserts" << std::endl;
    for (int i = 1; i <= 50; i++) {
        char data[DATA_SIZE] = {0};
        snprintf(data, DATA_SIZE, "Data for key %d", i);
        writeData(i, data);
    }
    std::cout << " Inserted 50 keys (1-50)" << std::endl;
    

    result = readData(25);
    if (result) {
        std::cout << " Read key 25: " << result << std::endl;
    }
    std::cout << std::endl;
    
    // Test 3: Range query
    std::cout << "Test 3: Range Query" << std::endl;
    int n = 0;
    char** range_results = readRangeData(10, 15, &n);
    if (range_results) {
        std::cout << " Range query [10-15] returned " << n << " results:" << std::endl;
        for (int i = 0; i < n; i++) {
            std::cout << "  " << range_results[i] << std::endl;
            delete[] range_results[i];
        }
        delete[] range_results;
    }
    std::cout << std::endl;
    
    // Test 4: Delete
    std::cout << "Test 4: Delete Operation" << std::endl;
    if (deleteData(25)) {
        std::cout << " Deleted key 25" << std::endl;
    }
    
    result = readData(25);
    if (result == nullptr) {
        std::cout << " Verified key 25 is deleted" << std::endl;
    } else {
        std::cout << " Key 25 still exists after deletion" << std::endl;
    }
    std::cout << std::endl;
    
    // Test 5: Large dataset
    std::cout << "Test 5: Large Dataset (1000 keys)" << std::endl;
    clock_t start = clock();
    for (int i = 1000; i < 2000; i++) {
        char data[DATA_SIZE] = {0};
        snprintf(data, DATA_SIZE, "Large dataset key %d", i);
        writeData(i, data);
    }
    clock_t end = clock();
    double insert_time = double(end - start) / CLOCKS_PER_SEC;
    std::cout << " Inserted 1000 keys in " << insert_time << " seconds" << std::endl;
    
    // Random reads
    start = clock();
    int success = 0;
    for (int i = 0; i < 100; i++) {
        int key = 1000 + (rand() % 1000);
        if (readData(key) != nullptr) {
            success++;
        }
    }
    end = clock();
    double read_time = double(end - start) / CLOCKS_PER_SEC;
    std::cout << " Performed 100 random reads in " << read_time << " seconds" << std::endl;
    std::cout << " Success rate: " << success << "/100" << std::endl;
    std::cout << std::endl;
    
    // Test 6: Range query on large dataset
    std::cout << "Test 6: Large Range Query" << std::endl;
    n = 0;
    range_results = readRangeData(1500, 1550, &n);
    if (range_results) {
        std::cout << " Range query [1500-1550] returned " << n << " results" << std::endl;
        for (int i = 0; i < n; i++) {
            delete[] range_results[i];
        }
        delete[] range_results;
    }
    std::cout << std::endl;
    
    // Test 7: Non-existent key
    std::cout << "Test 7: Non-existent Key" << std::endl;
    result = readData(99999);
    if (result == nullptr) {
        std::cout << " Correctly returned NULL for non-existent key" << std::endl;
    } else {
        std::cout << " Returned data for non-existent key" << std::endl;
    }
    std::cout << std::endl;
    
    std::cout << " All Tests Completed " << std::endl;
    
    closeTree();
    return 0;
}
