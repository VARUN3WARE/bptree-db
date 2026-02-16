#include <iostream>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <ctime>

using namespace std;
using namespace std::chrono;

#define DATA_SIZE 100

extern "C" {
    void initTree();
    void closeTree();
    int writeData(int key, const char* data);
    char* readData(int key);
    char** readRangeData(int lowerKey, int upperKey, int* n);
    int deleteData(int key);
}

void printSeparator() {
    cout << "================================================\n";
}

int main() {
    srand(time(NULL));
    
    cout << "\n";
    printSeparator();
    cout << " B+ TREE PERFORMANCE BENCHMARK\n";
    printSeparator();
    cout << "\n";
    
    initTree();
    cout << " B+ Tree initialized\n\n";
    
    // ========================================
    // Test 1: Sequential Insert (100k records)
    // ========================================
    printSeparator();
    cout << "TEST 1: Sequential Insert (100,000 records)\n";
    printSeparator();
    
    auto start = high_resolution_clock::now();
    for (int i = 0; i < 100000; i++) {
        char data[DATA_SIZE];
        snprintf(data, DATA_SIZE, "Record_%d_Data", i);
        writeData(i, data);
        
        // Progress indicator
        if ((i + 1) % 20000 == 0) {
            cout << "  Inserted " << (i + 1) << " records.\n";
        }
    }
    auto end = high_resolution_clock::now();
    auto insert_time = duration_cast<milliseconds>(end - start).count();
    
    cout << "\n Sequential Insert Time: " << insert_time << " ms\n";
    cout << "  Average: " << (insert_time / 100.0) << " ms per 1000 inserts\n";
    cout << "  Throughput: " << (100000.0 / insert_time * 1000) << " inserts/sec\n";
    cout << "\n";
    
    // ========================================
    // Test 2: Random Search (10k searches)
    // ========================================
    printSeparator();
    cout << "TEST 2: Random Search (10,000 searches)\n";
    printSeparator();
    
    int successful_reads = 0;
    start = high_resolution_clock::now();
    
    for (int i = 0; i < 10000; i++) {
        int key = rand() % 100000;
        char* result = readData(key);
        if (result != nullptr) {
            successful_reads++;
        }
    }
    
    end = high_resolution_clock::now();
    auto search_time = duration_cast<milliseconds>(end - start).count();
    
    cout << "\n Random Search Time: " << search_time << " ms\n";
    cout << "  Successful reads: " << successful_reads << "/10000\n";
    cout << "  Average: " << (search_time / 10.0) << " ms per 1000 searches\n";
    cout << "  Throughput: " << (10000.0 / search_time * 1000) << " searches/sec\n";
    cout << "\n";
    
    // ========================================
    // Test 3: Range Queries (100 queries)
    // ========================================
    printSeparator();
    cout << "TEST 3: Range Queries (100 range queries)\n";
    printSeparator();
    
    int total_records_found = 0;
    start = high_resolution_clock::now();
    
    for (int i = 0; i < 100; i++) {
        int lower = rand() % 99000;  
        int upper = lower + (rand() % 1000);  
        
        int count = 0;
        char** results = readRangeData(lower, upper, &count);
        
        if (results != nullptr) {
            total_records_found += count;

            for (int j = 0; j < count; j++) {
                delete[] results[j];
            }
            delete[] results;
        }
    }
    
    end = high_resolution_clock::now();
    auto range_time = duration_cast<milliseconds>(end - start).count();
    
    cout << "\n Range Query Time: " << range_time << " ms\n";
    cout << "  Total records retrieved: " << total_records_found << "\n";
    cout << "  Average per query: " << (total_records_found / 100.0) << " records\n";
    cout << "  Average time per query: " << (range_time / 100.0) << " ms\n";
    cout << "  Throughput: " << (100.0 / range_time * 1000) << " queries/sec\n";
    cout << "\n";
    
    // ========================================
    // Test 4: Mixed Operations (10k operations)
    // ========================================
    printSeparator();
    cout << "TEST 4: Mixed Operations\n";
    cout << "  40% Read, 30% Insert, 20% Range, 10% Delete\n";
    printSeparator();
    
    int next_insert_key = 100000;  
    int ops_read = 0, ops_insert = 0, ops_range = 0, ops_delete = 0;
    
    start = high_resolution_clock::now();
    
    for (int i = 0; i < 10000; i++) {
        int op = rand() % 100;
        
        if (op < 40) {

            int key = rand() % next_insert_key;
            readData(key);
            ops_read++;
            
        } else if (op < 70) {

            char data[DATA_SIZE];
            snprintf(data, DATA_SIZE, "Mixed_Record_%d", next_insert_key);
            writeData(next_insert_key, data);
            next_insert_key++;
            ops_insert++;
            
        } else if (op < 90) {

            int lower = rand() % (next_insert_key - 100);
            int upper = lower + (rand() % 100);
            int count = 0;
            char** results = readRangeData(lower, upper, &count);
            if (results != nullptr) {
                for (int j = 0; j < count; j++) {
                    delete[] results[j];
                }
                delete[] results;
            }
            ops_range++;
            
        } else {

            int key = rand() % next_insert_key;
            deleteData(key);
            ops_delete++;
        }
    }
    
    end = high_resolution_clock::now();
    auto mixed_time = duration_cast<milliseconds>(end - start).count();
    
    cout << "\n Mixed Operations Time: " << mixed_time << " ms\n";
    cout << "  Operations breakdown:\n";
    cout << "    - Reads: " << ops_read << "\n";
    cout << "    - Inserts: " << ops_insert << "\n";
    cout << "    - Range queries: " << ops_range << "\n";
    cout << "    - Deletes: " << ops_delete << "\n";
    cout << "  Average: " << (mixed_time / 10.0) << " ms per 1000 operations\n";
    cout << "  Throughput: " << (10000.0 / mixed_time * 1000) << " ops/sec\n";
    cout << "\n";
    
    // ========================================
    // Summary
    // ========================================
    printSeparator();
    cout << "PERFORMANCE SUMMARY\n";
    printSeparator();
    
    long long total_time = insert_time + search_time + range_time + mixed_time;
    
    cout << "\nTotal execution time: " << total_time << " ms\n";
    cout << "\nBreakdown:\n";
    cout << "  Test 1 (Sequential Insert): " << insert_time << " ms ("
         << (insert_time * 100.0 / total_time) << "%)\n";
    cout << "  Test 2 (Random Search):     " << search_time << " ms ("
         << (search_time * 100.0 / total_time) << "%)\n";
    cout << "  Test 3 (Range Queries):     " << range_time << " ms ("
         << (range_time * 100.0 / total_time) << "%)\n";
    cout << "  Test 4 (Mixed Operations):  " << mixed_time << " ms ("
         << (mixed_time * 100.0 / total_time) << "%)\n";
    
    cout << "\nOverall Performance Score: ";
    if (total_time < 3000) {
        cout << "EXCELLENT \n";
    } else if (total_time < 5000) {
        cout << "VERY GOOD \n";
    } else if (total_time < 10000) {
        cout << "GOOD \n";
    } else {
        cout << "NEEDS OPTIMIZATION\n";
    }
    
    cout << "\n";
    printSeparator();
    
    closeTree();
    cout << " B+ Tree closed and all changes saved\n\n";
    
    return 0;
}
