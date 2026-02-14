#include <iostream>
#include <cstring>
#include <string>
#include <limits>

#define DATA_SIZE 100

extern "C" {
    void initTree();
    void closeTree();
    int writeData(int key, const char* data);
    char* readData(int key);
    char** readRangeData(int lowerKey, int upperKey, int* n);
    int deleteData(int key);
}

void clearInputBuffer() {
    std::cin.clear();
    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
}

void printMenu() {
    std::cout << "\n=======================================\n";
    std::cout << "     B+ TREE INDEX - MAIN MENU         \n";
    std::cout << "==========================================\n";
    std::cout << "  1. Insert/Update Record\n";
    std::cout << "  2. Search by Key\n";
    std::cout << "  3. Range Query\n";
    std::cout << "  4. Delete Record\n";
    std::cout << "  5. Bulk Insert\n";
    std::cout << "  6. Display All Records (Range)\n";
    std::cout << "  7. Statistics\n";
    std::cout << "  0. Exit\n";
    std::cout << "================================================\n";
    std::cout << "Enter your choice: ";
}

void insertRecord() {
    std::cout << "\n =====INSERT/UPDATE RECORD ===========\n";
    
    int key;
    std::cout << " Enter key (integer): ";
    if (!(std::cin >> key)) {
        clearInputBuffer();
        std::cout << " == Invalid key! Please enter an integer.==\n";
        std::cout << "============================================\n";
        return;
    }
    clearInputBuffer();
    
    std::cout << " Enter data (max 99 chars): ";
    std::string input;
    std::getline(std::cin, input);
    
    if (input.empty()) {
        std::cout << "=== Data cannot be empty!====\n";
        std::cout << "==============================\n";
        return;
    }
    
    char data[DATA_SIZE] = {0};
    strncpy(data, input.c_str(), DATA_SIZE - 1);
    
    if (writeData(key, data)) {
        std::cout << "=== Record inserted/updated successfully!===\n";
        std::cout << "====   Key: " << key << "\n";
        std::cout << "====   Data: " << data << "\n";
    } else {
        std::cout << "==== Failed to insert/update record!\n";
    }
    std::cout << "===========================================\n";
}

void searchRecord() {
    std::cout << "\n===============SEARCH RECORD=====================\n";
    
    int key;
    std::cout << " Enter key to search: ";
    if (!(std::cin >> key)) {
        clearInputBuffer();
        std::cout << "=== Invalid key! Please enter an integer.\n";
        std::cout << "==========================================\n";
        return;
    }
    clearInputBuffer();
    
    char* result = readData(key);
    if (result != nullptr) {
        std::cout << " Record found!\n";
        std::cout << "   Key: " << key << "\n";
        std::cout << "   Data: " << result << "\n";
    } else {
        std::cout << " Record not found for key " << key << "\n";
    }
    std::cout << "=============================================\n";
}

void rangeQuery() {
    std::cout << "\n=========== RANGE QUERY ===========================\n";
    
    int lowerKey, upperKey;
    std::cout << " Enter lower bound key: ";
    if (!(std::cin >> lowerKey)) {
        clearInputBuffer();
        std::cout << " Invalid key!\n";
        std::cout << "====================================================\n";
        return;
    }
    
    std::cout << " Enter upper bound key: ";
    if (!(std::cin >> upperKey)) {
        clearInputBuffer();
        std::cout << " Invalid key!\n";
        std::cout << "====================================================\n";
        return;
    }
    clearInputBuffer();
    
    if (lowerKey > upperKey) {
        std::cout << " Lower bound must be ≤ upper bound!\n";
        std::cout << "==============================================\n";
        return;
    }
    
    int count = 0;
    char** results = readRangeData(lowerKey, upperKey, &count);
    
    if (results != nullptr) {
        std::cout << " Found " << count << " record(s) in range [" 
                  << lowerKey << ", " << upperKey << "]\n";
        std::cout << "=============================================\n";
        
        for (int i = 0; i < count; i++) {
            std::cout << " [" << (i + 1) << "] " << results[i] << "\n";
            delete[] results[i];
        }
        delete[] results;
    } else {
        std::cout << " No records found in range [" 
                  << lowerKey << ", " << upperKey << "]\n";
    }
    std::cout << "========================================================\n";
}

void deleteRecord() {
    std::cout << "\n==== DELETE RECORD ===================================\n";
    
    int key;
    std::cout << " Enter key to delete: ";
    if (!(std::cin >> key)) {
        clearInputBuffer();
        std::cout << " Invalid key!\n";
        std::cout << "================================================\n";
        return;
    }
    clearInputBuffer();
    

    char* existing = readData(key);
    if (existing == nullptr) {
        std::cout << " Record not found for key " << key << "\n";
        std::cout << "=================================================\n";
        return;
    }
    
    std::cout << " Record to delete:\n";
    std::cout << "   Key: " << key << "\n";
    std::cout << "   Data: " << existing << "\n";
    std::cout << " \n";
    std::cout << " Confirm deletion? (y/n): ";
    
    char confirm;
    std::cin >> confirm;
    clearInputBuffer();
    
    if (confirm == 'y' || confirm == 'Y') {
        if (deleteData(key)) {
            std::cout << " Record deleted successfully!\n";
        } else {
            std::cout << " Failed to delete record!\n";
        }
    } else {
        std::cout << " Deletion cancelled.\n";
    }
    std::cout << "============================================\n";
}

void bulkInsert() {
    std::cout << "\n======== BULK INSERT=============================\n";
    
    int startKey, count;
    std::cout << " Enter starting key: ";
    if (!(std::cin >> startKey)) {
        clearInputBuffer();
        std::cout << " Invalid key!\n";
        std::cout << "===============================================\n";
        return;
    }
    
    std::cout << " Enter number of records to insert: ";
    if (!(std::cin >> count) || count <= 0) {
        clearInputBuffer();
        std::cout << " Invalid count!\n";
        std::cout << "=============================================\n";
        return;
    }
    clearInputBuffer();
    
    std::cout << " Enter data pattern (use %d for key number): ";
    std::string pattern;
    std::getline(std::cin, pattern);
    
    if (pattern.empty()) {
        pattern = "Record_%d";
    }
    
    std::cout << " \n";
    std::cout << " Inserting " << count << " records.\n";
    
    int successCount = 0;
    for (int i = 0; i < count; i++) {
        char data[DATA_SIZE] = {0};
        snprintf(data, DATA_SIZE, pattern.c_str(), startKey + i);
        if (writeData(startKey + i, data)) {
            successCount++;
        }
        
        
        if ((i + 1) % 100 == 0) {
            std::cout << "   Progress: " << (i + 1) << "/" << count << " records\n";
        }
    }
    
    std::cout << "Successfully inserted " << successCount << "/" << count << " records\n";
    std::cout << "   Key range: [" << startKey << ", " << (startKey + count - 1) << "]\n";
    std::cout << "======================================================================\n";
}

void displayAllRecords() {
    std::cout << "\n=== DISPLAY ALL RECORD ================\n";
    
    int lowerKey, upperKey;
    std::cout << " Enter lower bound (or -999999 for all): ";
    if (!(std::cin >> lowerKey)) {
        clearInputBuffer();
        std::cout << " Invalid key!\n";
        std::cout << "=================================================\n";
        return;
    }
    
    std::cout << " Enter upper bound (or 999999 for all): ";
    if (!(std::cin >> upperKey)) {
        clearInputBuffer();
        std::cout << " Invalid key!\n";
        std::cout << "================================================\n";
        return;
    }
    clearInputBuffer();
    
    int count = 0;
    char** results = readRangeData(lowerKey, upperKey, &count);
    
    if (results != nullptr) {
        std::cout << " Found " << count << " record(s)\n";
        std::cout << "======================================================\n";
        
        int displayLimit = 50;
        int displayCount = (count > displayLimit) ? displayLimit : count;
        
        for (int i = 0; i < displayCount; i++) {
            std::cout << " [" << (i + 1) << "] " << results[i] << "\n";
        }
        
        if (count > displayLimit) {
            std::cout << " . (" << (count - displayLimit) << " more records not shown)\n";
            std::cout << " \n";
            std::cout << " Show all? (y/n): ";
            char choice;
            std::cin >> choice;
            clearInputBuffer();
            
            if (choice == 'y' || choice == 'Y') {
                for (int i = displayLimit; i < count; i++) {
                    std::cout << " [" << (i + 1) << "] " << results[i] << "\n";
                }
            }
        }
        

        for (int i = 0; i < count; i++) {
            delete[] results[i];
        }
        delete[] results;
    } else {
        std::cout << " No records found in the specified range\n";
    }
    std::cout << "=========================================================\n";
}

void showStatistics() {
    std::cout << "\n============STATISTICS ==========================\n";
    

    int count = 0;
    char** results = readRangeData(-999999, 999999, &count);
    
    std::cout << " Total records in database: " << count << "\n";
    
    if (results != nullptr && count > 0) {
        std::cout << " \n";
        std::cout << " Sample records:\n";
        int sampleCount = (count > 5) ? 5 : count;
        for (int i = 0; i < sampleCount; i++) {
            std::cout << "   • " << results[i] << "\n";
        }
        
        // Clean up
        for (int i = 0; i < count; i++) {
            delete[] results[i];
        }
        delete[] results;
    } else {
        std::cout << " Database is empty\n";
    }
    
    std::cout << " \n";
    std::cout << " Index file: bptree.idx\n";
    std::cout << " Page size: 4096 bytes\n";
    std::cout << " Data size per record: 100 bytes\n";
    std::cout << " Leaf node capacity: 35 records\n";
    std::cout << " Internal node capacity: 100 keys\n";
    std::cout << "=============================================\n";
}

int main() {
    // Initialize the B+ tree
    std::cout << "\n===========================================\n";
    std::cout << "   B+ TREE INDEX - INTERACTIVE MODE    \n";
    std::cout << "================================================\n";
    std::cout << "\nInitializing B+ Tree.\n";
    
    initTree();
    std::cout << " B+ Tree initialized successfully!\n";
    std::cout << " Index file: bptree.idx\n";
    
    // Main loop
    int choice;
    bool running = true;
    
    while (running) {
        printMenu();
        
        if (!(std::cin >> choice)) {
            clearInputBuffer();
            std::cout << "\n Invalid input! Please enter a number.\n";
            continue;
        }
        clearInputBuffer();
        
        switch (choice) {
            case 1:
                insertRecord();
                break;
                
            case 2:
                searchRecord();
                break;
                
            case 3:
                rangeQuery();
                break;
                
            case 4:
                deleteRecord();
                break;
                
            case 5:
                bulkInsert();
                break;
                
            case 6:
                displayAllRecords();
                break;
                
            case 7:
                showStatistics();
                break;
                
            case 0:
                std::cout << "\n========================================\n";
                std::cout << " Closing B+ Tree.\n";
                closeTree();
                std::cout << " All changes saved to disk\n";
                std::cout << "===========================================\n";
                running = false;
                break;
                
            default:
                std::cout << "\n Invalid choice! Please select 0-7.\n";
        }
    }
    
    return 0;
}
