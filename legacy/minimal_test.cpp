#include <iostream>
#include <cstring>

#define DATA_SIZE 100

extern "C" {
    void initTree();
    void closeTree();
    int writeData(int key, const char* data);
    char* readData(int key);
}

int main() {
    std::cout << "Starting minimal test..." << std::endl;
    
    std::cout << "1. Initializing tree." << std::endl;
    initTree();
    std::cout << "   Tree initialized" << std::endl;
    
    std::cout << "2. Creating test data..." << std::endl;
    char data1[DATA_SIZE];
    memset(data1, 0, DATA_SIZE);
    strcpy(data1, "Hello");
    std::cout << "   Data created: " << data1 << std::endl;
    
    std::cout << "3. Writing data with key 100..." << std::endl;
    int result = writeData(100, data1);
    std::cout << "   Write result: " << result << std::endl;
    
    std::cout << "4. Reading data with key 100..." << std::endl;
    char* read_result = readData(100);
    if (read_result) {
        std::cout << "   Read result: " << read_result << std::endl;
    } else {
        std::cout << "   Read result: NULL" << std::endl;
    }
    
    std::cout << "5. Closing tree..." << std::endl;
    closeTree();
    std::cout << "   Tree closed" << std::endl;
    
    std::cout << "Test completed successfully!" << std::endl;
    return 0;
}
