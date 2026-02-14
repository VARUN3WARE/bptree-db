/*
 * B+ Tree Index Implementation
 */
 
#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>

#define PAGE_SIZE 4096
#define DATA_SIZE 100
#define INDEX_FILE "bptree.idx"

// B+ tree node capacities
const int LEAF_ORDER = 35;       
const int INTERNAL_ORDER = 100;

class BPlusTree {
private:
    int fd;
    char* mapped_file;
    size_t file_size;
    int64_t root_offset;
    int64_t next_page_offset;
    
    void ensure_file_size(int64_t required_size) {
        if (required_size <= (int64_t)file_size) {
            return;
        }
        
        size_t new_size = ((required_size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
        
        // Unmap first
        if (mapped_file && mapped_file != MAP_FAILED) {
            munmap(mapped_file, file_size);
            mapped_file = nullptr;
        }
        
        // Resize file
        if (ftruncate(fd, new_size) != 0) {
            perror("ftruncate failed");
            exit(1);
        }
        
        // Remap
        mapped_file = (char*)mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (mapped_file == MAP_FAILED) {
            perror("mmap failed");
            exit(1);
        }
        
        file_size = new_size;
    }
    
    int64_t allocate_page() {
        int64_t offset = next_page_offset;
        next_page_offset += PAGE_SIZE;
        ensure_file_size(next_page_offset);
        
        // Zero out the new page
        if (offset + PAGE_SIZE <= (int64_t)file_size) {
            memset(mapped_file + offset, 0, PAGE_SIZE);
        }
        
        return offset;
    }
    
    void write_metadata() {
        if (file_size < PAGE_SIZE) {
            ensure_file_size(PAGE_SIZE);
        }
        
        memcpy(mapped_file, &root_offset, sizeof(int64_t));
        memcpy(mapped_file + 8, &next_page_offset, sizeof(int64_t));
        msync(mapped_file, PAGE_SIZE, MS_SYNC);
    }
    
    void read_metadata() {
        if (file_size >= PAGE_SIZE) {
            memcpy(&root_offset, mapped_file, sizeof(int64_t));
            memcpy(&next_page_offset, mapped_file + 8, sizeof(int64_t));
            
            // Validate metadata
            if (next_page_offset < PAGE_SIZE) {
                next_page_offset = PAGE_SIZE;
            }
            if (root_offset != -1 && (root_offset < PAGE_SIZE || root_offset >= (int64_t)file_size)) {
                root_offset = -1;
                next_page_offset = PAGE_SIZE;
            }
        }
    }
    
    bool is_leaf(int64_t offset) {
        if (offset < PAGE_SIZE || offset + 8 > (int64_t)file_size) {
            return false;
        }
        int flag;
        memcpy(&flag, mapped_file + offset + 4, sizeof(int));
        return flag == 1;
    }
    
    // Leaf node operations
    int leaf_get_num_keys(int64_t offset) {
        int n;
        memcpy(&n, mapped_file + offset, sizeof(int));
        return n;
    }
    
    void leaf_set_num_keys(int64_t offset, int n) {
        memcpy(mapped_file + offset, &n, sizeof(int));
    }
    
    int64_t leaf_get_next(int64_t offset) {
        int64_t next;
        memcpy(&next, mapped_file + offset + 8, sizeof(int64_t));
        return next;
    }
    
    void leaf_set_next(int64_t offset, int64_t next) {
        memcpy(mapped_file + offset + 8, &next, sizeof(int64_t));
    }
    
    int leaf_get_key(int64_t offset, int idx) {
        int key;
        int64_t pos = offset + 16 + idx * (4 + DATA_SIZE);
        memcpy(&key, mapped_file + pos, sizeof(int));
        return key;
    }
    
    void leaf_set_key(int64_t offset, int idx, int key) {
        int64_t pos = offset + 16 + idx * (4 + DATA_SIZE);
        memcpy(mapped_file + pos, &key, sizeof(int));
    }
    
    void leaf_get_data(int64_t offset, int idx, char* data) {
        int64_t pos = offset + 16 + idx * (4 + DATA_SIZE) + 4;
        memcpy(data, mapped_file + pos, DATA_SIZE);
    }
    
    void leaf_set_data(int64_t offset, int idx, const char* data) {
        int64_t pos = offset + 16 + idx * (4 + DATA_SIZE) + 4;
        memcpy(mapped_file + pos, data, DATA_SIZE);
    }
    
    void leaf_set_record(int64_t offset, int idx, int key, const char* data) {
        leaf_set_key(offset, idx, key);
        leaf_set_data(offset, idx, data);
    }
    
    void leaf_get_record(int64_t offset, int idx, int& key, char* data) {
        key = leaf_get_key(offset, idx);
        leaf_get_data(offset, idx, data);
    }
    
    // Internal node operations
    int internal_get_num_keys(int64_t offset) {
        int n;
        memcpy(&n, mapped_file + offset, sizeof(int));
        return n;
    }
    
    void internal_set_num_keys(int64_t offset, int n) {
        memcpy(mapped_file + offset, &n, sizeof(int));
    }
    
    int64_t internal_get_child(int64_t offset, int idx) {
        int64_t child;
        int64_t pos = offset + 8 + idx * 12;
        memcpy(&child, mapped_file + pos, sizeof(int64_t));
        return child;
    }
    
    void internal_set_child(int64_t offset, int idx, int64_t child) {
        int64_t pos = offset + 8 + idx * 12;
        memcpy(mapped_file + pos, &child, sizeof(int64_t));
    }
    
    int internal_get_key(int64_t offset, int idx) {
        int key;
        int64_t pos = offset + 8 + idx * 12 + 8;
        memcpy(&key, mapped_file + pos, sizeof(int));
        return key;
    }
    
    void internal_set_key(int64_t offset, int idx, int key) {
        int64_t pos = offset + 8 + idx * 12 + 8;
        memcpy(mapped_file + pos, &key, sizeof(int));
    }
    
    int64_t search_leaf(int key) {
        if (root_offset == -1) return -1;
        
        int64_t current = root_offset;
        
        while (!is_leaf(current)) {
            int num_keys = internal_get_num_keys(current);
            int i = 0;
            while (i < num_keys && key >= internal_get_key(current, i)) {
                i++;
            }
            current = internal_get_child(current, i);
            
            if (current < PAGE_SIZE || current >= (int64_t)file_size) {
                return -1;
            }
        }
        
        return current;
    }
    
    bool insert_into_leaf(int64_t leaf_offset, int key, const char* data, int& split_key, int64_t& new_leaf_offset) {
        int num_keys = leaf_get_num_keys(leaf_offset);
        

        for (int i = 0; i < num_keys; i++) {
            if (leaf_get_key(leaf_offset, i) == key) {
                leaf_set_data(leaf_offset, i, data);
                return false;
            }
        }
        
        if (num_keys < LEAF_ORDER) {

            int i = num_keys - 1;
            while (i >= 0 && leaf_get_key(leaf_offset, i) > key) {
                int temp_key;
                char temp_data[DATA_SIZE];
                leaf_get_record(leaf_offset, i, temp_key, temp_data);
                leaf_set_record(leaf_offset, i + 1, temp_key, temp_data);
                i--;
            }
            leaf_set_record(leaf_offset, i + 1, key, data);
            leaf_set_num_keys(leaf_offset, num_keys + 1);
            return false;
        } else {

            std::vector<std::pair<int, std::vector<char>>> records;
            for (int i = 0; i < num_keys; i++) {
                int k = leaf_get_key(leaf_offset, i);
                char d[DATA_SIZE];
                leaf_get_data(leaf_offset, i, d);
                records.push_back({k, std::vector<char>(d, d + DATA_SIZE)});
            }
            

            bool inserted = false;
            for (size_t i = 0; i < records.size(); i++) {
                if (key < records[i].first) {
                    records.insert(records.begin() + i, {key, std::vector<char>(data, data + DATA_SIZE)});
                    inserted = true;
                    break;
                }
            }
            if (!inserted) {
                records.push_back({key, std::vector<char>(data, data + DATA_SIZE)});
            }
            
            size_t mid = (records.size() + 1) / 2;
            

            leaf_set_num_keys(leaf_offset, mid);
            for (size_t i = 0; i < mid; i++) {
                leaf_set_record(leaf_offset, i, records[i].first, records[i].second.data());
            }
            

            new_leaf_offset = allocate_page();
            
            int is_leaf_flag = 1;
            memcpy(mapped_file + new_leaf_offset + 4, &is_leaf_flag, sizeof(int));
            
            leaf_set_num_keys(new_leaf_offset, records.size() - mid);
            
            for (size_t i = mid; i < records.size(); i++) {
                leaf_set_record(new_leaf_offset, i - mid, records[i].first, records[i].second.data());
            }
            

            int64_t old_next = leaf_get_next(leaf_offset);
            leaf_set_next(new_leaf_offset, old_next);
            leaf_set_next(leaf_offset, new_leaf_offset);
            
            split_key = leaf_get_key(new_leaf_offset, 0);
            return true;
        }
    }
    
    bool insert_into_internal(int64_t internal_offset, int key, int64_t child_offset, int& split_key, int64_t& new_internal_offset) {
        int num_keys = internal_get_num_keys(internal_offset);
        
        if (num_keys < INTERNAL_ORDER) {

            int i = num_keys - 1;
            while (i >= 0 && internal_get_key(internal_offset, i) > key) {
                internal_set_key(internal_offset, i + 1, internal_get_key(internal_offset, i));
                internal_set_child(internal_offset, i + 2, internal_get_child(internal_offset, i + 1));
                i--;
            }
            internal_set_key(internal_offset, i + 1, key);
            internal_set_child(internal_offset, i + 2, child_offset);
            internal_set_num_keys(internal_offset, num_keys + 1);
            return false;
        } else {

            std::vector<int> keys;
            std::vector<int64_t> children;
            
            for (int i = 0; i < num_keys; i++) {
                keys.push_back(internal_get_key(internal_offset, i));
            }
            for (int i = 0; i <= num_keys; i++) {
                children.push_back(internal_get_child(internal_offset, i));
            }
            

            size_t i = 0;
            while (i < keys.size() && keys[i] < key) {
                i++;
            }
            keys.insert(keys.begin() + i, key);
            children.insert(children.begin() + i + 1, child_offset);
            
            size_t mid = keys.size() / 2;
            split_key = keys[mid];
            

            internal_set_num_keys(internal_offset, mid);
            for (size_t j = 0; j < mid; j++) {
                internal_set_key(internal_offset, j, keys[j]);
                internal_set_child(internal_offset, j, children[j]);
            }
            internal_set_child(internal_offset, mid, children[mid]);
            

            new_internal_offset = allocate_page();
            
            int is_leaf_flag = 0;
            memcpy(mapped_file + new_internal_offset + 4, &is_leaf_flag, sizeof(int));
            
            internal_set_num_keys(new_internal_offset, keys.size() - mid - 1);
            
            for (size_t j = mid + 1; j < keys.size(); j++) {
                internal_set_key(new_internal_offset, j - mid - 1, keys[j]);
            }
            for (size_t j = mid + 1; j < children.size(); j++) {
                internal_set_child(new_internal_offset, j - mid - 1, children[j]);
            }
            
            return true;
        }
    }
    
    bool insert_recursive(int64_t offset, int key, const char* data, int& split_key, int64_t& new_offset) {
        if (is_leaf(offset)) {
            return insert_into_leaf(offset, key, data, split_key, new_offset);
        } else {
            int num_keys = internal_get_num_keys(offset);
            int i = 0;
            while (i < num_keys && key >= internal_get_key(offset, i)) {
                i++;
            }
            
            int child_split_key;
            int64_t child_new_offset;
            bool child_split = insert_recursive(internal_get_child(offset, i), key, data, child_split_key, child_new_offset);
            
            if (!child_split) {
                return false;
            }
            
            return insert_into_internal(offset, child_split_key, child_new_offset, split_key, new_offset);
        }
    }
    
public:
    BPlusTree() : fd(-1), mapped_file(nullptr), file_size(0), root_offset(-1), next_page_offset(PAGE_SIZE) {

        fd = open(INDEX_FILE, O_RDWR | O_CREAT, 0666);
        if (fd < 0) {
            perror("Failed to open file");
            exit(1);
        }
        
        struct stat sb;
        if (fstat(fd, &sb) != 0) {
            perror("Failed to stat file");
            exit(1);
        }
        file_size = sb.st_size;
        
        if (file_size == 0) {

            file_size = PAGE_SIZE;
            if (ftruncate(fd, file_size) != 0) {
                perror("Failed to initialize file");
                exit(1);
            }
        }
        

        mapped_file = (char*)mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (mapped_file == MAP_FAILED) {
            perror("mmap failed");
            exit(1);
        }
        
        if (sb.st_size >= PAGE_SIZE) {
            read_metadata();
        } else {
            write_metadata();
        }
    }
    
    ~BPlusTree() {
        if (mapped_file && mapped_file != MAP_FAILED) {
            write_metadata();
            msync(mapped_file, file_size, MS_SYNC);
            munmap(mapped_file, file_size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }
    
    bool writeData(int key, const char* data) {
        if (!mapped_file || mapped_file == MAP_FAILED) return false;
        
        if (root_offset == -1) {

            root_offset = allocate_page();
            
            int is_leaf_flag = 1;
            memcpy(mapped_file + root_offset + 4, &is_leaf_flag, sizeof(int));
            
            leaf_set_num_keys(root_offset, 1);
            leaf_set_next(root_offset, -1);
            leaf_set_record(root_offset, 0, key, data);
            write_metadata();
            return true;
        }
        
        int split_key;
        int64_t new_offset;
        bool split = insert_recursive(root_offset, key, data, split_key, new_offset);
        
        if (split) {

            int64_t new_root = allocate_page();
            
            int is_leaf_flag = 0;
            memcpy(mapped_file + new_root + 4, &is_leaf_flag, sizeof(int));
            
            internal_set_num_keys(new_root, 1);
            internal_set_key(new_root, 0, split_key);
            internal_set_child(new_root, 0, root_offset);
            internal_set_child(new_root, 1, new_offset);
            
            root_offset = new_root;
            write_metadata();
        }
        
        msync(mapped_file, file_size, MS_ASYNC);
        return true;
    }
    
    char* readData(int key) {
        if (!mapped_file || mapped_file == MAP_FAILED) return nullptr;
        
        int64_t leaf_offset = search_leaf(key);
        if (leaf_offset == -1) return nullptr;
        
        int num_keys = leaf_get_num_keys(leaf_offset);
        for (int i = 0; i < num_keys; i++) {
            if (leaf_get_key(leaf_offset, i) == key) {
                static char result[DATA_SIZE];
                leaf_get_data(leaf_offset, i, result);
                return result;
            }
        }
        
        return nullptr;
    }
    
    char** readRangeData(int lowerKey, int upperKey, int& n) {
        n = 0;
        if (!mapped_file || mapped_file == MAP_FAILED || root_offset == -1) return nullptr;
        
        std::vector<char*> results;
        
        int64_t leaf_offset = search_leaf(lowerKey);
        if (leaf_offset == -1) return nullptr;
        
        while (leaf_offset != -1 && leaf_offset >= PAGE_SIZE) {
            int num_keys = leaf_get_num_keys(leaf_offset);
            
            for (int i = 0; i < num_keys; i++) {
                int k = leaf_get_key(leaf_offset, i);
                if (k >= lowerKey && k <= upperKey) {
                    char* data = new char[DATA_SIZE];
                    leaf_get_data(leaf_offset, i, data);
                    results.push_back(data);
                } else if (k > upperKey) {
                    break;
                }
            }
            
            if (num_keys > 0 && leaf_get_key(leaf_offset, num_keys - 1) > upperKey) {
                break;
            }
            
            leaf_offset = leaf_get_next(leaf_offset);
        }
        
        n = results.size();
        if (n == 0) return nullptr;
        
        char** result_array = new char*[n];
        for (int i = 0; i < n; i++) {
            result_array[i] = results[i];
        }
        
        return result_array;
    }
    
    bool deleteData(int key) {
        if (!mapped_file || mapped_file == MAP_FAILED) return false;
        
        int64_t leaf_offset = search_leaf(key);
        if (leaf_offset == -1) return false;
        
        int num_keys = leaf_get_num_keys(leaf_offset);
        
        for (int i = 0; i < num_keys; i++) {
            if (leaf_get_key(leaf_offset, i) == key) {

                for (int j = i; j < num_keys - 1; j++) {
                    int temp_key;
                    char temp_data[DATA_SIZE];
                    leaf_get_record(leaf_offset, j + 1, temp_key, temp_data);
                    leaf_set_record(leaf_offset, j, temp_key, temp_data);
                }
                leaf_set_num_keys(leaf_offset, num_keys - 1);
                msync(mapped_file, file_size, MS_ASYNC);
                return true;
            }
        }
        
        return false;
    }
};

// Global tree instance
BPlusTree* tree = nullptr;

extern "C" {
    void initTree() {
        if (tree == nullptr) {
            tree = new BPlusTree();
        }
    }
    
    void closeTree() {
        if (tree != nullptr) {
            delete tree;
            tree = nullptr;
        }
    }
    
    int writeData(int key, const char* data) {
        if (tree == nullptr) initTree();
        return tree->writeData(key, data) ? 1 : 0;
    }
    
    char* readData(int key) {
        if (tree == nullptr) initTree();
        return tree->readData(key);
    }
    
    char** readRangeData(int lowerKey, int upperKey, int* n) {
        if (tree == nullptr) initTree();
        return tree->readRangeData(lowerKey, upperKey, *n);
    }
    
    int deleteData(int key) {
        if (tree == nullptr) initTree();
        return tree->deleteData(key) ? 1 : 0;
    }
}
