#include <iostream>

#define PAGE_SIZE 4096
#define DATA_SIZE 100

const int LEAF_ORDER = 35;       
const int INTERNAL_ORDER = 100;

int main() {
    std::cout << "=== Size Calculations ===" << std::endl;
    std::cout << "PAGE_SIZE: " << PAGE_SIZE << std::endl;
    std::cout << "DATA_SIZE: " << DATA_SIZE << std::endl;
    std::cout << std::endl;
    
    std::cout << "Leaf Node:" << std::endl;
    std::cout << "  Header (num_keys + is_leaf + next_leaf): " << (4 + 4 + 8) << " bytes" << std::endl;
    std::cout << "  Per record (key + data): " << (4 + DATA_SIZE) << " bytes" << std::endl;
    std::cout << "  LEAF_ORDER: " << LEAF_ORDER << std::endl;
    std::cout << "  Total size: " << (16 + LEAF_ORDER * (4 + DATA_SIZE)) << " bytes" << std::endl;
    std::cout << "  Fits in page? " << ((16 + LEAF_ORDER * (4 + DATA_SIZE)) <= PAGE_SIZE ? "YES" : "NO") << std::endl;
    std::cout << std::endl;
    
    std::cout << "Internal Node:" << std::endl;
    std::cout << "  Header (num_keys + is_leaf): " << (4 + 4) << " bytes" << std::endl;
    std::cout << "  Per entry (child + key): " << (8 + 4) << " bytes" << std::endl;
    std::cout << "  INTERNAL_ORDER: " << INTERNAL_ORDER << std::endl;
    std::cout << "  Total size: " << (8 + (INTERNAL_ORDER + 1) * 8 + INTERNAL_ORDER * 4) << " bytes" << std::endl;
    std::cout << "  Fits in page? " << ((8 + (INTERNAL_ORDER + 1) * 8 + INTERNAL_ORDER * 4) <= PAGE_SIZE ? "YES" : "NO") << std::endl;
    
    return 0;
}
