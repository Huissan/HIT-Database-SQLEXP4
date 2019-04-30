#include <iostream>
#include "BplusTree.h"

BPlusTree Tr;

bool INSERT(key_t a, tree_data_t b) {
    if (Tr.insert(a, b)) {
        printf("insert(%.0f, %d) -- OK\n", a, b);
        return true;
    } else {
        printf("insert(%.0f, %d) -- Error\n", a, b);
        return false;
    }
}

bool UPDATE(key_t a, key_t b) {
    if (Tr.update(a, b)) {
        printf("update(%.0f, %.0f) -- OK\n", a, b);
        return true;
    } else {
        printf("update(%.0f, %.0f) -- Error\n", a, b);
        return false;
    }
}

bool REMOVE(key_t a) {
    addr_t temp;
    if (Tr.remove(a, temp)) {
        printf("remove(%.1f) -- OK\n", a);
        return true;
    } else {
        printf("remove(%f) -- Error\n", a);
        return false;
    }
}

int main() {
    INSERT(3, 3);
    INSERT(1, 1);
    Tr.printData();
    INSERT(5, 5);
    Tr.printData();
    INSERT(3, 3);
    cout << "select 5: ";
    vector<tree_data_t> data = Tr.select(5, EQ);
    for (auto iter = data.begin(); iter != data.end(); ++iter) {
        tree_data_t d = *iter;
        cout << d << " ";
    }
    cout << endl;
    UPDATE(3, 6);
    Tr.printData();
    REMOVE(3);
    REMOVE(6);
    Tr.printData();

    system("pause");
    return 0;
}