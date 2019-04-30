#include "index.cpp"

int main() {
    bufferInit();
    table_t R = {R_start, R_size};
    addr_t clusterStartAddr = addrOfScan_2;
    tableClustering(R, clusterStartAddr);
    printf("开始建立索引\n");
    buildIndex(clusterStartAddr, index_start);
    printf("开始加载索引\n");
    loadIndex(index_start);
    printf("\n查找等于40的索引：");
    vector<tree_data_t> a = BPTR.select(40, EQ);
    for (auto iter = a.begin(); iter != a.end(); ++iter) {
        tree_data_t d = *iter;
        cout << d << " ";
    }
    cout << endl;
    system("pause");
    return 0;
}