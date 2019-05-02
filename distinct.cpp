#include "index.cpp"


const addr_t distinctTableStart = 500;


/**
 * @brief 去重操作
 * 
 * @param table 待去重的表
 * @param resStartAddr 去重结果存放的起始位置，默认值为distinctTableStart
 */
void tableDistinct(table_t table, addr_t resStartAddr = distinctTableStart) {
    int numOfUsedBlock = numOfBufBlock - 1;
    int numOfRows = numOfRowInBlk * numOfUsedBlock;
    addr_t resAddr = resStartAddr;

    // 想去重，先聚簇
    useCluster(table);
    addr_t readAddr = clusterTableMap.at(table.start).A;
    block_t readBlk[numOfUsedBlock], resBlk;
    resBlk.writeInit(resAddr);
    addr_t nextStart = readAddr;
    for (int i = 0; i < numOfUsedBlock; ++i) {
        readBlk[i].loadFromDisk(nextStart);
        nextStart = readBlk[i].readNextAddr();
        if (nextStart == END_OF_FILE) {
            numOfUsedBlock = i + 1;
            break;
        }
    }
    row_t t[numOfRows], prior;

    int readRows;
    while(1) {
        readRows = read_N_Rows_From_M_Block(readBlk, t, numOfRows, numOfUsedBlock);
        insertSort<row_t>(t, readRows);
        // printRows(t, readRows, val);
        for (int i = 0; i < readRows; ++i) {
            if (t[i] == prior) {
                // 聚簇后，与上一个出现过的记录相同则重复
                continue;
            }
            resBlk.writeRow(t[i]);
            prior = t[i];
        }
        if (readRows < numOfUsedBlock) {
            resBlk.writeLastBlock();
            break;
        }
    }
}

// int main() {
//     bufferInit();
//     tableDistinct(table_S);
//     showResult(distinctTableStart);
//     system("pause");
//     return 0;
// }