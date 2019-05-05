#include "utils.cpp"
#include "index.cpp"
#pragma once

/**
 * 连接算法
 * 条件：R.A 连接 S.C
*/

const addr_t joinResultStart = 3000;    // 连接结果的起始存放地址

// -----------------------------------------------------------
//                       Nest Loop Join                       
// -----------------------------------------------------------

/**
 * @brief 采用嵌套循环的方法进行表的连接
 * 
 * @param table1 待连接的第一个表的相关信息
 * @param table2 待连接的第二个表的相关信息
 * @return table_t 连接结果的存储信息表
 */
table_t NEST_LOOP_JOIN(table_t table1, table_t table2) {
    table_t resTable(joinResultStart), bigTable, smallTable;
    resTable.rowSize = 2 * sizeOfRow;
    // 决定大小表
    if (table1.size > table2.size) {
        bigTable = table1;
        smallTable = table2;
    } else {
        bigTable = table2;
        smallTable = table1;
    }
    addr_t bigTableAddr = bigTable.start, smallTableAddr = smallTable.start;
    int numOfSeriesBlock = 6;     // 大表使用的缓冲区块数
    int numOfRows_1 = numOfRowInBlk * numOfSeriesBlock; // 大表一次读入的记录数量
    int numOfRows_2 = numOfRowInBlk;                    // 小表一次读入的记录数量
    int numOfRows_res = numOfRowInBlk - 1;              // 结果表中的标准记录数量
    addr_t curAddr = 0;
    
    // 初始化缓冲区
    block_t seriesBlk[numOfSeriesBlock], singleBlk, resBlk;
    resBlk.writeInit(joinResultStart, numOfRows_res);
    for (int i = 0; i < numOfSeriesBlock; ++i) {
        seriesBlk[i].loadFromDisk(bigTableAddr);
        bigTableAddr = seriesBlk[i].readNextAddr();
        if (bigTableAddr == END_OF_FILE) {
            numOfSeriesBlock = i + 1;
            numOfRows_1 = numOfRowInBlk * numOfSeriesBlock;
            break;
        }
    }

    row_t tSeries[numOfRows_1], tSingle[numOfRows_2];
    int readRows_1, readRows_2;
    while(1) {
        // 小表循环嵌入大表循环，可以减少IO次数
        // 外层循环：大表一次从磁盘上读取numOfSeriesBlock块的内容
        readRows_1 = read_N_Rows_From_M_Block(seriesBlk, tSeries, numOfRows_1, numOfSeriesBlock);
        singleBlk.loadFromDisk(smallTableAddr);
        while(1) {
            // 内层循环：小表一次从磁盘上读取1块的内容
            readRows_2 = read_N_Rows_From_1_Block(singleBlk, tSingle, numOfRows_2);
            for (int i = 0; i < readRows_1; ++i) {
                for (int j = 0; j < readRows_2; ++j) {
                    if (tSeries[i].join_A(tSingle[j])) {
                        curAddr = resBlk.writeRow(tSeries[i]);
                        curAddr = resBlk.writeRow(tSingle[j]);
                        resTable.size += 1;
                    }
                }
            }
            if (readRows_2 < numOfRows_2) {
                // S读完了
                break;
            }
        }
        if (readRows_1 < numOfRows_1) {
            // R读完了
            addr_t endAddr = resBlk.writeLastBlock();
            if (endAddr != END_OF_FILE)
                curAddr = endAddr;
            resTable.end = curAddr;
            break;
        }
    }
    // 处理空结果表
    if (resTable.size == 0)
        resTable.start = resTable.end = 0;
    return resTable;
}

// -----------------------------------------------------------
//                      Sort Merge Join                       
// -----------------------------------------------------------

/**
 * @brief 采用多路归并排序的方法进行表的连接
 * 
 * @param table1 待连接的第一个表的相关信息
 * @param table2 待连接的第二个表的相关信息
 * @return table_t 连接结果的存储信息表
 */
table_t SORT_MERGE_JOIN(table_t table1, table_t table2) {
    table_t bigTable, smallTable, resTable(joinResultStart);
    resTable.rowSize = 2 * sizeOfRow;
    // 决定大小表
    if (table1.size > table2.size) {
        bigTable = table1;
        smallTable = table2;
    } else {
        bigTable = table2;
        smallTable = table1;
    }
    // 先对两表都做一遍聚簇
    useCluster(bigTable);
    addr_t smallTableAddr = useCluster(smallTable);
    // 对大表做索引
    addr_t indexAddr = useIndex(bigTable);
    loadIndex(indexAddr);

    block_t blk1, blk2, resBlk;
    resBlk.writeInit(resTable.start, numOfRowInBlk - 1);
    blk1.loadFromDisk(smallTableAddr);

    addr_t curAddr = 0, loadAddr;
    row_t t1[numOfRowInBlk], t2[numOfRowInBlk], t2_copy[numOfRowInBlk];
    int readRows_1, readRows_2, readRows_2_copy;
    int prior_1 = MAX_ATTR_VAL;
    bool earlyDie = false;
    cursor_t cursor_2;
    while(1) {
        readRows_1 = read_N_Rows_From_1_Block(blk1, t1, numOfRowInBlk);
        // printRows(t1, readRows_1);
        for (int k = 0; k < readRows_1; ++k) {
            bool isSametoPrior = (t1[k].A == prior_1);
            if (!isSametoPrior) {
                // 与上一条记录的A值不同时才加载，避免重复加载带来的IO开销
                if (t1[k].A == 21)
                    printf("");
                vector<addr_t> addrList = BPTR.select(t1[k].A, EQ);
                if (addrList.empty()) {
                    // 没有匹配的值，直接跳过该条记录的后续匹配工作
                    continue;
                }
                loadAddr = addrList[0];
                blk2.loadFromDisk(loadAddr);
                earlyDie = false;   // 加载了不同的索引必然没有早死现象
            }
            prior_1 = t1[k].A;
            if (earlyDie) {
                // 在匹配过程中出现了早死的情况
                // 需要重新从该记录的第一条索引指向的磁盘块开始加载
                blk2.loadFromDisk(loadAddr);
            }
            int loadBlocks = 0;
            while(1) {
                // 在索引加载块中检索连接值
                readRows_2 = read_N_Rows_From_1_Block(blk2, t2, numOfRowInBlk);
                loadBlocks += 1;
                bool joinFinish = (readRows_2 < numOfRowInBlk);
                cursor_2 = 0;
                if (loadBlocks > 1 && t2[0].join_A(t1[k])) {
                    // 早死现象捕获：当加载次数超过1次时，若新加载块的第一条记录驱动表当前的记录(t1[k])可以连接
                    // 说明上一块不完全包含所有可以连接的记录，即早死
                    // 这也说明了早死的检测时序在加载之后，这点请务必注意！
                    earlyDie = true;
                }
                if (earlyDie || (isSametoPrior == false && loadBlocks == 1)) {
                    readRows_2_copy = readRows_2;
                    for (int i = 0; i < readRows_2_copy; ++i)
                        t2_copy[i] = t2[i];
                    // 这里排除了一种不需要加载下一块（不改变上一次的读取副本t2_copy）的特殊情况：
                    // 小表(驱动表)在一块内有多条相同A值的记录，同时大表(被驱动表，索引表)中没有早死现象
                    // printRows(t2_copy, readRows_2_copy);
                }
                row_t *cmpRow = (loadBlocks == 1) ? t2_copy : t2;
                cursor_t cmpCursor = (loadBlocks == 1) ? readRows_2_copy : readRows_2;
                // 移动索引块中的指针到第一个匹配上连接值的位置
                while(cmpRow[cursor_2].A != t1[k].A && cursor_2 < cmpCursor)
                    cursor_2 += 1;
                if (cursor_2 == cmpCursor) {
                    if (joinFinish == false)
                        blk2.freeBlock();
                    // 边界条件，这一块中没有符合连接条件的记录
                    cursor_2 = readRows_2_copy; // 跳过连接操作
                    joinFinish = true;
                }
                for (; cursor_2 < readRows_2_copy; ++cursor_2) {
                    if (!t2_copy[cursor_2].join_A(t1[k])) {
                        if (joinFinish == false)
                            blk2.freeBlock();
                        joinFinish = true;
                        break;
                    }
                    curAddr = resBlk.writeRow(t1[k]);
                    curAddr = resBlk.writeRow(t2_copy[cursor_2]);
                    resTable.size += 1;
                }
                if (joinFinish)
                    break;
            }
        }
        if (readRows_1 < numOfRowInBlk) {
            addr_t endAddr = resBlk.writeLastBlock();
            if (endAddr != END_OF_FILE)
                curAddr = endAddr;
            resTable.end = curAddr;
            break;
        }
    }
    // 处理空结果表
    if (resTable.size == 0)
        resTable.start = resTable.end = 0;
    return resTable;
}

// -----------------------------------------------------------
//                          Hash Join                         
// -----------------------------------------------------------

/**
 * @brief 两趟扫描的中间步骤——按桶连接
 * 可连接的记录一定具有相同的散列值，因而只需遍历两表对应编号相同的桶中的所有记录即可
 * 
 * @param numOfBuckets 散列的桶的数量
 * @param scan_1_index_R 第一个表每个散列桶的起始地址
 * @param scan_1_index_S 第二个表每个散列桶的起始地址
 * @param resTable 连接结果的存储信息表
 */
void scan_2_HashJoin(int numOfBuckets, addr_t scan_1_index_R[], addr_t scan_1_index_S[], table_t &resTable) {
    addr_t curAddr = 0;
    int numOfRows = numOfRowInBlk * (numOfBuckets / 2);
    row_t R_data[numOfRows], S_data[numOfRows];
    block_t blk1, blk2, resBlk;
    resBlk.writeInit(resTable.start, numOfRowInBlk - 1);

    for (int k = 0; k < numOfBuckets; ++k) {
        blk1.loadFromDisk(scan_1_index_R[k]);
        int readRows_R, readRows_S;
        while(1) {
            readRows_R = read_N_Rows_From_1_Block(blk1, R_data, numOfRows);
            insertSort<row_t>(R_data, readRows_R);
            blk2.loadFromDisk(scan_1_index_S[k]);
            // printRows(R_data, readRows_R);
            while(1) {
                readRows_S = read_N_Rows_From_1_Block(blk2, S_data, numOfRows);
                insertSort<row_t>(S_data, readRows_S);
                // printRows(S_data, readRows_S);
                for (int i = 0; i < readRows_R; ++i) {
                    for (int j = 0; j < readRows_S; ++j) {
                        if (R_data[i].join_A(S_data[j])) {
                            curAddr = resBlk.writeRow(R_data[i]);
                            curAddr = resBlk.writeRow(S_data[j]);
                            resTable.size += 1;
                        }
                        if (R_data[i] < S_data[j])
                            break;
                    }
                }
                if (readRows_S < numOfRows) {
                    break;
                }
            }
            blk2.freeBlock();
            if (readRows_R < numOfRows)
                break;
        }
        blk1.freeBlock();
        if (k == numOfBuckets - 1) {
            addr_t endAddr = resBlk.writeLastBlock();
            if (endAddr != END_OF_FILE)
                curAddr = endAddr;
            resTable.end = curAddr;
        }
    }
    // 处理空结果表
    if (resTable.size == 0)
        resTable.start = resTable.end = 0;
}


/**
 * @brief 采用散列的方法进行表的连接
 * 
 * @param table1 待连接的第一个表的相关信息
 * @param table2 待连接的第二个表的相关信息
 * @return table_t 连接结果的存储信息表
 */
table_t HASH_JOIN(table_t table1, table_t table2) {
    /******************* 一趟扫描 *******************/
    int numOfBuckets = 6;   // 散列桶的数量，在试验中证明取当前值时IO数量最小
    addr_t scan_1_Index_R[7] = {5300, 5400, 5500, 5600, 5700, 5800, 5900};
    addr_t scan_1_Index_S[7] = {6300, 6400, 6500, 6600, 6700, 6800, 6900};
    // 对表R进行分组散列
    scan_1_HashToBucket(numOfBuckets, table1.start, scan_1_Index_R);
    // 对表S进行分组散列
    scan_1_HashToBucket(numOfBuckets, table2.start, scan_1_Index_S);

    /******************* 二趟扫描 *******************/
    table_t resTable(joinResultStart);
    resTable.rowSize = 2 * sizeOfRow;
    scan_2_HashJoin(numOfBuckets, scan_1_Index_R, scan_1_Index_S, resTable);
    for (int i = 0; i < numOfBuckets; ++i) {
        DropFiles(scan_1_Index_R[i]);
        DropFiles(scan_1_Index_S[i]);
    }
    // 处理空结果表
    if (resTable.size == 0)
        resTable.start = resTable.end = 0;
    return resTable;
}


/**************************** main ****************************/
// int main() {
//     bufferInit();
//     // table_t res = NEST_LOOP_JOIN(table_R, table_S);
//     table_t res = SORT_MERGE_JOIN(table_R, table_S);
//     // table_t res = HASH_JOIN();
//     if (res.size == 0) {
//         system("pause");
//         return FAIL;
//     }
//     showResult(joinResultStart, 2 * sizeOfRow);
//     int numOfUsedBlocks = ceil(1.0 * res.size / (numOfRowInBlk / 2));
//     printf("\n注：结果写入磁盘块序号：3000-%d\n", res.start + numOfUsedBlocks - 1);
//     printf("\n本次共发生%ld次I/O\n", buff.numIO);
//     system("pause");
//     return OK;
// }