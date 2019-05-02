#include "utils.cpp"
#include "index.cpp"

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
 * @param table1 第一个表的相关信息
 * @param table2 第二个表的相关信息
 * @return table_t 连接结果存放区域的信息表
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
    return resTable;
}

// -----------------------------------------------------------
//                      Sort Merge Join                       
// -----------------------------------------------------------
/**
 * @brief 三趟扫描
 * 从磁盘中读取完全有序的两表到缓冲区上
 * 检查连接条件，并将可连接的记录写入结果存储块中
 * 在这里选用了R表读1块，S表读2块的缓冲区读写策略
 * 
 * @param R_addr 有序R表的存储起始地址
 * @param S_addr 有序S表的存储起始地址
 * @param resTable 连接结果的存储信息表
 */
void scan_3_SortedJoin(addr_t R_addr, addr_t S_addr, table_t &resTable) {
    int maxNumOfRows_S = 2 * numOfRowInBlk; // 采用一次从磁盘上读取2块S表的策略
    block_t Rblk, Sblk, resBlk;
    Row t_R, t_S[maxNumOfRows_S];
    resBlk.writeInit(resTable.start, numOfRowInBlk - 1);
    Rblk.loadFromDisk(R_addr);
    Sblk.loadFromDisk(S_addr);

    int numOfRows_S = maxNumOfRows_S;
    int pt_S = numOfRows_S;     // 记录S表的最后一个可连接记录的检索位置
    int matchMark = 0;          // 记录上一批连接操作的第一次匹配位置，以便剪枝
    addr_t curAddr = resTable.start;
    bool prior_match = false;
    bool R_over = false, S_over = false;
    while(1) {
        // 循环检索连接条件
        t_R = Rblk.getNewRow();
        R_over = (t_R.isFilled == false);
        if (R_over || S_over) {
            // 结束连接循环检索的条件：
            // 1. R表读完了  或  2. S表读完了
            if (!R_over)
                Rblk.freeBlock();
            else if (!S_over)
                Sblk.freeBlock();
            else
                ;
            addr_t endAddr = resBlk.writeLastBlock();
            if (endAddr != END_OF_FILE)
                curAddr = endAddr;
            resTable.end = curAddr;
            break;
        }
        if (pt_S == numOfRows_S) {
            // 检索完所有加载到缓冲区的的S表数据，需要加载下一批数据
            pt_S = 0;       // 两个标记的复位
            matchMark = 0;
            numOfRows_S = read_N_Rows_From_1_Block(Sblk, t_S, maxNumOfRows_S);
            S_over = (numOfRows_S < maxNumOfRows_S);
        }
        for (int i = matchMark; i < numOfRows_S; ++i) { 
            // R表记录与S表读取的每一条记录进行比对&连接
            if (t_R > t_S[i]) {
                prior_match = false;
                if (i == numOfRows_S - 1) {
                    // 目前为止没有能够连接上的，需要更新标记，以加载下一批数据
                    pt_S = numOfRows_S;
                }
                continue;
            } else if (t_R.join_A(t_S[i])) {
                matchMark = prior_match ? matchMark : i;
                prior_match = true;
                curAddr = resBlk.writeRow(t_R);
                curAddr = resBlk.writeRow(t_S[i]);
                resTable.size += 1;
                pt_S = i + 1;   // 更新最后一次连接的位置标记
            } else {
                prior_match = false;
                break;
            }
        }
    }
}


/**
 * @brief 采用多路归并排序的方法进行表的连接
 * 
 * @return table_t 连接结果存放区域的最后一块的地址
 */
table_t SORT_MERGE_JOIN() {
    int sizeOfSubTable = numOfRowInBlk * numOfBufBlock;           // 由缓冲区划分出的子表大小
    int numOfSubTables_R = ceil(1.0 * R_size / sizeOfSubTable);   // R划分出的子表数量
    int numOfSubTables_S = ceil(1.0 * S_size / sizeOfSubTable);   // S划分出的子表数量
    /******************* 一趟扫描 *******************/
    printf("============ 一趟扫描开始 ============\n");
    // 对表R进行分组排序
    addr_t scan_1_next = addrOfScan_1;     // 第一趟扫描结果存储的起始地址
    addr_t scan_1_Index_R, scan_1_Index_S; // R、S表的第一趟扫描结果存储的起始位置索引
    if ((scan_1_Index_R = scan_1_next) == ADDR_NOT_EXISTS)
        return FAIL;
    scan_1_next = scan_1_PartialSort(numOfSubTables_R, scan_1_next, R_start, sizeOfSubTable);
    // 对表S进行分组排序
    if ((scan_1_Index_S = scan_1_next) == ADDR_NOT_EXISTS)
        return FAIL;
    scan_1_next = scan_1_PartialSort(numOfSubTables_S, scan_1_next, S_start, sizeOfSubTable);

    /******************* 两趟扫描 *******************/
    printf("============ 两趟扫描开始 ============\n");
    // 对表R进行归并排序
    addr_t scan_2_next = addrOfScan_2;     // 第二趟扫描结果存储的起始地址
    addr_t scan_2_Index_R, scan_2_Index_S; // R、S表的第二趟扫描结果存储的起始位置索引
    if ((scan_2_Index_R = scan_2_next) == ADDR_NOT_EXISTS)
        return FAIL;
    scan_2_next = scan_2_SortMerge(numOfSubTables_R, scan_1_Index_R, scan_2_Index_R);
    // 对表S进行归并排序
    if ((scan_2_Index_S = scan_2_next) == ADDR_NOT_EXISTS)
        return FAIL;
    scan_2_next = scan_2_SortMerge(numOfSubTables_S, scan_1_Index_S, scan_2_Index_S);

    /******************* 三趟扫描 *******************/
    printf("============ 三趟扫描开始 ============\n");
    table_t resTable(joinResultStart);
    resTable.rowSize = numOfRowInBlk - 1;
    scan_3_SortedJoin(scan_2_Index_R, scan_2_Index_S, resTable);
    return resTable;
}

// -----------------------------------------------------------
//                          Hash Join                         
// -----------------------------------------------------------

/**
 * @brief 两趟扫描的中间步骤——按桶连接
 * 可连接的记录一定具有相同的散列值，因而只需遍历一个桶中的所有记录即可
 * 
 * @param numOfBuckets 散列的桶的数量
 * @param scan_1_index_R 
 * @param scan_1_index_S 
 * @return addr_t 连接结果存放区域的最后一块的地址
 */
addr_t scan_2_HashJoin(int numOfBuckets, addr_t scan_1_index_R[], addr_t scan_1_index_S[]) {
// addr_t scan_2_HashJoin(int numOfBuckets, addr_t readIndex, addr_t scan_1_index[], addr_t scan_2_index) {
    // addr_t curAddr = 0;
    // int numOfRows = numOfRowInBlk * (numOfBuckets / 2);
    // row_t R_data[numOfRows], S_data[numOfRows];
    // block_t Sblk, Rblk, resBlk;
    // resBlk.writeInit(scan_2_index, numOfRowInBlk - 1);

    // Rblk.loadFromDisk(readIndex);
    // int readRows_R, readRows_S;
    // while(1) {
    //     readRows_R = read_N_Rows_From_1_Block(Rblk, R_data, numOfRows);
    //     for (int i = 0; i < readRows_R; ++i) {
    //         int R_hash = hashRowsByA(R_data[i], numOfBuckets);
    //         Sblk.loadFromDisk(scan_1_index[R_hash]);
    //         while(1) {
    //             readRows_S = read_N_Rows_From_1_Block(Sblk, S_data, numOfRows);
    //             for (int j = 0; j < readRows_S; ++j) {
    //                 if (R_data[i].join_A(S_data[j])) {
    //                     curAddr = resBlk.writeRow(R_data[i]);
    //                     curAddr = resBlk.writeRow(S_data[j]);
    //                 }
    //             }
    //             if(readRows_S < numOfRows) {
    //                 Sblk.freeBlock();
    //                 break;
    //             }
    //         }
    //     }
    //     if (readRows_R < numOfRows) {
    //         Rblk.freeBlock();
    //         addr_t endAddr = resBlk.writeLastBlock();
    //         if (endAddr != END_OF_FILE)
    //             curAddr = endAddr;
    //         break;
    //     }
    // }
    // return curAddr;
    
    addr_t curAddr = 0;
    int numOfRows = numOfRowInBlk * (numOfBuckets / 2);
    row_t R_data[numOfRows], S_data[numOfRows];
    block_t Rblk, Sblk, resBlk;
    resBlk.writeInit(joinResultStart, numOfRowInBlk - 1);

    for (int k = 0; k < numOfBuckets; ++k) {
        Rblk.loadFromDisk(scan_1_index_R[k]);
        Sblk.loadFromDisk(scan_1_index_S[k]);
        int readRows_R, readRows_S;
        bool R_over, S_over;
        while(1) {
            readRows_R = read_N_Rows_From_1_Block(Rblk, R_data, numOfRows);
            insertSort<row_t>(R_data, readRows_R);
            R_over = (readRows_R < numOfRows); // R表读入记录数量小于numOfRows表明R表已读完
            readRows_S = read_N_Rows_From_1_Block(Sblk, S_data, numOfRows);
            insertSort<row_t>(S_data, readRows_S);
            S_over = (readRows_S < numOfRows); // S表读入记录数量小于numOfRows表明S表已读完
            for (int i = 0; i < readRows_R; ++i) {
                for (int j = 0; j < readRows_S; ++j) {
                    if (R_data[i].join_A(S_data[j])) {
                        curAddr = resBlk.writeRow(R_data[i]);
                        curAddr = resBlk.writeRow(S_data[j]);
                    }
                }
                if (S_over)
                    break;
            }
            if (R_over)
                break;
        }
        Rblk.freeBlock();
        Sblk.freeBlock();
        if (k == numOfBuckets - 1) {
            addr_t endAddr = resBlk.writeLastBlock();
            if (endAddr != END_OF_FILE)
                curAddr = endAddr;
        }
    }
    return curAddr;
}


/**
 * @brief 采用散列的方法进行表的连接
 * 
 * @return addr_t 连接结果存放区域的最后一块的地址
 */
addr_t HASH_JOIN() {
    /******************* 一趟扫描 *******************/
    printf("============ 一趟扫描开始 ============\n");
    int numOfBuckets = numOfBufBlock - 1;   // 散列桶的数量一般取缓冲区块数-1
    addr_t scan_1_Index_R[7] = {5300, 5400, 5500, 5600, 5700, 5800, 5900};
    addr_t scan_1_Index_S[7] = {6300, 6400, 6500, 6600, 6700, 6800, 6900};
    printf("---- 对表R进行分组散列 ----\n");
    scan_1_HashToBucket(numOfBuckets, R_start, scan_1_Index_R);
    printf("---- 对表S进行分组散列 ----\n");
    scan_1_HashToBucket(numOfBuckets, S_start, scan_1_Index_S);

    /******************* 二趟扫描 *******************/
    printf("============ 二趟扫描开始 ============\n");
    // addr_t curAddr = scan_2_HashJoin(numOfBuckets, R_start, scan_1_Index_S, joinResultStart);
    addr_t curAddr = scan_2_HashJoin(numOfBuckets, scan_1_Index_R, scan_1_Index_S);

    return curAddr;
}


/**************************** main ****************************/
int main() {
    bufferInit();
    // table_t res = NEST_LOOP_JOIN(table_R, table_S);
    table_t res = SORT_MERGE_JOIN();
    // table_t res = HASH_JOIN();
    if (res.size == 0) {
        system("pause");
        return FAIL;
    }
    showResult(joinResultStart, 2 * sizeOfRow);
    int numOfUsedBlocks = ceil(1.0 * res.size / (numOfRowInBlk / 2));
    printf("\n注：结果写入磁盘块序号：3000-%d\n", res.start + numOfUsedBlocks - 1);
    printf("\n本次共发生%ld次I/O\n", buff.numIO);
    system("pause");
    return OK;
}