#include "Block.h"
#include "Block.cpp"
#include "utils.cpp"
#include "index.cpp"


/**
 * 条件检索算法
 * 条件：R.A = 40  或  S.C = 60
 */

const addr_t condQueryStart = 1000; //　检索结果的其实存放地址

/** 
 * 比较函数
 * 
 * return: 记录R相对val的位置
 */
int cmp(row_t R, int val) {
    if (R.A == val)
        return EQ;
    else if (R.A < val)
        return LT;
    else
        return GT;
}

/**
 * 相等条件函数
 * 
 * return: 记录中A字段的值是否与val相等
 */
bool EQ_cond(row_t R, int val) { return (R.A == val); }

/**
 * 表的线性检索
 * 对待检索的表按照检索条件cond和检索值val进行线性检索
 * 
 * table: 待检索的表信息
 * resTable: 检索结果表信息
 * val: 检索值
 * cond: 检索条件函数
 * 
 * return: 检索结果存放区域的最后一块的地址
 */
addr_t linearQuery(const table_t &table, table_t &resTable, int val, bool (*cond)(row_t, int)) {
    int numOfUsedBlock = numOfBufBlock - 1;
    int numOfRows = numOfRowInBlk * numOfUsedBlock;
    addr_t curAddr = 0, readAddr = table.start;

    block_t blk[numOfUsedBlock], resBlk;
    resBlk.writeInit(resTable.start);
    for (int i = 0; i < numOfUsedBlock; ++i)
        blk[i].loadFromDisk(readAddr++);
    row_t t[numOfRows];

    int readRows;
    while(1) {
        readRows = read_N_Rows_From_M_Block(blk, t, numOfRows, numOfUsedBlock);
        // printRows(t, readRows, val);
        bool isLastBlock = (readRows < numOfRows);
        for (int i = 0; i < readRows; ++i) {
            if (cond(t[i], val)) 
                curAddr = resBlk.writeRow(t[i]);
        }
        if (isLastBlock) {
            addr_t endAddr = resBlk.writeLastBlock();
            if (endAddr != END_OF_FILE)
                curAddr = endAddr;
            break;
        }
    }
    return curAddr;
}

/**
 * 二分检索
 * 对待检索的表按照匹配条件cmp和匹配值val进行线性检索
 * 
 * table: 待检索的表信息
 * resTable: 检索结果表信息
 * val: 匹配值
 * cmp: 检索条件比较函数，能够反映出目标记录与当前检索记录的位置信息
 * 
 * return: 检索结果存放区域的最后一块的地址
 */
addr_t binaryQuery(const table_t &table, table_t &resTable, int val, int (*cmp)(row_t, int)) {
    int numOfUsedBlock = numOfBufBlock - 1;     // 读入7块同时处理
    int numOfRows = numOfRowInBlk * numOfUsedBlock;
    addr_t curAddr = 0, readAddr = table.start;

    printf("----- 对每%d块进行二分检索 -----\n", numOfUsedBlock);
    row_t t[numOfRows], res[numOfRows];
    int pRes = 0;       // 记录符合检索条件的记录个数
    block_t blk[numOfBufBlock - 1], resBlk;
    resBlk.writeInit(resTable.start);
    for (int i = 0; i < numOfUsedBlock; ++i)
        blk[i].loadFromDisk(readAddr++);

    int readRows;
    while(1) {
        readRows = read_N_Rows_From_M_Block(blk, t, numOfRows, numOfUsedBlock);
        // printRows(t, readRows, val);
        insertSort<row_t>(t, readRows);
        // 开始二分查找
        int left = 0, right = readRows - 1;
        while (left <= right) {
            int mid = (right + left) / 2;
            if (cmp(t[mid], val) == EQ) {
                int forward = mid, back = mid;
                res[pRes++] = t[mid];
                // 由于这一段记录是有序的，因此查找到了一条符合条件的记录，其附近可能还有同样符合条件的其他记录
                // 检索该记录附近的所有记录，查找符合条件的记录
                while(cmp(t[--back], val) == EQ)
                    res[pRes++] = t[back];
                while(cmp(t[++forward], val) == EQ)
                    res[pRes++] = t[forward];
                break;
            } else if (cmp(t[mid], val) == LT) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        if (readRows < numOfRows)
            break;
    }
    if (pRes == 0) {
        printf("没有检索到符合条件的记录!\n");
        return 0;
    }
    insertSort<row_t>(res, pRes);
    for (int i = 0; i < pRes; ++i)
        curAddr = resBlk.writeRow(res[i]);
    addr_t endAddr = resBlk.writeLastBlock();
    if (endAddr != END_OF_FILE)
        curAddr = endAddr;
    return curAddr;
}

/**
 * 按索引检索
 * 对待检索的表，按照val对应的索引信息，加载对应的磁盘块后再进行线性检索
 * 若该表未做聚簇/建立索引的操作，会先对该表做聚簇/建立索引
 * 
 * table: 待检索的表信息
 * resTable: 检索结果表信息
 * val: 检索值
 * 
 * return: 检索结果存放区域的最后一块的地址
 */
addr_t indexQuery(const table_t &table, table_t &resTable, int val) {
    // 表未聚簇时，需要先聚簇
    addr_t clusterAddr, indexAddr, curAddr = 0;
    table_map_t::iterator findCluster = clusterTableMap.find(table.start);
    if (findCluster == clusterTableMap.end()) {
        printf("当前查找表未仍未聚簇，现进行聚簇操作...\n");
        if (clusterTableMap.size() == 0)
            clusterAddr = addrOfScan_2;
        else {
            table_map_t::iterator iter = clusterTableMap.end();
            --iter;
            clusterAddr = (iter->second).B + 1;
        }
        tableClustering(table, clusterAddr);
        printf("\n聚簇完成！\n");
        printf("聚簇所用IO: %d\n\n", buff.numIO);
    } else {
        index_t addrItem = indexTableMap.at(table.start);
        clusterAddr = addrItem.A;
    }

    // 表未建立索引时，需要先建立索引
    table_map_t::iterator findIndex = indexTableMap.find(table.start);
    if (findIndex == indexTableMap.end()) {
        printf("当前查找表未仍未建立索引，现建立索引...\n");
        addr_t indexStartAddr;
        if (indexTableMap.size() == 0)
            indexStartAddr = index_start;
        else {
            table_map_t::iterator iter = indexTableMap.end();
            --iter;
            indexStartAddr = (iter->second).B + 1;
        }
        unsigned long prior_IO = buff.numIO;
        addr_t indexEndAddr = buildIndex(clusterAddr, indexStartAddr);
        if (indexEndAddr != END_OF_FILE)
            printf("\n完成！索引写入磁盘块：%d-%d\n", indexStartAddr, indexEndAddr);
        indexAddr = indexStartAddr;
        printf("建立索引所用IO: %d\n\n", buff.numIO - prior_IO);
    } else {
        index_t addrItem = indexTableMap.at(table.start);
        indexAddr = addrItem.A;
    }

    // 加载索引，读取对应的聚簇存放地址
    unsigned long prior_IO = buff.numIO;
    loadIndex(indexAddr);
    if(buff.numIO)
        printf("加载索引所用IO: %d\n\n", buff.numIO - prior_IO);
    else
        printf("索引已被加载到内存中，无需加载！\n");
    vector<tree_data_t> data = BPTR.select(val, EQ);
    addr_t loadAddr = data[0];

    // 从索引指向的聚簇存放地址中查找结果
    block_t readBlk, resBlk;
    row_t R[numOfRowInBlk];
    resBlk.writeInit(resTable.start);
    readBlk.loadFromDisk(loadAddr);

    // 在索引指向的块中查询对应结果
    int readRows, cursor = 0;
    while(1) {
        readRows = read_N_Rows_From_1_Block(readBlk, R, numOfRowInBlk);
        resTable.size += readRows;
        cursor = 0;
        if (readRows > 0) {
            if (R[cursor].A > val)  {
                // 找到第一个大于索引字段值时结束检索
                break;
            }
            while(R[cursor].A < val)
                cursor += 1;
            while(R[cursor].A == val) {
                curAddr = resBlk.writeRow(R[cursor++]);
                if (cursor == readRows)
                    break;
            }
        } else {
            // 已经读到文件的末尾了
            break;
        }
    }
    addr_t endAddr = resBlk.writeLastBlock();
    if (endAddr != END_OF_FILE)
        curAddr = endAddr;
    return curAddr;
}

/**
 * 看函数名就知道这玩意儿是用来干什么的了
 * 
 * 由于这里所有的传参都只是为了传给里面的indexQuery
 * 所以参数内容就跟indexQuery完全相同，这里就不赘述了
 * 详细参数内容请参阅indexQuery的参数部分
 */
void searchByIndex_and_Show(const table_t &table, table_t &resTable, int val) {
    clear_Buff_IO_Count();
    addr_t resEndAddr = indexQuery(table, resTable, val);
    showResult(resTable.start);
    if (resEndAddr)
        printf("\n注：结果写入磁盘块：%d-%d\n", resTable.start, resEndAddr);
    printf("本次共发生%ld次I/O\n\n", buff.numIO);
}


/**************************** main ****************************/
int main() {
    bufferInit();
    addr_t resEndAddr;
    table_t Result_table_R = {condQueryStart, 0};
    printf("===================== 开始检索R表 ====================\n");
    /********* 线性检索与二分检索测试部分 *********/
    clear_Buff_IO_Count();
    // resEndAddr = linearQuery(table_R, Result_table_R, 40, EQ_cond);
    // resEndAddr = binaryQuery(table_R, Result_table_R, 40, cmp);
    // showResult(Result_table_R.start);
    // printf("\n注：结果写入磁盘块：%d-%d\n", Result_table_R.start, resEndAddr);
    // printf("本次共发生%ld次I/O\n\n", buff.numIO);

    /********* 索引检索测试部分 *********/
    printf("----- 未建立索引时进行检索 -----\n");
    searchByIndex_and_Show(table_R, Result_table_R, 40);
    printf("----- 已建立索引时进行检索 -----\n");
    searchByIndex_and_Show(table_R, Result_table_R, 40);

    printf("===================== 开始检索S表 ====================\n");

    /********* 线性检索与二分检索测试部分 *********/
    clear_Buff_IO_Count();
    // addr_t newStartAddr = resEndAddr + 1;
    // table_t Result_table_S = {newStartAddr, 0};
    // resEndAddr = linearQuery(table_S, Result_table_S, 60, EQ_cond);
    // resEndAddr = binaryQuery(table_S, Result_table_S, 60, cmp);
    // showResult(Result_table_S.start);
    // printf("\n注：结果写入磁盘块：%d-%d\n", Result_table_S.start, resEndAddr);
    // printf("本次共发生%ld次I/O\n\n", buff.numIO);

    /********* 索引检索测试部分 *********/
    addr_t newStartAddr = 1001;
    table_t Result_table_S = {newStartAddr, 0};
    printf("----- 未建立索引时进行检索 -----\n");
    searchByIndex_and_Show(table_S, Result_table_S, 60);
    printf("----- 已建立索引时进行检索 -----\n");
    searchByIndex_and_Show(table_S, Result_table_S, 60);
    printf("=================== 看看R是否有索引 ==================\n");
    searchByIndex_and_Show(table_R, Result_table_R, 40);

    system("pause");
    return OK;
}