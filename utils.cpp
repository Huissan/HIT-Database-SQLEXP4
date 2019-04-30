#include "Block.h"
#include "Block.cpp"
#include <cmath>
#pragma once

const int     R_start = 1;			// 磁盘中R地址从1开始
const int     S_start = 20;		    // 磁盘中S地址从20开始
const int     R_size = 112;         // R关系的记录总数
const int     S_size = 224;         // S关系的记录总数
const table_t table_R = {R_start, R_size};
const table_t table_S = {S_start, S_size};

/** 
 * 按记录的A字段值进行散列
 * 
 * R: 待散列的记录
 * numOfBuckets: 散列桶的数量
 * 
 * return: 该记录的散列结果（所归属桶的编号）
 */
int hashRowsByA(row_t R, int numOfBuckets) { return R.A % numOfBuckets; }

/**
 * argmin的实现
 * 
 * t[]: 待argmin的数组
 * size: 数组的大小
 * 
 * return: 数组t中最小值的下标
 */
template <typename T>
int argmin(T t[], int size) {
    int min = 0;
    for (int i = 0; i < size; ++i) {
        if (t[i] < t[min])
            min = i;
    }
    return min;
}

/**
 * 插入排序
 * 
 * array[]: 待排序的数组
 * size: 待排序数组的大小
 */
template <typename T>
void insertSort(T array[], int size) {
    T tmp;
    int i, j;
    for (i = 1; i < size; ++i) {
        tmp = array[i];
        for (j = i; j > 0 && array[j - 1] > tmp; --j)
            array[j] = array[j - 1];
        array[j] = tmp;
    }
}

/**
 * 对于一组记录，输出每一条记录的内容
 * 
 * t[]: 需要输出的记录组
 * size: 记录的数量
 * matchVal: 匹配值，通过该字段在输出中显示匹配的记录
 */
void printRows(row_t t[], int size, int matchVal = -1) {
    for (int i = 0; i < size; ++i) {
        printf("t[%d]: (%d, %d)\t", i, t[i].A, t[i].B);
        if (t[i].A == matchVal)
            printf("<-Matched here");
        printf("\n");
    }
    printf("\n");
}



/**
 * 输出结果磁盘块中存放的内容
 * 
 * resStartAddr: 结果磁盘块的起始地址
 * newSizeOfRow: 记录的长度，用于控制换行符的位置
 */
void showResult(addr_t resStartAddr, int newSizeOfRow = sizeOfRow) {
    float timesToStandartRow = 1.0 * newSizeOfRow / sizeOfRow;
    int numOfRows = floor(numOfRowInBlk / timesToStandartRow);
    int numOfStandardRows = timesToStandartRow * numOfRows;
    // 下面是一些输出结果中用到的参数
    bool rowStart, rowEnd, timesSmallerThanOne = (timesToStandartRow < 1);
    char delim = (timesSmallerThanOne) ? '\n' : '\t';
    int base = (timesSmallerThanOne) ? 1 : (int)timesToStandartRow;

    block_t readBlk;
    readBlk.loadFromDisk(resStartAddr, sizeOfRow * numOfStandardRows);
    row_t t[numOfStandardRows];
    int readRows, count = 0;
    printf("\n------------ 输出结果 ------------\n");
    int curRows = 0;
    while(1) {
        readRows = read_N_Rows_From_1_Block(readBlk, t, numOfStandardRows);
        // 输出结果的IO次数不计入表操作的IO次数，所以要减去
        // 这里还假设了结果是连续存储的，且除最后一块以外其他块都是写满的
        buff.numIO -= ceil(1.0 * readRows / numOfRowInBlk);
        count += readRows;
        if (timesSmallerThanOne) {
            rowStart = true;
            rowEnd = true;
        }
        for (int i = 0; i < readRows; ++i, ++curRows) {
            if (!timesSmallerThanOne) {
                rowStart = ((curRows % (int)timesToStandartRow) == 0);
                rowEnd = (((i + 1) % (int)timesToStandartRow) == 0);
            }
            if (rowStart)
                printf("%d\t", curRows / base + 1);
            printf("%d%c", t[i].A, delim);
            if (timesSmallerThanOne)
                printf("%d\t", ++curRows / base + 1);
            printf("%d%c", t[i].B, delim);
            if (rowEnd && !timesSmallerThanOne)
                printf("\n");
        }
        if (readRows < numOfStandardRows)
            break;
    }

    printf("----------------------------------\n");
    printf(" 共%d行结果\n\n", curRows / base);
    readBlk.freeBlock();
}


/**
 * 一趟扫描的中间步骤——块内散列
 * 从磁盘中读取数据到缓冲区上，散列成若干组并将每组散列结果存储回磁盘上
 * 
 * numOfSubTables: 待排序关系表的子表数量
 * scan_1_index: 该表一趟扫描结果存储的起始地址
 * startIndex: 该表一趟扫描所读入磁盘块的起始地址
 */
void scan_1_HashToBucket(int numOfBuckets, addr_t startIndex, addr_t scan_1_index[]) {
    int numOfRows = sizeOfRow;
    row_t R_data[numOfRows], R_Empty;
    R_Empty.A = MAX_ATTR_VAL, R_Empty.B = MAX_ATTR_VAL;
    block_t readBlk, bucketBlk[numOfBuckets];
    readBlk.loadFromDisk(startIndex);
    for (int i = 0; i < numOfBuckets; ++i)
        bucketBlk[i].writeInit(scan_1_index[i]);

    int readRows;
    while(1) {
        readRows = read_N_Rows_From_1_Block(readBlk, R_data, numOfRows);
        for (int k = 0; k < numOfRows; ++k) {
            int hashVal = hashRowsByA(R_data[k], numOfBuckets);
            bucketBlk[hashVal].writeRow(R_data[k]);
        }
        if (readRows < numOfRows) {
            addr_t endAddr;
            for (int i = 0; i < numOfBuckets; ++i) {
                endAddr = bucketBlk[i].writeLastBlock();
            }
            break;
        }
    }
}

/**
 * 一趟扫描的中间步骤——块内排序
 * 从磁盘中读取数据到缓冲区上，排序并将排序结果存储回磁盘上
 * 同时，记录下排序过程中得到的每个子表的首地址
 * 
 * numOfSubTables: 待排序关系表的子表数量
 * scan_1_index: 该表一趟扫描结果存储的起始地址
 * startIndex: 该表一趟扫描所读入磁盘块的起始地址
 * 
 * return: 一趟扫描结果所在存储区域的最后一个磁盘块的地址
 */
addr_t scan_1_PartialSort(int numOfSubTables, addr_t scan_1_index, addr_t startIndex, int sizeOfSubTable) {
    unsigned int numOfUsedBlk = numOfBufBlock;
    addr_t curAddr = scan_1_index;
    block_t readBlk[numOfUsedBlk];
    row_t R_data[sizeOfSubTable];
    for (int k = 0; k < numOfSubTables; ++k) {
        // printf("** 开始第%d个子表的排序 **\n", k + 1);
        // 一次从磁盘中读8块进来，作为一个子表
        addr_t nextStart = startIndex + 8 * k;
        for (int i = 0; i < numOfUsedBlk; ++i) {
            readBlk[i].loadFromDisk(nextStart);
            nextStart = (int)readBlk[i].readNextAddr();	// 取下一块地址到next中
        }
        // 提取这8块中的数据
        for (int i = 0; i < numOfUsedBlk; ++i) {
            for (int j = 0, count; j < numOfRowInBlk; ++j) {
                count = i * numOfRowInBlk + j;
                R_data[count] = readBlk[i].getNewRow();
            }
            readBlk[i].freeBlock();
            readBlk[i].writeInit(scan_1_index + 8 * k + i);
        }
        // 对当前子表进行排序
        insertSort<row_t>(R_data, sizeOfSubTable);
        // 将排序的子表写回磁盘
        for (int i = 0; i < numOfUsedBlk; ++i) {
            for (int j = 0, count; j < numOfRowInBlk; ++j) {
                count = i * numOfRowInBlk + j;
                readBlk[i].writeRow(R_data[count]);
            }
            curAddr += 1;
            addr_t writeAddr = curAddr;
            if (i == numOfUsedBlk - 1)
                writeAddr = 0;
            readBlk[i].writeToDisk(writeAddr);
        }
    }
    return curAddr;
}

/**
 * 两趟扫描的中间步骤——组内归并排序
 * 从磁盘中读取一个表中所有分组排序的数据到缓冲区上
 * 进行总表排序并将排序结果存储回磁盘上
 * 
 * numOfSubTables: 待排序关系表的子表数量
 * scan_1_index: 该表一趟扫描结果存储的起始地址
 * scan_2_index: 该表二趟扫描结果存储的起始地址
 * 
 * return: 二趟扫描结果所在存储区域的最后一个磁盘块的地址
 */
addr_t scan_2_SortMerge(int numOfSubTables, addr_t scan_1_index, addr_t scan_2_index) {
    addr_t resultAddr = scan_2_index;
    block_t readBlk[numOfSubTables];
    block_t bucketBlk;
    Row rtemp, rFirst[numOfSubTables];  // 记录每块的第一个元组
    bucketBlk.writeInit(resultAddr);
    // 先装载当前关系的所有子表中的第一块
    for (int i = 0; i < numOfSubTables; ++i) {
        addr_t curAddr = scan_1_index + i * 8;
        readBlk[i].loadFromDisk(curAddr);
        rFirst[i] = readBlk[i].getNewRow();
        if (rFirst[i].isFilled == false)
            return FAIL;
    }
    
    int arg = argmin(rFirst, numOfSubTables);
    while(1) {
        // 从当前位于每个块最前面的记录中，取索引字段值最小的一条记录的下标
        // 即arg为该记录所在的子表编号;
        rtemp = rFirst[arg];
        // printf("arg: %d\trtemp: %d\t\t", arg, rtemp.A);
        // for (int i = 0; i < numOfSubTables; ++i)
        //     printf("rFirst[%d]: %d\t", i, rFirst[i].A);
        // printf("\n");
        rFirst[arg] = readBlk[arg].getNewRow();
        arg = argmin(rFirst, numOfSubTables);
        if (rtemp.isFilled == false) {
            // 所有子表都已读完
            resultAddr += numOfBufBlock * numOfSubTables - 1;
            int isLastBlock = bucketBlk.writeLastBlock();
            if (isLastBlock) {
                // 结果块中还有剩余的内容
                resultAddr += 1;
            }
            break;
        }
        bucketBlk.writeRow(rtemp);
    }
    // showResult(scan_2_index);
    return resultAddr;
}

/**
 * 去重操作
 * 
 * tableStartAddr: 待去重的表的起始地址
 */
void tableDistinct(addr_t tableStartAddr) {
    int numOfUsedBlock = numOfBufBlock - 1;
    int numOfRows = numOfRowInBlk * numOfUsedBlock;
    addr_t readAddr = tableStartAddr, resAddr = tableStartAddr;
    
    block_t readBlk[numOfUsedBlock], bucketBlk;
    bucketBlk.writeInit(resAddr);
    for (int i = 0; i < numOfUsedBlock; ++i)
        readBlk[i].loadFromDisk(readAddr++);
    row_t t[numOfRows];

    int readRows;
    while(1) {
        readRows = read_N_Rows_From_M_Block(readBlk, t, numOfRows, numOfUsedBlock);
        // printRows(t, readRows, val);
        
    }
}
