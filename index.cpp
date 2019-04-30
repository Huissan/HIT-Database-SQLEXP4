#include <map>
#include "utils.cpp"
#include "BplusTree/BplusTree.h"

/**
 * 索引相关操作
 * 
 * 内存中一共有两张采用map实现的地址映射表：
 * 聚簇地址映射表(clusterTableMap)——用于保存原表到聚簇表的磁盘地址映射关系
 * 索引地址映射表(indexTableMap)——用于保存原表到索引表的磁盘地址映射关系
 * 
 * table_map_t: 表的映射关系，格式为<原表首地址, (目标表首地址, 目标表末尾地址)>
 * pair_t: 表的映射关系对，方便map的insert操作
 */

typedef std::map<addr_t, index_t> table_map_t;
typedef std::pair<addr_t, index_t> pair_t;

const addr_t index_start = 10000;
const int maxNumOfIndex = 10;
const index_t invalid_index;

table_map_t clusterTableMap, indexTableMap; // 两张全局地址映射表
BPlusTree BPTR; // 在全局设置1棵B+树


/**
 * 表的聚簇操作：对table进行聚簇，并将聚簇结果存入磁盘中
 * 聚簇实际上就是使整张表有序，一共分为两步，分别对应两趟归并排序
 * 第一步：表的组内排序，对应scan_1_PartialSort
 * 第二步：表的组间排序，对应scan_2_SortMerge
 * 
 * table: 当前待聚簇表信息
 * clusterAddr: 聚簇结果的起始存储位置
 * 
 * return: 聚簇结果所在存储区域的最后一个磁盘块的地址
 */
addr_t tableClustering(table_t table, addr_t clusterAddr) {
    addr_t scan_1_Index = addrOfScan_1;
    int sizeOfSubTable = numOfRowInBlk * numOfBufBlock;       // 由缓冲区划分出的子表大小
    int numOfSubTables = ceil(1.0 * table.size / sizeOfSubTable);   // R划分出的子表数量
    scan_1_PartialSort(numOfSubTables, scan_1_Index, table.start, sizeOfSubTable);
    addr_t retAddr = scan_2_SortMerge(numOfSubTables, scan_1_Index, clusterAddr);

    // 检查聚簇地址是否有对应的条目，若有则覆盖之
    for (auto iter = clusterTableMap.begin(); iter != clusterTableMap.end(); ++iter) {
        index_t clusterIndex = iter->second;
        if (clusterIndex.A == clusterAddr) {
            printf("此处已有聚簇条目，准备覆盖...\n");
            clusterTableMap.erase(iter);
            printf("覆盖完成！\n");
            break;
        }
    }
    index_t clusterIndex;
    clusterIndex.A = clusterAddr, clusterIndex.B = retAddr;
    clusterTableMap.insert(pair_t(table.start, clusterIndex));
    return retAddr;
}

/**
 * 建立索引文件
 * 索引格式：(索引字段值, 第一次出现该值的索引块地址)
 * 前提：被索引的表需经过聚簇操作
 * 
 * clusteredTableStart: 被索引的聚簇表的起始地址
 * indexStart: 索引文件的起始地址
 * 
 * return: 索引文件所在存储区域的最后一个磁盘块的地址
 */
addr_t buildIndex(addr_t clusteredTableStart, addr_t indexStart) {
    addr_t next = clusteredTableStart, indexAddr = 0;
    int numOfReadBlocks = numOfBufBlock - 1;
    int numOfIndices = numOfRowInBlk * numOfReadBlocks;

    // 查找聚簇地址映射表中是否有对应的项
    addr_t tableAddr;
    auto findOrigin = std::find_if(clusterTableMap.begin(), clusterTableMap.end(),
        [clusteredTableStart](const table_map_t::value_type item)
    {   // 查找聚簇表首地址对应的原表首地址
        index_t clusterIndex = item.second;
        return (clusterIndex.A == clusteredTableStart);
    });
    if (findOrigin == clusterTableMap.end()) {
        // 这里默认了buildIndex之前该表已做过聚簇(tableClustering)
        // 所以如果没有找到对应的首地址，证明该聚簇表的信息丢失了
        printf("严重错误：聚簇信息丢失！\n");
        system("pause");
        exit(0);
    }
    tableAddr = findOrigin->first;  // 获取对应的原表首地址

    row_t R[numOfIndices], R_prior;
    block_t blk[numOfReadBlocks], resBlk;
    resBlk.writeInit(indexStart);
    for (int i = 0; i < numOfReadBlocks; ++i)
        blk[i].loadFromDisk(next++);
    int readRows, count = 0;
    while(1) {
        // 顺序遍历，寻找每个索引字段值第一次出现的块地址，将其写入结果中
        readRows = read_N_Rows_From_M_Block(blk, R, numOfIndices, numOfReadBlocks);
        insertSort<row_t>(R, readRows);
        // printRows(R, readRows);
        for (int i = 0; i < readRows; ++i, ++count) {
            if (R_prior.isFilled == false || R_prior.A < R[i].A) {
                // 新的索引字段值与上一个索引字段值不同，即该值第一次出现
                index_t indexItem;
                indexItem.A = R[i].A;
                indexItem.B = clusteredTableStart + count / numOfRowInBlk;
                indexAddr = resBlk.writeRow(indexItem);
            }
            R_prior = R[i];
        }
        if (readRows < numOfIndices) {
            // 聚簇表已读完
            addr_t endAddr = resBlk.writeLastBlock();
            if (endAddr != END_OF_FILE)
                indexAddr = endAddr;
            break;
        }
    }

    // 写索引地址映射表
    table_map_t::iterator findIndex;
    findIndex = indexTableMap.find(tableAddr);
    if (findIndex != indexTableMap.end()) {
        printf("该处已有索引项，准备覆盖...\n");
        indexTableMap.erase(findIndex);
    }
    index_t addrItem;
    addrItem.A = indexStart, addrItem.B = indexAddr;
    indexTableMap.insert(pair_t(tableAddr, addrItem));
    return indexAddr;
}

/**
 * 从磁盘中加载索引文件
 * 索引格式：(索引字段值, 第一次出现的索引块地址)
 * 
 * indexStart: 索引文件的起始地址
 */
void loadIndex(addr_t indexStart) {
    addr_t next = indexStart;
    int numOfReadBlocks = numOfBufBlock;
    block_t blk[numOfReadBlocks];
    for (int i = 0; i < numOfReadBlocks; ++i) {
        blk[i].loadFromDisk(next++);
        if (blk[i].readNextAddr() == END_OF_FILE) {
            numOfReadBlocks = i + 1;
            break;
        }
    }
    int numOfIndices = numOfRowInBlk * numOfReadBlocks;
    index_t index[numOfIndices];
    BPTR.clear();

    int readRows, numOfUsedBlocks;
    while(1) {
        readRows = read_N_Rows_From_M_Block(blk, index, numOfIndices, numOfReadBlocks);
        numOfUsedBlocks = ceil(1.0 * readRows / numOfRowInBlk);
        insertSort<row_t>(index, readRows);
        // printRows(R, readRows);
        for (int i = 0; i < readRows; ++i) {
            BPTR.insert(index[i].A, index[i].B);
        }
        if (numOfUsedBlocks < numOfReadBlocks) {
            for (int i = 0; i < numOfUsedBlocks; ++i)
                blk[i].freeBlock();
            break;
        }
    }
    // BPTR.printData();
}