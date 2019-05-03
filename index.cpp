#include <map>
#include "utils.cpp"
#include "BplusTree/BplusTree.h"

/**
 * @brief 索引相关操作
 * 
 * 内存中一共有两张采用map实现的地址映射表：
 * 聚簇地址映射表(clusterTableMap)——用于保存原表到聚簇表的磁盘地址映射关系
 * 索引地址映射表(indexTableMap)——用于保存原表到索引表的磁盘地址映射关系
 * 
 * 新定义的两种类型
 * table_map_t: 表的映射关系，格式为<原表首地址, (目标表首地址, 目标表末尾地址)>
 * pair_t: 表的映射关系对，方便map的insert操作
 */
typedef std::map<addr_t, index_t> table_map_t;
typedef std::pair<addr_t, index_t> pair_t;

const addr_t index_start = 10000;   // index的起始存储位置
const index_t invalid_index;

table_map_t clusterTableMap, indexTableMap; // 两张全局地址映射表
BPlusTree BPTR;


/**
 * @brief 表的聚簇操作：对table进行聚簇，并将聚簇结果存入磁盘中
 * 聚簇实际上就是使整张表有序，一共分为两步，分别对应两趟归并排序
 * 第一步：表的组内排序，对应scan_1_PartialSort
 * 第二步：表的组间排序，对应scan_2_SortMerge
 * 
 * @param table 当前待聚簇表信息
 * @param clusterAddr 聚簇结果的起始存储位置
 */
void tableClustering(table_t table, addr_t clusterAddr) {
    addr_t scan_1_Index = addrOfScan_1; // 一趟扫描的起始地址
    int rowTimes2Standard = table.rowSize / sizeOfRow;
    int numOfRows = rowTimes2Standard * (numOfRowInBlk / rowTimes2Standard);    // 一块中有多少条标准大小的记录
    int sizeOfSubTable = numOfRows * numOfBufBlock;      // 由缓冲区划分出的子表大小（单位：行）
    if (sizeOfSubTable == 0)
        error("table信息不全：请检查table的参数！");
    int numOfSubTables = ceil(1.0 * rowTimes2Standard * table.size / sizeOfSubTable);   // 划分出的子表数量

    // 聚簇操作：两趟归并排序
    scan_1_PartialSort(numOfSubTables, scan_1_Index, table.start, sizeOfSubTable);
    addr_t endAddr = scan_2_SortMerge(numOfSubTables, scan_1_Index, clusterAddr);
    DropFiles(scan_1_Index);
    if (endAddr == ADDR_NOT_EXISTS)
        error("二趟扫描出现错误！");

    // 创建新的聚簇条目
    index_t clusterIndex;
    clusterIndex.A = clusterAddr, clusterIndex.B = endAddr;
    clusterTableMap.insert(pair_t(table.start, clusterIndex));
}


/**
 * @brief 建立索引文件
 * 索引格式：(索引字段值, 第一次出现该值的索引块地址)
 * 前提：被索引的表需经过聚簇操作，在clusterTableMap中有对应的映射项
 * 
 * @param clusteredTableStart 被索引的聚簇表的起始地址
 * @param indexStart 索引文件的起始地址
 * @return addr_t 索引文件所在存储区域的最后一个磁盘块的地址
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
 * @brief 从磁盘中加载索引文件
 * 索引格式：(索引字段值, 第一次出现的索引块地址)
 * 
 * @param indexStart 索引文件的起始地址
 */
void loadIndex(addr_t indexStart) {
    addr_t next = indexStart;
    int numOfReadBlocks = numOfBufBlock;
    block_t blk[numOfReadBlocks];
    for (int i = 0; i < numOfReadBlocks; ++i) {
        blk[i].loadFromDisk(next);
        next = blk[i].readNextAddr();
        if (next == END_OF_FILE) {
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


/**
 * @brief 聚簇管理——使用聚簇文件前的准备工作
 * 对table进行聚簇文件、聚簇条目等结构的生成和管理
 * 用作对外提供聚簇的接口
 * 
 * @param table 待做聚簇管理的表
 * @param clusterAddr 聚簇文件的起始地址，若为默认值DEFAULT_ADDR则使用现有的聚簇文件
 *  否则将用该参数覆盖clusterTableMap中对应的条目
 */
void useCluster(table_t table, addr_t clusterAddr = DEFAULT_ADDR) {
    if (clusterAddr == DEFAULT_ADDR) {
        // 若使用的是默认聚簇地址，则检查当前表是否有对应的聚簇条目
        table_map_t::iterator findCluster = clusterTableMap.find(table.start);
        if (findCluster == clusterTableMap.end()) {
            // 若无对应的聚簇条目，则创建之并进行聚簇操作
            printf("当前查找表未仍未聚簇，现进行聚簇操作...\n");
            if (clusterTableMap.size() == 0) {
                // 若clusterTableMap中没有聚簇条目
                // 则从指定的地址addrOfScan_2开始建立聚簇文件
                clusterAddr = addrOfScan_2;
            } else {
                // 从最后一条聚簇条目的下一个地址开始建立聚簇文件
                table_map_t::iterator iter = clusterTableMap.end();
                --iter;
                clusterAddr = (iter->second).B + 1;
            }
        } else {
            return;
        }
    } else {
        // 若使用的是自定义聚簇地址，则先查看是否有对应的聚簇条目需要覆盖
        for (auto iter = clusterTableMap.begin(); iter != clusterTableMap.end(); ++iter) {
            index_t clusterIndex = iter->second;
            if (clusterIndex.A == clusterAddr) {
                printf("此处已有聚簇条目，准备覆盖...\n");
                clusterTableMap.erase(iter);    // 清除原来的聚簇条目
                break;
            }
        }
    }
    tableClustering(table, clusterAddr);
    printf("\n聚簇完成！\n");
    printf("聚簇所用IO: %d\n\n", buff.numIO);
}


/**
 * @brief 索引管理——使用索引文件前的准备工作
 * 对table进行索引文件、索引条目等结构的生成和管理
 * 用作对外提供索引的接口
 * 
 * @param table 待做聚簇管理的表
 * @return addr_t table对应索引文件的起始地址
 */
addr_t useIndex(table_t table) {
    addr_t indexAddr;
    table_map_t::iterator findIndex = indexTableMap.find(table.start);
    // 检查table的索引是否已建立
    if (findIndex == indexTableMap.end()) {
        printf("当前查找表未仍未建立索引，现建立索引...\n");
        addr_t indexStartAddr;
        if (indexTableMap.size() == 0) {
            // 若indexTableMap中没有索引条目
            // 则从指定的地址index_start开始建立索引
            indexStartAddr = index_start;
        } else {
            // 从最后一条索引条目的下一个地址开始建立索引
            table_map_t::iterator iter = indexTableMap.end();
            --iter;
            indexStartAddr = (iter->second).B + 1;
        }
        unsigned long prior_IO = buff.numIO;
        addr_t clusterAddr = clusterTableMap.at(table.start).A;
        addr_t indexEndAddr = buildIndex(clusterAddr, indexStartAddr);
        if (indexEndAddr != END_OF_FILE)
            printf("\n完成！索引写入磁盘块：%d-%d\n", indexStartAddr, indexEndAddr);
        indexAddr = indexStartAddr;
        printf("建立索引所用IO: %d\n\n", buff.numIO - prior_IO);
    } else {
        // 若有对应的索引条目，则直接引用索引条目即可
        index_t addrItem = indexTableMap.at(table.start);
        indexAddr = addrItem.A;
    }
    return indexAddr;
}
