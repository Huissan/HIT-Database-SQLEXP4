#include <iostream>
#include "Block.h"
extern "C" {
    #include "extmem.c"
}
#pragma once

#define CHAR_ZERO_ASCII '0'
#define CHAR_NINE_ASCII '9'
#define CHAR_EMPTY_ASCII 0

Block::Block() {
    // memset(blkData, 0, endOfBlock);
    endAddrOfData = addrOfLastRow;
}

/**
 * @brief 块的释放
 * 从缓冲区buff中释放该块，并完成一些相应的回收工作
 */
void Block::freeBlock() {
    readBlkAddr = 0;
    writeBlkAddr = 0;
    cursor = BLK_START_ADDR;        // 读写指针复位
    freeBlockInBuffer(blkData, &buff);
    memset(blkData, 0, endOfBlock); //　清空该块的数据
}

/**
 * @brief 从磁盘中加载地址为addr的磁盘块到缓冲区中
 * 
 * @param addr 将要加载的磁盘块地址
 * @param endPos 该磁盘块的末尾位置，它一定程度上决定了该磁盘读取的记录数量
 */
void Block::loadFromDisk(addr_t addr, cursor_t endPos) {
    if ((blkData = readBlockFromDisk(addr, &buff)) == NULL) {
        printf("Fail to read from disk %d!\n", addr);
        system("pause");
        exit(FAIL);
    }
    // printf("装载第%d块\n", addr);
    readBlkAddr = addr;
    cursor = BLK_START_ADDR;    // 指针复位
    endAddrOfData = endPos;
}

/**
 * @brief 块的写初始化
 * 
 * @param filename 写入文件名，在此代表块地址
 * @param maxNumOfRows 该块写入的最大记录数量
 */
void Block::writeInit(const file_t filename, int maxNumOfRows) {
    writeBlkAddr = filename;
    cursor = BLK_START_ADDR;
    if ((blkData = getNewBlockInBuffer(&buff)) == NULL){
        printf("Buffer Allocation Error!\n");
        system("pause");
        exit(FAIL);
    }
    memset(blkData, 0, endOfBlock);
    endAddrOfData = maxNumOfRows * sizeOfRow;
}

/**
 * @brief 写入最后一块并释放写块
 * 
 * @return addr_t 当前写入最后一块的写入地址
 */
addr_t Block::writeLastBlock() {
    if (cursor > BLK_START_ADDR) {
        // 表示写块里面还有剩余的内容
        _writeAddr(END_OF_FILE);
        _writeToDisk(writeBlkAddr);
        writeBlkAddr += 1;
        return writeBlkAddr - 1;
    } else {
        freeBlock();
        return 0;
    }
}

/**
 * @brief 从当前的Block中获取一条新记录
 * 含自动迭代功能：当前块读完时，自动读取下一个地址到缓冲区
 * 当前文件读完时，无论接下来继续读取多少次，都返回空记录
 * 
 * @return row_t 读取的记录结果
 */
row_t Block::getNewRow() {
    row_t R, emptyRow;
    if (cursor == endAddrOfData) {
        // 该block的所有内容已读取完成，需要读取一块新的block进来
        addr_t nextAddr = readNextAddr();
        freeBlock();    // 想要继续申请必须先释放
        if (nextAddr == END_OF_FILE) {
            // 当前已读到文件末尾，即无下一块可读时，返回空记录
            return emptyRow;
        } else {
            loadFromDisk(nextAddr, endAddrOfData);
        }
    }
    // 读取Row中的属性值A, B
    char str1[5] = "\0", str2[5] = "\0";
    for (int i = 0; i < 4; ++i) {
        str1[i] = *(blkData + cursor + i);
        if (str1[i] == '\0')
            str1[i] = 32;
        str2[i] = *(blkData + cursor + 4 + i);
        if (str2[i] == '\0')
            str2[i] = 32;
    }
    int scan1 = sscanf(str1, "%4d", &R.A);
    int scan2 = sscanf(str2, "%4d", &R.B);
    R.isFilled = true;
    if (scan1 <= 0)
        R.isFilled = false;
    if (scan2 <= 0)
        R.B = MAX_ATTR_VAL;
    cursor += sizeOfRow;    // cursor移动到下一条记录
    return R;
}

/**
 * @brief 向当前的Block中写入一条新记录
 * 缓冲区写满时，自动写入磁盘
 * 
 * @param R 写入的记录
 * @return addr_t 当前写入的块的地址
 */
addr_t Block::writeRow(const row_t R) {
    // 先判断block是否满
    if (cursor == endAddrOfData) {
        // printf("满一个块的结果，");
        try {
            addr_t next = writeBlkAddr + 1;
            _writeAddr(next);
            _writeToDisk(writeBlkAddr);
            if (next == END_OF_FILE) {
                writeBlkAddr = END_OF_FILE;
                cursor = endOfBlock;
            } else {
                // 写完后继续写，重新申请缓冲区
                if ((blkData = getNewBlockInBuffer(&buff)) == NULL)
                    throw std::bad_alloc();
                writeBlkAddr += 1;
                memset(blkData, 0, sizeOfRow * (numOfRowInBlk + 1));
                cursor = BLK_START_ADDR;
            }
        } catch(const std::bad_alloc &e) {
            std::cerr << e.what() << std::endl;
            printf("Buffer Allocation Error!\n");
            system("pause");
            exit(FAIL);
        }
    }
    // 再写数据到block上
    char a_buf[5] = "\0", b_buf[5] = "\0";
    if (R.A < MAX_ATTR_VAL)
        snprintf(a_buf, 5, "%-4d", R.A);
    if (R.B < MAX_ATTR_VAL)
        snprintf(b_buf, 5, "%-4d", R.B);
    for (int i = 0; i < 4; ++i) {
        if (a_buf[i] < CHAR_ZERO_ASCII || a_buf[i] > CHAR_NINE_ASCII)
            a_buf[i] = CHAR_EMPTY_ASCII;
        *(blkData + cursor + i) = a_buf[i];
        if (b_buf[i] < CHAR_ZERO_ASCII || b_buf[i] > CHAR_NINE_ASCII)
            b_buf[i] = CHAR_EMPTY_ASCII;
        *(blkData + cursor + 4 + i) = b_buf[i];
    }
    cursor += sizeOfRow;
    return writeBlkAddr;
}

/**
 * @brief 读取当前块指向的下一个地址
 * 
 * @return addr_t 读取的地址
 */
addr_t Block::readNextAddr() {
    char nextAddr[8] = "\0";
    unsigned int offset = addrOfLastRow;
    for (int i = 0; i < 8; ++i) {
        nextAddr[i] = *(blkData + offset + i);
    }
    addr_t next;
    if (sscanf(nextAddr, "%8d", &next) < 0)
        next = END_OF_FILE;
    return next;
}

// -----------------------------------------------------------
//                       Private Methods                      
// -----------------------------------------------------------

/**
 * @brief 写地址到当前块中
 * 
 * @param nextAddr 写入的地址
 */
void Block::_writeAddr(addr_t nextAddr) {
    char buf[9];
    unsigned int offset = addrOfLastRow;
    snprintf(buf, 8, "%-8d", nextAddr);
    for (int i = 0; i < 8; ++i) {
        if (buf[i] < CHAR_ZERO_ASCII || buf[i] > CHAR_NINE_ASCII)
            buf[i] = CHAR_EMPTY_ASCII;
        *(blkData + offset + i) = buf[i];
    }
}

/**
 * @brief 将当前块的内容写入到磁盘上
 * 
 * @param addr 写入的磁盘地址
 */
void Block::_writeToDisk(addr_t addr) {
    if (writeBlockToDisk(blkData, addr, &buff) != 0) {
        printf("Fail to write to disk %d!\n", addr);
        system("pause");
        exit(FAIL);
    }
    // printf("写磁盘到第%d块\n", addr);
    cursor = BLK_START_ADDR;    // 指针复位
    memset(blkData, 0, endOfBlock);
}


// -----------------------------------------------------------
//                 Integrated Block Operations                
// -----------------------------------------------------------

/**
 * @brief 初始化全局缓冲区
 * 在所有使用缓冲区的main方法中，在使用缓冲区前必须要调用此函数生成一个全局缓冲区变量
 * 否则会无法正常使用缓冲区
 */
void bufferInit() {
    if (!initBuffer(520, 64, &buff)) {
        perror("Buffer Initialization Failed!\n");
        system("pause");
        exit(FAIL);
    }
}


/**
 * @brief 清空缓冲区中IO的计数
 */
void clear_Buff_IO_Count() { buff.numIO = 0; }


/**
 * @brief 只利用1个Block，连续读取一个表中最多N条记录
 * 读取到表尾时，会自动归还缓冲区块
 * 前提：blk必须已被初始化(通过loadFromDisk等方法)
 * 
 * @param readBlk 用于读取记录的缓冲区块
 * @param R 用于读取记录数组
 * @param N 记录数组的最大长度
 * @return int 实际读取的记录数量
 */
int read_N_Rows_From_1_Block(block_t &readBlk, row_t *R, int N) {
    int totalRead = N;
    for (int i = 0; i < N; ++i) {
        R[i] = readBlk.getNewRow();
        if (R[i].isFilled == false) {
            // 有些block不一定有完整的7条记录，一般是表的末尾
            totalRead = i;
            break;
        }
    }
    return totalRead;
}


/**
 * @brief 利用M个Block，连续读取一个表中最多N条记录
 * 读取完N条记录后，会自动加载下一组表块到缓冲区中，方便下一次读取
 * 读取到表尾时，会自动归还缓冲区块
 * 前提：所有M个blk都必须已被初始化(通过loadFromDisk等方法)
 * 
 * @param readBlk 用于读取记录的连续缓冲区块
 * @param R 用于读取记录数组
 * @param N 记录数组的最大长度
 * @param M block的最大数量
 * @return int 实际读取的记录数量
 */
int read_N_Rows_From_M_Block(block_t *readBlk, row_t *R, int N, int M) {
    int totalRead = 0;
    row_t rtemp;
    // 从当前的M块中读取记录
    for (int i = 0; i < N; ) {
        for (int j = 0; j < M; ++j) {
            rtemp = readBlk[j].getNewRow();
            i += 1;
            if (rtemp.isFilled)
                R[totalRead++] = rtemp;
        }
    }
    addr_t next = readBlk[M - 1].readNextAddr();
    int count = 0;
    if (next == END_OF_FILE) {
        // 最后一块若没被使用，只可能是处于已经被归还的状态
        // 这时读出来的地址一定是END_OF_FILE，以此判断是否读到表尾
        if (totalRead > 0) {
            do {    // 归还剩余的缓冲区
                next = readBlk[count].readNextAddr();
                readBlk[count].freeBlock();
                count += 1;
            } while(next != END_OF_FILE);
        } else {
            // 上一轮刚好读完整张表，这一轮不需要归还缓冲区
            // 因为在上一轮所有缓冲区都已被释放
            ;
        }
    } else {
        while(count < M && next != END_OF_FILE) {
            // 装载后续剩余的块，当next=END_OF_FILE时表已读完
            readBlk[count].freeBlock();
            readBlk[count].loadFromDisk(next);
            next = readBlk[count].readNextAddr();
            count += 1;
        }
        for (int i = 0; i < M - count; ++i) {
            // 归还没有用上的缓冲区块
            readBlk[count + i].freeBlock();
        }
    }
    return totalRead;
}
